package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/redis/agent-filesystem/mount/client"
)

// downloadOpKind enumerates the operations the downloader can apply to the
// local filesystem.
type downloadOpKind int

const (
	opDownloadFile downloadOpKind = iota + 1
	opDownloadSymlink
	opDownloadMkdir
	opDownloadDelete
	opDownloadChmod
)

// downloadOp is the work item the reconciler hands to the downloader.
type downloadOp struct {
	Kind        downloadOpKind
	Path        string // workspace-relative POSIX, no leading slash
	AbsPath     string // absolute local destination
	Mode        uint32
	Symlink     string // target, only for opDownloadSymlink
	StoredEntry SyncEntry
	HasStored   bool
	Conflict    bool // when true, downloader writes the conflict-copy first then writes remote
	// Chunked download fields (set when remote file is chunked).
	Chunked     bool
	FileSize    int64
	ChunkSize   int
	ChunkHashes []string // remote's complete manifest
	DirtyChunks []int    // indices to fetch
}

// downloadResult informs the reconciler of the outcome so it can update state.
type downloadResult struct {
	Op           downloadOp
	Err          error
	Skipped      bool // local changed since staging; reconcile again without adopting this result
	RemoteHash   string
	RemoteStat   *client.StatResult
	ConflictPath string // populated when Conflict is true and the local file was preserved
	Mode         uint32
	Size         int64
	MtimeMs      int64
	LocalMtimeMs int64
	Target       string // for symlinks
}

// downloader runs in its own goroutine, draining ops from the reconciler.
type downloader struct {
	fs       client.Client
	results  chan<- downloadResult
	root     string // local workspace root
	pid      int
	conflict *conflictNamer
	echo     *echoSuppressor
	readonly bool
	log      *syncLogger
}

func newDownloader(fs client.Client, results chan<- downloadResult, root string, conflict *conflictNamer, echo *echoSuppressor, readonly bool, log *syncLogger) *downloader {
	return &downloader{
		fs:       fs,
		results:  results,
		root:     root,
		pid:      os.Getpid(),
		conflict: conflict,
		echo:     echo,
		readonly: readonly,
		log:      log,
	}
}

// run drains in until ctx is cancelled.
func (d *downloader) run(ctx context.Context, in <-chan downloadOp) {
	for {
		select {
		case <-ctx.Done():
			return
		case op, ok := <-in:
			if !ok {
				return
			}
			d.process(ctx, op)
		}
	}
}

func (d *downloader) process(ctx context.Context, op downloadOp) {
	switch op.Kind {
	case opDownloadFile:
		d.processFile(ctx, op)
	case opDownloadSymlink:
		d.processSymlink(ctx, op)
	case opDownloadMkdir:
		d.processMkdir(ctx, op)
	case opDownloadDelete:
		d.processDelete(ctx, op)
	case opDownloadChmod:
		d.processChmod(ctx, op)
	default:
		d.send(downloadResult{Op: op, Err: fmt.Errorf("unknown download op kind: %d", op.Kind)})
	}
}

func (d *downloader) processFile(ctx context.Context, op downloadOp) {
	if op.Chunked {
		d.processChunkedFile(ctx, op)
		return
	}
	local, err := snapshotDownloadLocal(op.AbsPath, op.StoredEntry)
	if err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}
	remotePath := absoluteRemotePath(op.Path)
	stat, err := d.fs.Stat(ctx, remotePath)
	if err != nil && !isClientNotFound(err) {
		d.send(downloadResult{Op: op, Err: fmt.Errorf("stat remote %s: %w", op.Path, err)})
		return
	}
	if stat == nil {
		// Treat as a delete: the inode vanished between the invalidation
		// dispatch and our follow-up read.
		if !local.unchanged(op.AbsPath) || !local.matchesStored(op) {
			d.send(downloadResult{Op: op, Skipped: true})
			return
		}
		d.removeLocalFile(op)
		d.send(downloadResult{Op: downloadOp{Kind: opDownloadDelete, Path: op.Path, AbsPath: op.AbsPath, StoredEntry: op.StoredEntry, HasStored: op.HasStored}})
		return
	}
	if stat.Type == "dir" {
		d.processMkdir(ctx, op)
		return
	}
	if stat.Type == "symlink" {
		target, err := d.fs.Readlink(ctx, remotePath)
		if err != nil {
			d.send(downloadResult{Op: op, Err: err})
			return
		}
		op.Symlink = target
		d.processSymlink(ctx, op)
		return
	}

	data, err := d.fs.Cat(ctx, remotePath)
	if err != nil {
		d.send(downloadResult{Op: op, Err: fmt.Errorf("read remote %s: %w", op.Path, err)})
		return
	}
	hash := sha256Hex(data)
	mode := stat.Mode
	if d.readonly {
		mode = 0o444
	}
	if !local.unchanged(op.AbsPath) {
		d.send(downloadResult{Op: op, Skipped: true})
		return
	}
	if local.info != nil && local.info.Mode().IsRegular() && local.hash == hash && uint32(local.info.Mode().Perm()) == mode&0o777 {
		d.sendFileResult(op, stat, hash, "", mode, int64(len(data)))
		return
	}
	if !op.Conflict && !local.matchesStored(op) {
		d.send(downloadResult{Op: op, Skipped: true})
		return
	}

	var conflictPath string
	if op.Conflict {
		moved, err := moveLocalToConflict(d.conflict, op.AbsPath)
		if err != nil {
			d.send(downloadResult{Op: op, Err: fmt.Errorf("preserve conflict copy %s: %w", op.Path, err)})
			return
		}
		conflictPath = moved
	}

	if err := d.atomicWriteFile(op.AbsPath, data, mode); err != nil {
		d.send(downloadResult{Op: op, Err: fmt.Errorf("write local %s: %w", op.Path, err)})
		return
	}
	d.echo.markFile(op.Path, hash)

	d.sendFileResult(op, stat, hash, conflictPath, mode, int64(len(data)))
}

func (d *downloader) sendFileResult(op downloadOp, stat *client.StatResult, hash, conflictPath string, mode uint32, size int64) {
	info, err := os.Lstat(op.AbsPath)
	if err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}
	d.send(downloadResult{
		Op:           op,
		RemoteHash:   hash,
		RemoteStat:   stat,
		ConflictPath: conflictPath,
		Mode:         mode,
		Size:         size,
		MtimeMs:      stat.Mtime,
		LocalMtimeMs: info.ModTime().UnixMilli(),
	})
}

func (d *downloader) processChunkedFile(ctx context.Context, op downloadOp) {
	if op.Conflict {
		// Moving a conflict aside removes the unchanged local chunks too.
		// Materialize the whole remote file instead of patching a new file.
		op.Chunked, op.FileSize, op.ChunkSize = false, 0, 0
		op.ChunkHashes, op.DirtyChunks = nil, nil
		d.processFile(ctx, op)
		return
	}
	local, err := snapshotDownloadLocal(op.AbsPath, op.StoredEntry)
	if err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}
	if local.info == nil || !op.HasStored || op.StoredEntry.ChunkSize != op.ChunkSize {
		// A delta requires the original complete file as its base.
		op.Chunked, op.FileSize, op.ChunkSize = false, 0, 0
		op.ChunkHashes, op.DirtyChunks = nil, nil
		d.processFile(ctx, op)
		return
	}
	if !local.matchesStored(op) {
		d.send(downloadResult{Op: op, Skipped: true})
		return
	}
	remotePath := absoluteRemotePath(op.Path)
	stat, err := d.fs.Stat(ctx, remotePath)
	if err != nil && !isClientNotFound(err) {
		d.send(downloadResult{Op: op, Err: fmt.Errorf("stat remote %s: %w", op.Path, err)})
		return
	}
	if stat == nil {
		if !local.unchanged(op.AbsPath) {
			d.send(downloadResult{Op: op, Skipped: true})
			return
		}
		d.removeLocalFile(op)
		d.send(downloadResult{Op: downloadOp{Kind: opDownloadDelete, Path: op.Path, AbsPath: op.AbsPath, StoredEntry: op.StoredEntry, HasStored: op.HasStored}})
		return
	}

	// Fetch dirty chunks from remote.
	chunkData, err := d.fs.ReadChunks(ctx, remotePath, op.DirtyChunks, op.ChunkSize)
	if err != nil {
		d.send(downloadResult{Op: op, Err: fmt.Errorf("read chunks %s: %w", op.Path, err)})
		return
	}
	if !local.unchanged(op.AbsPath) {
		d.send(downloadResult{Op: op, Skipped: true})
		return
	}
	for _, idx := range op.DirtyChunks {
		if idx < 0 || idx >= len(op.ChunkHashes) || sha256Hex(chunkData[idx]) != op.ChunkHashes[idx] {
			d.send(downloadResult{Op: op, Skipped: true})
			return
		}
	}

	mode := stat.Mode
	if d.readonly {
		mode = 0o444
	}
	hash := compositeHash(op.ChunkHashes)
	if local.baselineHash == hash && local.info.Size() == op.FileSize && uint32(local.info.Mode().Perm()) == mode&0o777 {
		d.sendFileResult(op, stat, hash, "", mode, op.FileSize)
		return
	}

	// Ensure parent directory exists.
	if err := os.MkdirAll(filepath.Dir(op.AbsPath), 0o755); err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}

	// Patch the local file at chunk offsets.
	f, err := os.OpenFile(op.AbsPath, os.O_RDWR|os.O_CREATE, fs.FileMode(mode&0o7777))
	if err != nil {
		d.send(downloadResult{Op: op, Err: fmt.Errorf("open %s: %w", op.Path, err)})
		return
	}
	for idx, data := range chunkData {
		offset := int64(idx) * int64(op.ChunkSize)
		if _, err := f.WriteAt(data, offset); err != nil {
			_ = f.Close()
			d.send(downloadResult{Op: op, Err: fmt.Errorf("write chunk %d of %s: %w", idx, op.Path, err)})
			return
		}
	}
	if err := f.Truncate(op.FileSize); err != nil {
		_ = f.Close()
		d.send(downloadResult{Op: op, Err: fmt.Errorf("truncate %s: %w", op.Path, err)})
		return
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		d.send(downloadResult{Op: op, Err: err})
		return
	}
	_ = f.Close()
	_ = os.Chmod(op.AbsPath, fs.FileMode(mode&0o7777))

	d.echo.markFile(op.Path, hash)
	d.sendFileResult(op, stat, hash, "", mode, op.FileSize)
}

// The persisted baseline detects queued overwrites of newer local edits. A
// second stat check detects edits or replacements while remote reads run.
type downloadLocalSnapshot struct {
	info         fs.FileInfo
	hash         string
	baselineHash string
	target       string
}

func snapshotDownloadLocal(abs string, stored SyncEntry) (downloadLocalSnapshot, error) {
	s := downloadLocalSnapshot{}
	info, err := os.Lstat(abs)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	s.info = info
	if info.Mode()&os.ModeSymlink != 0 {
		s.target, err = os.Readlink(abs)
	} else if info.Mode().IsRegular() {
		f, openErr := os.Open(abs)
		if openErr != nil {
			return s, openErr
		}
		h := sha256.New()
		_, err = io.Copy(h, f)
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
		s.hash = hex.EncodeToString(h.Sum(nil))
		s.baselineHash = s.hash
		if err == nil && stored.ChunkSize > 0 {
			var hashes []string
			hashes, _, err = streamChunkHashes(abs, stored.ChunkSize)
			s.baselineHash = compositeHash(hashes)
		}
	}
	return s, err
}

func (s downloadLocalSnapshot) unchanged(abs string) bool {
	now, err := os.Lstat(abs)
	if s.info == nil {
		return os.IsNotExist(err)
	}
	return err == nil && os.SameFile(s.info, now) && s.info.Mode() == now.Mode() && s.info.Size() == now.Size() && s.info.ModTime().Equal(now.ModTime())
}

func (s downloadLocalSnapshot) matchesStored(op downloadOp) bool {
	if s.info == nil {
		return !op.HasStored || op.StoredEntry.Deleted
	}
	if !op.HasStored || op.StoredEntry.Deleted {
		return false
	}
	stored := op.StoredEntry
	switch {
	case s.info.Mode().IsRegular():
		return stored.Type == "file" && uint32(s.info.Mode().Perm()) == stored.Mode&0o777 && s.info.Size() == stored.Size && stored.LocalHash != "" && (s.hash == stored.LocalHash || s.baselineHash == stored.LocalHash)
	case s.info.Mode()&os.ModeSymlink != 0:
		return stored.Type == "symlink" && s.target == stored.Target
	case s.info.IsDir():
		return stored.Type == "dir" && uint32(s.info.Mode().Perm()) == stored.Mode&0o777
	}
	return false
}

func (d *downloader) processSymlink(ctx context.Context, op downloadOp) {
	local, err := snapshotDownloadLocal(op.AbsPath, op.StoredEntry)
	if err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}
	if op.Symlink == "" {
		remotePath := absoluteRemotePath(op.Path)
		target, err := d.fs.Readlink(ctx, remotePath)
		if err != nil {
			d.send(downloadResult{Op: op, Err: err})
			return
		}
		op.Symlink = target
	}
	if !local.unchanged(op.AbsPath) {
		d.send(downloadResult{Op: op, Skipped: true})
		return
	}
	if local.info != nil && local.info.Mode()&os.ModeSymlink != 0 && local.target == op.Symlink {
		d.send(downloadResult{Op: op, Target: op.Symlink, LocalMtimeMs: local.info.ModTime().UnixMilli()})
		return
	}
	if !op.Conflict && !local.matchesStored(op) {
		d.send(downloadResult{Op: op, Skipped: true})
		return
	}
	var conflictPath string
	if op.Conflict {
		conflictPath, err = moveLocalToConflict(d.conflict, op.AbsPath)
		if err != nil {
			d.send(downloadResult{Op: op, Err: err})
			return
		}
	}
	if err := os.MkdirAll(filepath.Dir(op.AbsPath), 0o755); err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}
	if _, err := os.Lstat(op.AbsPath); err == nil {
		_ = os.Remove(op.AbsPath)
	}
	if err := os.Symlink(op.Symlink, op.AbsPath); err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}
	d.echo.markSymlink(op.Path, op.Symlink)
	info, err := os.Lstat(op.AbsPath)
	if err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}
	d.send(downloadResult{Op: op, Target: op.Symlink, ConflictPath: conflictPath, LocalMtimeMs: info.ModTime().UnixMilli()})
}

func (d *downloader) processMkdir(ctx context.Context, op downloadOp) {
	if err := os.MkdirAll(op.AbsPath, 0o755); err != nil {
		d.send(downloadResult{Op: op, Err: fmt.Errorf("mkdir local %s: %w", op.Path, err)})
		return
	}
	d.echo.markDir(op.Path)
	d.send(downloadResult{Op: op})
}

func (d *downloader) processDelete(ctx context.Context, op downloadOp) {
	d.removeLocalFile(op)
	d.send(downloadResult{Op: op})
}

func (d *downloader) removeLocalFile(op downloadOp) {
	info, err := os.Lstat(op.AbsPath)
	if err != nil {
		return
	}
	d.echo.markDelete(op.Path)
	if info.IsDir() {
		_ = os.RemoveAll(op.AbsPath)
		return
	}
	_ = os.Remove(op.AbsPath)
}

func (d *downloader) processChmod(ctx context.Context, op downloadOp) {
	if err := os.Chmod(op.AbsPath, fs.FileMode(op.Mode&0o7777)); err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}
	d.send(downloadResult{Op: op, Mode: op.Mode})
}

// atomicWriteFile writes content into a sibling temp file and renames it
// over the destination so concurrent readers (like our own watcher) see
// either the old or the new file but never a partial. The temp filename
// embeds .afssync.tmp so the baseline ignore filter drops the watcher event.
func (d *downloader) atomicWriteFile(absPath string, data []byte, mode uint32) error {
	return writeAtomicFile(absPath, data, mode)
}

func (d *downloader) send(r downloadResult) {
	if d.results == nil {
		return
	}
	d.results <- r
}

func randomSuffix() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// echoSuppressor is the deterministic, hash-based mechanism we use to drop
// fsnotify events that are echoes of our own downloader writes. The
// downloader stamps the expected post-rename hash before the rename; the
// reconciler consults this on every local event and ignores it when the
// observed disk content matches.
type echoSuppressor struct {
	mu      sync.Mutex
	pending map[string]echoExpectation
}

type echoExpectation struct {
	kind string // "file" | "symlink" | "dir" | "delete"
	hash string // sha256 hex for files; symlink target for symlinks
}

func newEchoSuppressor() *echoSuppressor {
	return &echoSuppressor{pending: make(map[string]echoExpectation)}
}

func (e *echoSuppressor) markFile(rel, hash string) {
	e.set(rel, echoExpectation{kind: "file", hash: hash})
}

func (e *echoSuppressor) markSymlink(rel, target string) {
	e.set(rel, echoExpectation{kind: "symlink", hash: target})
}

func (e *echoSuppressor) markDir(rel string) {
	e.set(rel, echoExpectation{kind: "dir"})
}

func (e *echoSuppressor) markDelete(rel string) {
	e.set(rel, echoExpectation{kind: "delete"})
}

func (e *echoSuppressor) set(rel string, exp echoExpectation) {
	e.mu.Lock()
	if e.pending == nil {
		e.pending = make(map[string]echoExpectation)
	}
	e.pending[rel] = exp
	e.mu.Unlock()
}

// consume returns the expectation for rel and removes it. Returns ok==false
// if there is no pending echo. Use this in the reconciler before reading the
// local file from disk: if ok and the on-disk hash matches, drop the event.
func (e *echoSuppressor) consume(rel string) (echoExpectation, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	exp, ok := e.pending[rel]
	if ok {
		delete(e.pending, rel)
	}
	return exp, ok
}
