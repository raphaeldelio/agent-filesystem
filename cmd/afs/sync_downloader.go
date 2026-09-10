package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"

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
	RemoteHash   string
	RemoteStat   *client.StatResult
	ConflictPath string // populated when Conflict is true and the local file was preserved
	Mode         uint32
	Size         int64
	MtimeMs      int64
	Target       string // for symlinks
}

// downloader runs in its own goroutine, draining ops from the reconciler.
type downloader struct {
	stopCh   <-chan struct{}
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
			if !ok || ctx.Err() != nil {
				return
			}
			d.process(ctx, op)
		}
	}
}

func (d *downloader) process(ctx context.Context, op downloadOp) {
	if d.cancelled(ctx, op) {
		return
	}
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
	remotePath := absoluteRemotePath(op.Path)
	stat, err := d.fs.Stat(ctx, remotePath)
	if err != nil && !isClientNotFound(err) {
		d.send(downloadResult{Op: op, Err: fmt.Errorf("stat remote %s: %w", op.Path, err)})
		return
	}
	if d.cancelled(ctx, op) {
		return
	}
	if stat == nil {
		// Treat as a delete: the inode vanished between the invalidation
		// dispatch and our follow-up read.
		op.Kind = opDownloadDelete
		d.processDelete(ctx, op)
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
	if d.cancelled(ctx, op) {
		return
	}
	hash := sha256Hex(data)

	if d.readonly {
		// Mark read-only files in readonly mode (0444).
		stat.Mode = 0o444
	}

	conflictPath, err := d.writeLocalFile(ctx, op, stat.Mode, func(file *os.File) error {
		_, err := file.Write(data)
		return err
	})
	if err != nil {
		d.send(downloadResult{Op: op, Err: fmt.Errorf("write local %s: %w", op.Path, err)})
		return
	}
	d.echo.markFile(op.Path, hash)

	d.send(downloadResult{
		Op:           op,
		RemoteHash:   hash,
		RemoteStat:   stat,
		ConflictPath: conflictPath,
		Mode:         stat.Mode,
		Size:         stat.Size,
		MtimeMs:      stat.Mtime,
	})
}

func (d *downloader) processChunkedFile(ctx context.Context, op downloadOp) {
	remotePath := absoluteRemotePath(op.Path)
	stat, err := d.fs.Stat(ctx, remotePath)
	if err != nil && !isClientNotFound(err) {
		d.send(downloadResult{Op: op, Err: fmt.Errorf("stat remote %s: %w", op.Path, err)})
		return
	}
	if d.cancelled(ctx, op) {
		return
	}
	if stat == nil {
		op.Kind = opDownloadDelete
		d.processDelete(ctx, op)
		return
	}

	// Fetch dirty chunks from remote.
	chunkData, err := d.fs.ReadChunks(ctx, remotePath, op.DirtyChunks, op.ChunkSize)
	if err != nil {
		d.send(downloadResult{Op: op, Err: fmt.Errorf("read chunks %s: %w", op.Path, err)})
		return
	}

	if d.cancelled(ctx, op) {
		return
	}

	mode := stat.Mode
	if d.readonly {
		mode = 0o444
	}

	// Patch a sibling file so cancellation cannot leave partial local bytes.
	conflictPath, err := d.writeLocalFile(ctx, op, mode, func(file *os.File) error {
		local, err := os.OpenFile(op.AbsPath, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err == nil {
			defer local.Close()
			info, err := local.Stat()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("chunk destination is not a regular file")
			}
			if _, err := io.Copy(file, local); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		for idx, data := range chunkData {
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, err := file.WriteAt(data, int64(idx)*int64(op.ChunkSize)); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if op.FileSize > 0 {
			return file.Truncate(op.FileSize)
		}
		return nil
	})
	if err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}

	hash := compositeHash(op.ChunkHashes)
	d.echo.markFile(op.Path, hash)

	d.send(downloadResult{
		Op:           op,
		RemoteHash:   hash,
		RemoteStat:   stat,
		ConflictPath: conflictPath,
		Mode:         mode,
		Size:         op.FileSize,
		MtimeMs:      stat.Mtime,
	})
}

func (d *downloader) processSymlink(ctx context.Context, op downloadOp) {
	if op.Symlink == "" {
		remotePath := absoluteRemotePath(op.Path)
		target, err := d.fs.Readlink(ctx, remotePath)
		if err != nil {
			d.send(downloadResult{Op: op, Err: err})
			return
		}
		op.Symlink = target
	}
	if d.cancelled(ctx, op) {
		return
	}
	if err := os.MkdirAll(filepath.Dir(op.AbsPath), 0o755); err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}
	if d.cancelled(ctx, op) {
		return
	}
	suffix, err := randomSuffix()
	if err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}
	tempPath := filepath.Join(filepath.Dir(op.AbsPath), "."+filepath.Base(op.AbsPath)+".afssync.tmp."+suffix)
	if err := os.Symlink(op.Symlink, tempPath); err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}
	defer os.Remove(tempPath)
	info, statErr := os.Lstat(op.AbsPath)
	if d.cancelled(ctx, op) {
		return
	}
	if statErr == nil && info.IsDir() {
		// Preserve the existing behavior for replacing an empty directory.
		if err := os.Remove(op.AbsPath); err != nil {
			d.send(downloadResult{Op: op, Err: err})
			return
		}
	}
	if err := os.Rename(tempPath, op.AbsPath); err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}
	d.echo.markSymlink(op.Path, op.Symlink)
	d.send(downloadResult{Op: op, Target: op.Symlink})
}

func (d *downloader) processMkdir(ctx context.Context, op downloadOp) {
	if d.cancelled(ctx, op) {
		return
	}
	if err := os.MkdirAll(op.AbsPath, 0o755); err != nil {
		d.send(downloadResult{Op: op, Err: fmt.Errorf("mkdir local %s: %w", op.Path, err)})
		return
	}
	d.echo.markDir(op.Path)
	d.send(downloadResult{Op: op})
}

func (d *downloader) processDelete(ctx context.Context, op downloadOp) {
	d.send(downloadResult{Op: op, Err: d.removeLocalFile(ctx, op)})
}

func (d *downloader) removeLocalFile(ctx context.Context, op downloadOp) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Lstat(op.AbsPath)
	if err != nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.echo.markDelete(op.Path)
	if info.IsDir() {
		_ = os.RemoveAll(op.AbsPath)
		return nil
	}
	_ = os.Remove(op.AbsPath)
	return nil
}

func (d *downloader) processChmod(ctx context.Context, op downloadOp) {
	if d.cancelled(ctx, op) {
		return
	}
	if err := os.Chmod(op.AbsPath, fs.FileMode(op.Mode&0o7777)); err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return
	}
	d.send(downloadResult{Op: op, Mode: op.Mode})
}

func (d *downloader) cancelled(ctx context.Context, op downloadOp) bool {
	if err := ctx.Err(); err != nil {
		d.send(downloadResult{Op: op, Err: err})
		return true
	}
	return false
}

// Stage bytes before replacing the destination. Cancelled downloads discard
// their temporary files without moving the local file to a conflict copy.
func (d *downloader) writeLocalFile(ctx context.Context, op downloadOp, mode uint32, write func(*os.File) error) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(op.AbsPath), 0o755); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	file, err := os.CreateTemp(filepath.Dir(op.AbsPath), "."+filepath.Base(op.AbsPath)+".afssync.tmp.*")
	if err != nil {
		return "", err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err := write(file); err != nil {
		return "", err
	}
	if err := file.Chmod(fs.FileMode(mode & 0o7777)); err != nil {
		return "", err
	}
	if err := file.Sync(); err != nil {
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var conflictPath string
	if op.Conflict {
		conflictPath, err = moveLocalToConflict(d.conflict, op.AbsPath)
		if err != nil {
			return "", err
		}
	}
	return conflictPath, os.Rename(file.Name(), op.AbsPath)
}

func (d *downloader) send(r downloadResult) {
	if d.results == nil {
		return
	}
	select {
	case d.results <- r:
	case <-d.stopCh:
		// The next generation inspects the actual local tree.
	}
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
