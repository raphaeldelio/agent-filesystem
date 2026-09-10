package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// The supervisor outlives individual worker generations. Requests are polled
// independently of fsnotify so an overflowing event queue cannot hide a save.
// Only this goroutine owns the active generation and writes save responses.
type syncSaveService struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	active *syncDaemon
	// The mount remains addressable if a worker generation cannot restart.
	workspace string
	localRoot string
}

func startSyncSaveService(ctx context.Context, daemon *syncDaemon) *syncSaveService {
	ctx, cancel := context.WithCancel(ctx)
	service := &syncSaveService{ctx: ctx, cancel: cancel, done: make(chan struct{}), active: daemon,
		workspace: daemon.cfg.Workspace, localRoot: daemon.cfg.LocalRoot}
	go service.run()
	return service
}

func (s *syncSaveService) Stop() {
	s.cancel()
	<-s.done
}

func (s *syncSaveService) run() {
	defer close(s.done)
	defer func() {
		if s.active != nil {
			s.active.Stop()
		}
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.poll()
		}
	}
}

func (s *syncSaveService) poll() {
	s.rememberMountIdentity()
	if s.localRoot == "" {
		return
	}
	root := s.localRoot
	entries, err := os.ReadDir(filepath.Join(root, syncControlRequestsDirName))
	if err != nil {
		return
	}
	for _, entry := range entries {
		if s.ctx.Err() != nil {
			return
		}
		rel := filepath.Join(syncControlRequestsDirName, entry.Name())
		id, ok := syncControlRequestID(rel)
		if !ok || entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() > 64*1024 {
			continue
		}
		path := filepath.Join(root, rel)
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var request syncControlRequest
		if json.Unmarshal(raw, &request) != nil || request.Operation != syncControlOpSave {
			continue
		}
		result := s.save(request)
		// Remove the request first so a failed response write cannot repeat the save.
		_ = os.Remove(path)
		if err := writeSyncControlJSON(syncControlResultPath(root, id), result, 0o600); err != nil {
			fmt.Fprintf(os.Stderr, "afs sync: write save response: %v\n", err)
		}
	}
}

func (s *syncSaveService) rememberMountIdentity() {
	if s.localRoot == "" && s.active != nil {
		s.workspace = s.active.cfg.Workspace
		s.localRoot = s.active.cfg.LocalRoot
	}
}

func (s *syncSaveService) save(request syncControlRequest) syncControlResult {
	s.rememberMountIdentity()
	result := syncControlResult{Version: syncControlVersion, Operation: syncControlOpSave,
		Volume: request.Volume, LocalRoot: request.LocalRoot}
	fail := func(err error) syncControlResult { result.Error = err.Error(); return result }
	if request.Version != syncControlVersion {
		return fail(errors.New("unsupported sync save version"))
	}
	if request.Volume != s.workspace || filepath.Clean(request.LocalRoot) != s.localRoot {
		return fail(errors.New("save request does not match the mounted volume and local root"))
	}
	daemon := s.active
	if daemon == nil {
		return fail(errors.New("sync daemon is unavailable"))
	}
	if request.Path != "" || request.Content != "" || request.VersionID != "" || request.FileID != "" || request.Ordinal != 0 {
		return fail(errors.New("save operates on the entire mounted sync volume"))
	}
	if daemon.cfg.Readonly {
		return fail(errors.New("cannot save a read-only sync mount"))
	}
	if request.DeadlineUnixMilli <= 0 {
		return fail(errors.New("save requires a deadline"))
	}
	deadline := time.UnixMilli(request.DeadlineUnixMilli)
	if !time.Now().Before(deadline) {
		return fail(context.DeadlineExceeded)
	}
	ctx, cancel := context.WithDeadline(s.ctx, deadline)
	defer cancel()

	// Preserve the caller's local tree across the drain. An old inbound write
	// must never become the tree that this save silently acknowledges.
	beforeDrain, err := scanSyncSaveLocal(ctx, daemon.reconciler)
	if err != nil {
		return fail(fmt.Errorf("scan local tree before save: %w", err))
	}

	// Cancelling the old generation stops producers as well as workers. Join
	// delayed delete senders and in-flight writes before inspecting the tree.
	// If a client has already timed out, no later success or new save is started.
	daemon.StopForSave(ctx)
	s.active = nil
	s.applyStoppedUploadResults(daemon)
	var receipt syncSaveReceipt
	afterDrain, saveErr := scanSyncSaveLocal(ctx, daemon.reconciler)
	if saveErr == nil {
		saveErr = compareSyncSaveTrees(beforeDrain, afterDrain)
		if saveErr != nil {
			saveErr = fmt.Errorf("local tree changed while pausing sync: %w", saveErr)
		}
	}
	if saveErr == nil {
		receipt, saveErr = saveSyncTree(ctx, daemon.reconciler, afterDrain)
	}
	if ctx.Err() != nil {
		saveErr = ctx.Err()
	}

	// Resume with new channels, watcher, subscription and workers. Old queued
	// operations must never run after a successful save receipt.
	if s.ctx.Err() == nil {
		info, err := os.Lstat(daemon.cfg.LocalRoot)
		if err == nil && !info.IsDir() {
			err = errors.New("local root is no longer a directory")
		}
		var fresh *syncDaemon
		if err == nil {
			fresh, err = newSyncDaemon(daemon.cfg)
		}
		if err == nil {
			fresh.stateWriter.state = daemon.Snapshot()
			fresh.stateWriter.dirty = true
			err = fresh.StartSteadyStateOnly(s.ctx)
		}
		if err != nil {
			saveErr = errors.Join(saveErr, fmt.Errorf("could not resume sync: %w", err))
		} else {
			s.active = fresh
		}
	}
	if saveErr != nil {
		return fail(saveErr)
	}
	if ctx.Err() != nil {
		return fail(ctx.Err())
	}
	result.Success = true
	result.Save = &receipt
	return result
}

// Successful uploads whose result was waiting when shutdown began still
// establish ownership of remote paths. In particular, a renamed temporary file
// may otherwise look like an unrelated remote-only file to the strict save.
func (s *syncSaveService) applyStoppedUploadResults(d *syncDaemon) {
	apply := func(result uploadResult) {
		if result.Err != nil || result.Conflict {
			return
		}
		if result.RemoteStat != nil {
			d.stateWriter.mu.Lock()
			existing, ok := d.stateWriter.state.Entries[result.Op.Path]
			d.stateWriter.mu.Unlock()
			if ok && existing.RemoteMtimeMs > result.RemoteStat.Mtime {
				return
			}
		}
		d.reconciler.handleUploadResult(context.Background(), result)
	}
	for {
		select {
		case result := <-d.reconciler.uploadResCh:
			apply(result)
		default:
			goto retained
		}
	}
retained:
	for _, result := range d.uploader.stoppedResults {
		apply(result)
	}
	// Cancelled rename candidates cannot enqueue again; their tombstones remain
	// available to the save planner for detecting offline deletes.
	d.reconciler.renameMu.Lock()
	d.reconciler.renameCandidates = make(map[string]renameCandidate)
	d.reconciler.renameMu.Unlock()
}
