package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/agent-filesystem/mount/client"
)

// These diagnostic regressions run the real upload and reconciliation paths in
// controlled order, without starting goroutines. The ordering models the gap
// between an uploader writing Redis and the reconciler consuming its result.
func TestSyncRecoveryDoesNotConflictWithOwnPendingUpload(t *testing.T) {
	env, d := recoveryBaselineDiagnostic(t)
	ctx := context.Background()
	const rel, content = "package.py", "print('single writer')\n"
	abs := env.writeLocalFile(t, rel, content)
	oldTime := time.Unix(10, 0)
	if err := os.Chtimes(abs, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	d.reconciler.handleLocalEvent(ctx, LocalEvent{Path: rel, KindHint: "create"})
	op := nextDiagnosticUpload(t, d)
	d.uploader.processFile(ctx, op)
	res := <-d.reconciler.uploadResCh
	if res.Err != nil || res.Conflict {
		t.Fatalf("initial upload: %+v", res)
	}
	if got := env.readRemoteFile(t, rel); got != content {
		t.Fatalf("remote content = %q, want %q", got, content)
	}
	if _, exists := d.Snapshot().Entries[rel]; exists {
		t.Fatal("test requires the successful upload result not yet applied to SyncState")
	}
	t.Logf("before scan: identical local/remote bytes; local mtime=%d, remote mtime=%d; no stored baseline", oldTime.UnixMilli(), res.RemoteStat.Mtime)
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	copies, err := filepath.Glob(abs + ".conflict-*")
	if err != nil {
		t.Fatal(err)
	}
	for _, copyPath := range copies {
		data, err := os.ReadFile(copyPath)
		t.Logf("conflict copy %s = %q, read error=%v", filepath.Base(copyPath), data, err)
	}
	if len(copies) != 0 {
		t.Fatalf("recovery created %d conflict copies for an identical file uploaded by this daemon", len(copies))
	}
}

// Recovery defers a path represented by the upload queue until its baseline
// result is applied. It must not compete with that operation.
func TestSyncRecoveryDoesNotConflictWithOwnQueuedUpload(t *testing.T) {
	env, d := recoveryBaselineDiagnostic(t)
	ctx := context.Background()
	const rel = "package.py"
	abs := env.writeLocalFile(t, rel, "old content\n")
	oldTime := time.Unix(10, 0)
	if err := os.Chtimes(abs, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	const content = "new package content\n"
	env.writeLocalFile(t, rel, content)
	d.reconciler.handleLocalEvent(ctx, LocalEvent{Path: rel, KindHint: "write"})
	op := nextDiagnosticUpload(t, d)
	if !op.HasStored || op.StoredEntry.RemoteHash == op.LocalHash {
		t.Fatal("test requires a queued edit with the previous remote baseline")
	}
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := env.readRemoteFile(t, rel); got != "old content\n" {
		t.Fatalf("recovery raced the queued upload: remote = %q", got)
	}
	d.uploader.processFile(ctx, op)
	res := <-d.reconciler.uploadResCh
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	t.Logf("queued baseline=%s; intended local hash=%s; observed remote hash=%s; conflict=%v", op.StoredEntry.RemoteHash, op.LocalHash, res.RemoteHashSeen, res.Conflict)
	if res.Conflict {
		t.Fatal("queued uploader reports a conflict with recovery")
	}
}

func recoveryBaselineDiagnostic(t *testing.T) (*syncTestEnv, *syncDaemon) {
	t.Helper()
	env := newSyncTestEnv(t)
	d, err := newSyncDaemon(syncDaemonConfig{
		Workspace: env.workspace,
		LocalRoot: env.localRoot,
		FS:        env.fsClient,
		Store:     env.store,
	})
	if err != nil {
		t.Fatal(err)
	}
	return env, d
}

func nextDiagnosticUpload(t *testing.T, d *syncDaemon) uploadOp {
	t.Helper()
	select {
	case op := <-d.reconciler.uploadIn():
		return op
	default:
		t.Fatal("expected an upload operation")
		return uploadOp{}
	}
}

func TestSyncRecoveryDiscardsQueuedOlderLocalVersion(t *testing.T) {
	env, d := recoveryBaselineDiagnostic(t)
	ctx := context.Background()
	const rel = "package.py"
	env.writeLocalFile(t, rel, "baseline")
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	env.writeLocalFile(t, rel, "queued version")
	d.reconciler.handleLocalEvent(ctx, LocalEvent{Path: rel, KindHint: "write"})
	op := nextDiagnosticUpload(t, d)
	env.writeLocalFile(t, rel, "newest version on disk")
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	d.uploader.processFile(ctx, op)
	res := <-d.reconciler.uploadResCh
	if !res.Skipped || res.Conflict || res.Err != nil {
		t.Fatalf("old upload = %+v", res)
	}
	d.reconciler.handleUploadResult(ctx, res)
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := env.readRemoteFile(t, rel); got != "newest version on disk" {
		t.Fatalf("remote = %q", got)
	}
	if got := d.Snapshot().Entries[rel].RemoteHash; got != sha256Hex([]byte("newest version on disk")) {
		t.Fatalf("stored remote hash = %s", got)
	}
}

func TestSyncRecoveryWaitsForPendingResultBeforeNextEdit(t *testing.T) {
	env, d := recoveryBaselineDiagnostic(t)
	ctx := context.Background()
	const rel = "package.py"
	abs := env.writeLocalFile(t, rel, "first upload")
	stamp := time.Unix(10, 0)
	if err := os.Chtimes(abs, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	d.reconciler.handleLocalEvent(ctx, LocalEvent{Path: rel, KindHint: "create"})
	d.uploader.processFile(ctx, nextDiagnosticUpload(t, d))
	res := <-d.reconciler.uploadResCh
	if res.Err != nil || res.Conflict {
		t.Fatalf("upload: %+v", res)
	}
	// The pending result prevents recovery from adopting a transient baseline.
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	env.writeLocalFile(t, rel, "latest contents")
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, exists := d.Snapshot().Entries[rel]; exists {
		t.Fatal("recovery adopted a pending baseline")
	}
	d.reconciler.handleUploadResult(ctx, res)
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := env.readRemoteFile(t, rel); got != "latest contents" {
		t.Fatalf("remote = %q", got)
	}
	copies, _ := filepath.Glob(abs + ".conflict-*")
	if len(copies) != 0 {
		t.Fatalf("false conflicts = %v", copies)
	}
	select {
	case <-d.reconciler.fullSweepRequests():
	default:
		t.Fatal("stale result did not request another sweep")
	}
}

func TestSyncRecoveryUploadRecordsStagedLocalMtime(t *testing.T) {
	env, d := recoveryBaselineDiagnostic(t)
	ctx := context.Background()
	abs := env.writeLocalFile(t, "package.py", "staged bytes")
	stamp := time.Unix(10, 0)
	if err := os.Chtimes(abs, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	d.reconciler.handleLocalEvent(ctx, LocalEvent{Path: "package.py", KindHint: "create"})
	d.uploader.processFile(ctx, nextDiagnosticUpload(t, d))
	res := <-d.reconciler.uploadResCh
	if res.Err != nil || res.Conflict {
		t.Fatalf("upload: %+v", res)
	}
	env.writeLocalFile(t, "package.py", "new bytes while result was pending")
	d.reconciler.handleUploadResult(ctx, res)
	entry := d.Snapshot().Entries["package.py"]
	if entry.LocalMtimeMs != stamp.UnixMilli() || entry.RemoteMtimeMs != res.RemoteStat.Mtime {
		t.Fatalf("wrong staged baseline: %+v", entry)
	}
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := env.readRemoteFile(t, "package.py"); got != "new bytes while result was pending" {
		t.Fatalf("remote = %q", got)
	}
}

func TestSyncRecoveryRejectsChangedLocalPlan(t *testing.T) {
	for _, change := range []string{"edit", "chmod", "delete", "recreate"} {
		t.Run(change, func(t *testing.T) {
			env, d := recoveryBaselineDiagnostic(t)
			ctx := context.Background()
			abs := env.writeLocalFile(t, "file.txt", "original")
			if err := d.full.warmStart(ctx, nil); err != nil {
				t.Fatal(err)
			}
			env.writeRemoteFile(t, "file.txt", "remote edit")
			local, err := d.full.scanLocalMeta()
			if err != nil {
				t.Fatal(err)
			}
			remote, err := d.full.scanRemoteMeta(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			plan := d.full.buildPlan(ctx, local, remote)
			if len(plan) != 1 || plan[0].kind != "download" {
				t.Fatalf("plan = %+v", plan)
			}
			switch change {
			case "edit":
				env.writeLocalFile(t, "file.txt", "new local edit")
			case "chmod":
				if err := os.Chmod(abs, 0o700); err != nil {
					t.Fatal(err)
				}
			case "delete":
				if err := os.Remove(abs); err != nil {
					t.Fatal(err)
				}
			case "recreate":
				if err := os.Remove(abs); err != nil {
					t.Fatal(err)
				}
				env.writeLocalFile(t, "file.txt", "replacement")
			}
			before, _ := os.ReadFile(abs)
			if err := d.full.executePlan(ctx, plan, nil); err != nil {
				t.Fatal(err)
			}
			after, _ := os.ReadFile(abs)
			if string(after) != string(before) {
				t.Fatalf("stale plan changed local: %q -> %q", before, after)
			}
			if change == "chmod" {
				fi, _ := os.Stat(abs)
				if fi.Mode().Perm() != 0o700 {
					t.Fatal("stale plan replaced new mode")
				}
			}
			copies, _ := filepath.Glob(abs + ".conflict-*")
			if len(copies) != 0 {
				t.Fatalf("stale plan made conflict copies: %v", copies)
			}
			select {
			case <-d.reconciler.fullSweepRequests():
			default:
				t.Fatal("stale plan did not request another sweep")
			}
		})
	}
}

func TestSyncRecoveryRejectsRemoteEditAfterUploadPlan(t *testing.T) {
	env, d := recoveryBaselineDiagnostic(t)
	ctx := context.Background()
	env.writeLocalFile(t, "file.txt", "baseline")
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	env.writeLocalFile(t, "file.txt", "local edit")
	local, err := d.full.scanLocalMeta()
	if err != nil {
		t.Fatal(err)
	}
	remote, err := d.full.scanRemoteMeta(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan := d.full.buildPlan(ctx, local, remote)
	if len(plan) != 1 || plan[0].kind != "upload" {
		t.Fatalf("plan = %+v", plan)
	}
	env.writeRemoteFile(t, "file.txt", "another writer's newer edit")
	if err := d.full.executePlan(ctx, plan, nil); err != nil {
		t.Fatal(err)
	}
	if got := env.readRemoteFile(t, "file.txt"); got != "another writer's newer edit" {
		t.Fatalf("remote edit overwritten: %q", got)
	}
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	copies, _ := filepath.Glob(filepath.Join(env.localRoot, "file.txt.conflict-*"))
	if len(copies) != 1 {
		t.Fatalf("real conflict copies = %v", copies)
	}
	data, err := os.ReadFile(copies[0])
	if err != nil || string(data) != "local edit" {
		t.Fatalf("preserved local = %q, err = %v", data, err)
	}
}

func TestSyncRecoveryRefreshPreservesContentBaseline(t *testing.T) {
	env, d := recoveryBaselineDiagnostic(t)
	ctx := context.Background()
	env.writeLocalFile(t, "file.txt", "baseline")
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	before := d.Snapshot().Entries["file.txt"]
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	after := d.Snapshot().Entries["file.txt"]
	if after.LocalHash != before.LocalHash || after.RemoteHash != before.RemoteHash {
		t.Fatalf("refresh cleared hashes: %+v", after)
	}
}

type recoveryReadHook struct {
	client.Client
	afterRead func()
}

func (c *recoveryReadHook) Cat(ctx context.Context, path string) ([]byte, error) {
	data, err := c.Client.Cat(ctx, path)
	if c.afterRead != nil {
		hook := c.afterRead
		c.afterRead = nil
		hook()
	}
	return data, err
}

func TestSyncRecoveryRejectsEditDuringRemoteRead(t *testing.T) {
	env, d := recoveryBaselineDiagnostic(t)
	ctx := context.Background()
	env.writeLocalFile(t, "file.txt", "baseline")
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	env.writeRemoteFile(t, "file.txt", "remote edit")
	d.reconciler.fs = &recoveryReadHook{Client: d.reconciler.fs, afterRead: func() { env.writeLocalFile(t, "file.txt", "local edit during remote read") }}
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := env.readLocalFile(t, "file.txt"); got != "local edit during remote read" {
		t.Fatalf("new edit overwritten: %q", got)
	}
	select {
	case <-d.reconciler.fullSweepRequests():
	default:
		t.Fatal("read race did not request retry")
	}
}

func TestSyncRecoveryDefersPartiallyWrittenPendingUpload(t *testing.T) {
	env, d := recoveryBaselineDiagnostic(t)
	ctx := context.Background()
	const rel, contents = "package.py", "complete package contents"
	abs := env.writeLocalFile(t, rel, contents)
	d.reconciler.handleLocalEvent(ctx, LocalEvent{Path: rel, KindHint: "create"})
	op := nextDiagnosticUpload(t, d)
	// A chunked create can expose its new inode before WriteChunks completes.
	env.writeRemoteFile(t, rel, "")
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := env.readLocalFile(t, rel); got != contents {
		t.Fatalf("partial upload replaced local data: %q", got)
	}
	copies, _ := filepath.Glob(abs + ".conflict-*")
	if len(copies) != 0 {
		t.Fatalf("partial upload made conflict copies: %v", copies)
	}
	select {
	case <-d.reconciler.fullSweepRequests():
		t.Fatal("pending upload caused an immediate rescan loop")
	default:
	}
	d.uploader.processFile(ctx, op)
	d.reconciler.handleUploadResult(ctx, <-d.reconciler.uploadResCh)
	select {
	case <-d.reconciler.fullSweepRequests():
	default:
		t.Fatal("completed upload did not wake deferred scan")
	}
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := env.readRemoteFile(t, rel); got != contents {
		t.Fatalf("remote = %q", got)
	}
	if len(d.reconciler.pendingUploads) != 0 {
		t.Fatal("finished upload retained pending state")
	}
}

func TestSyncRecoveryDeferredSweepRetriesWithoutAnotherEvent(t *testing.T) {
	env := newSyncTestEnv(t)
	failed := &overflowScanFailure{failed: make(chan struct{})}
	d := env.startDaemon(t, func(cfg *syncDaemonConfig) {
		cfg.WatcherDebounce = time.Hour
		failed.Client = cfg.FS
		cfg.FS = failed
	})
	if err := d.watcher.w.Remove(env.localRoot); err != nil {
		t.Fatal(err)
	}
	env.writeLocalFile(t, ".venv/package.py", "preserve hidden content across deferred retry")
	failed.armed.Store(true)
	d.reconciler.requestFullSweep()
	select {
	case <-failed.failed:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep did not reach injected failure")
	}
	assertEventually(t, 5*time.Second, "deferred sweep failure to retry without another event", func() bool {
		got, err := env.fsClient.Cat(context.Background(), "/.venv/package.py")
		return err == nil && string(got) == "preserve hidden content across deferred retry"
	})
}

func TestSyncRecoveryDirectoryModesConverge(t *testing.T) {
	for _, side := range []string{"local", "remote"} {
		t.Run(side, func(t *testing.T) {
			env, d := recoveryBaselineDiagnostic(t)
			ctx := context.Background()
			abs := filepath.Join(env.localRoot, "directory")
			if err := os.Mkdir(abs, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := d.full.warmStart(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if side == "local" {
				if err := os.Chmod(abs, 0o700); err != nil {
					t.Fatal(err)
				}
			} else if err := env.fsClient.Chmod(ctx, "/directory", 0o700); err != nil {
				t.Fatal(err)
			}
			if err := d.full.warmStart(ctx, nil); err != nil {
				t.Fatal(err)
			}
			fi, err := os.Stat(abs)
			if err != nil {
				t.Fatal(err)
			}
			remote, err := env.fsClient.Stat(ctx, "/directory")
			if err != nil {
				t.Fatal(err)
			}
			if fi.Mode().Perm() != 0o700 || remote.Mode != 0o700 {
				t.Fatalf("modes did not converge: local=%o remote=%o", fi.Mode().Perm(), remote.Mode)
			}
		})
	}
}

func TestSyncRecoveryDefersBothPathsOfQueuedRename(t *testing.T) {
	env, d := recoveryBaselineDiagnostic(t)
	ctx := context.Background()
	oldPath := env.writeLocalFile(t, "old.txt", "contents preserved during rename")
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	// Record the local identity needed by normal rename pairing.
	d.reconciler.state.mu.Lock()
	entry := d.reconciler.state.state.Entries["old.txt"]
	entry.LocalIdentity = localFileIdentityFromPath(oldPath)
	d.reconciler.state.state.Entries["old.txt"] = entry
	d.reconciler.state.mu.Unlock()
	newPath := filepath.Join(env.localRoot, "new.txt")
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatal(err)
	}
	d.reconciler.handleLocalEvent(ctx, LocalEvent{Path: "old.txt", KindHint: "rename"})
	d.reconciler.handleLocalEvent(ctx, LocalEvent{Path: "new.txt", KindHint: "create"})
	op := nextDiagnosticUpload(t, d)
	if op.Kind != opUploadRename || !op.Tracked {
		t.Fatalf("expected tracked rename, got %+v", op)
	}
	if err := d.full.warmStart(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := env.readLocalFile(t, "new.txt"); got != "contents preserved during rename" {
		t.Fatalf("local rename lost: %q", got)
	}
	if got := env.readRemoteFile(t, "old.txt"); got != "contents preserved during rename" {
		t.Fatalf("remote source lost: %q", got)
	}
	d.uploader.process(ctx, op)
	res := <-d.reconciler.uploadResCh
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	d.reconciler.handleUploadResult(ctx, res)
	if len(d.reconciler.pendingUploads) != 0 {
		t.Fatalf("rename kept pending paths: %+v", d.reconciler.pendingUploads)
	}
	if got := env.readRemoteFile(t, "new.txt"); got != "contents preserved during rename" {
		t.Fatalf("remote rename = %q", got)
	}
}
