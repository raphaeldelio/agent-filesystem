package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/redis/agent-filesystem/mount/client"
)

func newSyncSaveTestReconciler(t *testing.T, env *syncTestEnv) *reconciler {
	t.Helper()
	ignore, err := loadSyncIgnore(env.localRoot)
	if err != nil {
		t.Fatal(err)
	}
	return &reconciler{
		root: env.localRoot, workspace: env.workspace, fs: env.fsClient,
		state:  newStateWriter(newSyncState(env.workspace, env.localRoot), time.Second),
		ignore: ignore, maxFileBytes: 16 * 1024 * 1024,
	}
}

// Direct engine tests supply the same scanned manifest as the save service.
func scanAndSaveSyncTree(ctx context.Context, r *reconciler) (syncSaveReceipt, error) {
	local, err := scanSyncSaveLocal(ctx, r)
	if err != nil {
		return syncSaveReceipt{}, err
	}
	return saveSyncTree(ctx, r, local)
}

func TestSyncSaveEngineRejectsEditsAfterManifestCapture(t *testing.T) {
	env := newSyncTestEnv(t)
	env.writeLocalFile(t, "file", "before")
	r := newSyncSaveTestReconciler(t, env)
	local, err := scanSyncSaveLocal(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	env.writeLocalFile(t, "file", "edited")
	receipt, err := saveSyncTree(context.Background(), r, local)
	if err == nil || !strings.Contains(err.Error(), "local tree changed during save") || receipt.TreeSHA256 != "" {
		t.Fatalf("changed manifest acknowledged: receipt=%+v, err=%v", receipt, err)
	}
	if env.remoteExists(t, "file") {
		t.Fatal("save uploaded a file changed after manifest capture")
	}
}

func requireSyncSave(t *testing.T, r *reconciler) syncSaveReceipt {
	t.Helper()
	receipt, err := scanAndSaveSyncTree(context.Background(), r)
	if err != nil {
		t.Fatalf("saveSyncTree: %v", err)
	}
	if receipt.TreeSHA256 == "" || receipt.CompletedAt.IsZero() {
		t.Fatalf("incomplete receipt: %+v", receipt)
	}
	return receipt
}

// These tests never start a watcher: every change must be discovered by save.
func TestSyncSaveEngineIncludesMissedEventsAndLargeFiles(t *testing.T) {
	env := newSyncTestEnv(t)
	large := bytes.Repeat([]byte{0, 255, 17, 128, 10}, (1<<20)/5+17)
	abs := env.writeLocalFile(t, ".venv/pkg/native.so", string(large))
	if err := os.Chmod(abs, 0o751); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Dir(abs), 0o710); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing-target", filepath.Join(env.localRoot, ".venv", "dangling")); err != nil {
		t.Fatal(err)
	}
	env.writeLocalFile(t, ".afsignore", "ignored/\n")
	env.writeLocalFile(t, "ignored/secret", "excluded")
	env.writeLocalFile(t, ".afs-sync/requests/test.json", "excluded control request")
	r := newSyncSaveTestReconciler(t, env)
	receipt := requireSyncSave(t, r)
	if receipt.Entries != 4 || receipt.Files != 1 || receipt.Bytes != int64(len(large)) {
		t.Fatalf("receipt = %+v", receipt)
	}
	if got := env.readRemoteFile(t, ".venv/pkg/native.so"); !bytes.Equal([]byte(got), large) {
		t.Fatal("large file bytes differ")
	}
	for rel, mode := range map[string]uint32{".venv/pkg": 0o710, ".venv/pkg/native.so": 0o751} {
		st, err := env.fsClient.Stat(context.Background(), absoluteRemotePath(rel))
		if err != nil || st == nil || st.Mode != mode {
			t.Fatalf("Stat %s = %+v, %v; want mode %o", rel, st, err, mode)
		}
	}
	if got, err := env.fsClient.Readlink(context.Background(), "/.venv/dangling"); err != nil || got != "missing-target" {
		t.Fatalf("Readlink = %q, %v", got, err)
	}
	if env.remoteExists(t, "ignored") || env.remoteExists(t, ".afs-sync") {
		t.Fatal("ignored paths were uploaded")
	}
	second := requireSyncSave(t, r)
	if second.TreeSHA256 != receipt.TreeSHA256 {
		t.Fatal("unchanged tree receipt hash changed")
	}
	persisted, err := loadSyncState(env.workspace)
	if err != nil {
		t.Fatal(err)
	}
	entry := persisted.Entries[".venv/pkg/native.so"]
	st, err := env.fsClient.Stat(context.Background(), "/.venv/pkg/native.so")
	if err != nil {
		t.Fatal(err)
	}
	if entry.RemoteHash != compositeHash(entry.ChunkHashes) || entry.ChunkSize != defaultChunkSize || entry.RemoteMtimeMs != st.Mtime {
		t.Fatalf("incorrect verified baseline: %+v", entry)
	}
}

func TestSyncSaveEngineEditsDeletesRenamesModesAndTypes(t *testing.T) {
	env := newSyncTestEnv(t)
	edit := env.writeLocalFile(t, "edit", "before")
	env.writeLocalFile(t, "gone/child", "deleted")
	env.writeLocalFile(t, "old", "renamed")
	env.writeLocalFile(t, "file-to-dir", "old file")
	env.writeLocalFile(t, "dir-to-file/child", "old dir")
	if err := os.Symlink("edit", filepath.Join(env.localRoot, "link")); err != nil {
		t.Fatal(err)
	}
	r := newSyncSaveTestReconciler(t, env)
	requireSyncSave(t, r)
	info, err := os.Stat(edit)
	if err != nil {
		t.Fatal(err)
	}
	env.writeLocalFile(t, "edit", "after!")
	if err := os.Chtimes(edit, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(edit, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(env.localRoot, "gone")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(env.localRoot, "old"), filepath.Join(env.localRoot, "new")); err != nil {
		t.Fatal(err)
	}
	// A provisional rename baseline must not turn a missing remote destination
	// into a local deletion. No remote rename or upload has occurred here.
	r.state.state.Entries["new"] = r.state.state.Entries["old"]
	if err := os.Remove(filepath.Join(env.localRoot, "file-to-dir")); err != nil {
		t.Fatal(err)
	}
	env.writeLocalFile(t, "file-to-dir/child", "new dir")
	if err := os.RemoveAll(filepath.Join(env.localRoot, "dir-to-file")); err != nil {
		t.Fatal(err)
	}
	env.writeLocalFile(t, "dir-to-file", "new file")
	if err := os.Remove(filepath.Join(env.localRoot, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("new", filepath.Join(env.localRoot, "link")); err != nil {
		t.Fatal(err)
	}
	requireSyncSave(t, r)
	if got := env.readRemoteFile(t, "edit"); got != "after!" {
		t.Fatalf("same size/mtime edit = %q", got)
	}
	if env.remoteExists(t, "gone") || env.remoteExists(t, "old") {
		t.Fatal("offline deletion/rename left remote extras")
	}
	if got := env.readRemoteFile(t, "new"); got != "renamed" {
		t.Fatalf("rename content = %q", got)
	}
	if got := env.readRemoteFile(t, "file-to-dir/child"); got != "new dir" {
		t.Fatalf("replacement directory = %q", got)
	}
	if got := env.readRemoteFile(t, "dir-to-file"); got != "new file" {
		t.Fatalf("replacement file = %q", got)
	}
}

func TestSyncSaveEngineCompositeBaselineUsesActualRemoteBytes(t *testing.T) {
	env := newSyncTestEnv(t)
	env.writeLocalFile(t, "file", "abcdefghijk")
	r := newSyncSaveTestReconciler(t, env)
	requireSyncSave(t, r)
	entry := r.state.state.Entries["file"]
	entry.ChunkSize = 4
	entry.ChunkHashes = []string{sha256Hex([]byte("abcd")), sha256Hex([]byte("efgh")), sha256Hex([]byte("ijk"))}
	entry.LocalHash, entry.RemoteHash = compositeHash(entry.ChunkHashes), compositeHash(entry.ChunkHashes)
	r.state.state.Entries["file"] = entry
	env.writeLocalFile(t, "file", "updated content")
	requireSyncSave(t, r)
	if got := env.readRemoteFile(t, "file"); got != "updated content" {
		t.Fatalf("content = %q", got)
	}
}

func TestSyncSaveEngineRejectsConflictsBeforeAnyApply(t *testing.T) {
	for _, scenario := range []string{"remote_edit", "remote_mode", "remote_only", "deleted_remote_edit", "missing_hash_baseline"} {
		t.Run(scenario, func(t *testing.T) {
			env := newSyncTestEnv(t)
			env.writeLocalFile(t, "tracked", "baseline")
			r := newSyncSaveTestReconciler(t, env)
			requireSyncSave(t, r)
			env.writeLocalFile(t, "would-create", "must not upload before conflict")
			switch scenario {
			case "remote_edit":
				env.writeLocalFile(t, "tracked", "local intent")
				env.writeRemoteFile(t, "tracked", "remote intent")
			case "remote_mode":
				env.writeLocalFile(t, "tracked", "local intent")
				if err := env.fsClient.Chmod(context.Background(), "/tracked", 0o700); err != nil {
					t.Fatal(err)
				}
			case "remote_only":
				env.writeRemoteFile(t, "extra", "peer data")
			case "deleted_remote_edit":
				if err := os.Remove(filepath.Join(env.localRoot, "tracked")); err != nil {
					t.Fatal(err)
				}
				env.writeRemoteFile(t, "tracked", "remote intent")
			case "missing_hash_baseline":
				entry := r.state.state.Entries["tracked"]
				entry.RemoteHash = ""
				r.state.state.Entries["tracked"] = entry
				env.writeLocalFile(t, "tracked", "local intent")
			}
			localBefore, err := scanSyncSaveLocal(context.Background(), r)
			if err != nil {
				t.Fatal(err)
			}
			remoteBefore, err := scanSyncSaveRemote(context.Background(), r, r.state.snapshot())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := scanAndSaveSyncTree(context.Background(), r); err == nil || !strings.Contains(err.Error(), "conflict") {
				t.Fatalf("save error = %v, want conflict", err)
			}
			localAfter, _ := scanSyncSaveLocal(context.Background(), r)
			remoteAfter, _ := scanSyncSaveRemote(context.Background(), r, r.state.snapshot())
			if err := compareSyncSaveTrees(localBefore, localAfter); err != nil {
				t.Fatalf("save changed local intent: %v", err)
			}
			if err := compareSyncSaveTrees(remoteBefore, remoteAfter); err != nil {
				t.Fatalf("save changed remote data before rejecting conflict: %v", err)
			}
		})
	}
}

type syncSaveFaultClient struct {
	client.Client
	write  func(context.Context, string, []byte, uint32) error
	cat    func(context.Context, string) ([]byte, error)
	chmod  func(context.Context, string, uint32) error
	remove func(context.Context, string) error
}

func (c *syncSaveFaultClient) EchoCreate(ctx context.Context, p string, data []byte, mode uint32) error {
	if c.write != nil {
		return c.write(ctx, p, data, mode)
	}
	return c.Client.EchoCreate(ctx, p, data, mode)
}
func (c *syncSaveFaultClient) Cat(ctx context.Context, p string) ([]byte, error) {
	if c.cat != nil {
		return c.cat(ctx, p)
	}
	return c.Client.Cat(ctx, p)
}
func (c *syncSaveFaultClient) Chmod(ctx context.Context, p string, mode uint32) error {
	if c.chmod != nil {
		return c.chmod(ctx, p, mode)
	}
	return c.Client.Chmod(ctx, p, mode)
}
func (c *syncSaveFaultClient) Rm(ctx context.Context, p string) error {
	if c.remove != nil {
		return c.remove(ctx, p)
	}
	return c.Client.Rm(ctx, p)
}

func TestSyncSaveEngineFailsThenRetries(t *testing.T) {
	for _, operation := range []string{"write", "directory_chmod", "delete", "readback", "nil_without_write", "persistence"} {
		t.Run(operation, func(t *testing.T) {
			env := newSyncTestEnv(t)
			env.writeLocalFile(t, "file", "before")
			env.writeLocalFile(t, "dir/child", "directory child")
			r := newSyncSaveTestReconciler(t, env)
			requireSyncSave(t, r)
			fault := &syncSaveFaultClient{Client: env.fsClient}
			r.fs = fault
			injected := errors.New("injected save failure")
			switch operation {
			case "write":
				env.writeLocalFile(t, "file", "after")
				fault.write = func(context.Context, string, []byte, uint32) error { return injected }
			case "directory_chmod":
				if err := os.Chmod(filepath.Join(env.localRoot, "dir"), 0o700); err != nil {
					t.Fatal(err)
				}
				fault.chmod = func(context.Context, string, uint32) error { return injected }
			case "delete":
				if err := os.Remove(filepath.Join(env.localRoot, "file")); err != nil {
					t.Fatal(err)
				}
				fault.remove = func(context.Context, string) error { return injected }
			case "readback":
				env.writeLocalFile(t, "file", "after")
				fault.write = func(ctx context.Context, p string, data []byte, mode uint32) error {
					err := env.fsClient.EchoCreate(ctx, p, data, mode)
					fault.cat = func(context.Context, string) ([]byte, error) { return nil, injected }
					return err
				}
			case "nil_without_write":
				env.writeLocalFile(t, "file", "after")
				fault.write = func(context.Context, string, []byte, uint32) error { return nil }
			case "persistence":
				if err := os.RemoveAll(syncStateDir()); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(syncStateDir(), []byte("blocked"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if receipt, err := scanAndSaveSyncTree(context.Background(), r); err == nil || receipt.TreeSHA256 != "" {
				t.Fatalf("failed operation returned receipt %+v, error %v", receipt, err)
			}
			r.fs = env.fsClient
			if operation == "persistence" {
				if err := os.Remove(syncStateDir()); err != nil {
					t.Fatal(err)
				}
			}
			requireSyncSave(t, r)
		})
	}
}

func TestSyncSaveEnginePreflightRejectsOversizeAndUnsupported(t *testing.T) {
	for _, scenario := range []string{"oversize", "fifo", "missing_root", "readonly"} {
		t.Run(scenario, func(t *testing.T) {
			env := newSyncTestEnv(t)
			env.writeLocalFile(t, "a-would-upload", "small")
			r := newSyncSaveTestReconciler(t, env)
			switch scenario {
			case "oversize":
				r.maxFileBytes = 8
				env.writeLocalFile(t, "z-large", "larger than cap")
			case "fifo":
				if err := syscall.Mkfifo(filepath.Join(env.localRoot, "z-pipe"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "missing_root":
				if err := os.RemoveAll(env.localRoot); err != nil {
					t.Fatal(err)
				}
			case "readonly":
				r.readonly = true
			}
			if _, err := scanAndSaveSyncTree(context.Background(), r); err == nil {
				t.Fatal("save succeeded despite failed preflight")
			}
			if env.remoteExists(t, "a-would-upload") {
				t.Fatal("preflight failure occurred after remote mutation")
			}
		})
	}
}

func TestSyncSaveEngineCancellationAndLocalChange(t *testing.T) {
	for _, scenario := range []string{"cancelled", "timeout", "local_change", "remote_extra_during_apply"} {
		t.Run(scenario, func(t *testing.T) {
			env := newSyncTestEnv(t)
			env.writeLocalFile(t, "file", "intent")
			r := newSyncSaveTestReconciler(t, env)
			fault := &syncSaveFaultClient{Client: env.fsClient}
			r.fs = fault
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			switch scenario {
			case "cancelled":
				cancel()
			case "timeout":
				fault.write = func(ctx context.Context, _ string, _ []byte, _ uint32) error { <-ctx.Done(); return ctx.Err() }
			case "local_change":
				fault.write = func(ctx context.Context, p string, data []byte, mode uint32) error {
					if err := env.fsClient.EchoCreate(ctx, p, data, mode); err != nil {
						return err
					}
					return os.WriteFile(filepath.Join(env.localRoot, "file"), []byte("newest"), 0o644)
				}
			case "remote_extra_during_apply":
				fault.write = func(ctx context.Context, p string, data []byte, mode uint32) error {
					if err := env.fsClient.EchoCreate(ctx, p, data, mode); err != nil {
						return err
					}
					return env.fsClient.EchoCreate(ctx, "/peer-extra", []byte("peer"), 0o644)
				}
			}
			if receipt, err := scanAndSaveSyncTree(ctx, r); err == nil || receipt.TreeSHA256 != "" {
				t.Fatalf("unsafe receipt %+v, error %v", receipt, err)
			}
			if scenario == "local_change" && env.readLocalFile(t, "file") != "newest" {
				t.Fatal("save reverted application edit")
			}
		})
	}
}

func TestSyncSaveEngineResumesChunkedEditsWithoutFalseConflict(t *testing.T) {
	for _, scenario := range []string{"unchanged", "changed_during_save", "large_to_small_to_large", "small_to_large"} {
		t.Run(scenario, func(t *testing.T) {
			env := newSyncTestEnv(t)
			abs := env.writeLocalFile(t, "file", "abcdefghijklmnop")
			env.writeRemoteFile(t, "file", "abcdefghijklmnop")
			hashes := []string{sha256Hex([]byte("abcd")), sha256Hex([]byte("efgh")), sha256Hex([]byte("ijkl")), sha256Hex([]byte("mnop"))}
			if err := env.fsClient.WriteChunks(context.Background(), "/file", nil, 4, 16, hashes); err != nil {
				t.Fatal(err)
			}
			r := newSyncSaveTestReconciler(t, env)
			r.chunkSize, r.chunkThreshold = 4, 8
			r.uploadCh = make(chan uploadOp, 1)
			requireSyncSave(t, r)
			want := "abcdEDITijklmnop"
			switch scenario {
			case "changed_during_save":
				env.writeLocalFile(t, "file", "ABCDefghijklmnop")
				requireSyncSave(t, r)
				want = "ABCDEDITijklmnop"
			case "large_to_small_to_large", "small_to_large":
				env.writeLocalFile(t, "file", "small")
				requireSyncSave(t, r)
				cs, hs, err := env.fsClient.ChunkMeta(context.Background(), "/file")
				if err != nil || cs != 0 || len(hs) != 0 {
					t.Fatalf("small saved file retains chunk metadata: %d %v %v", cs, hs, err)
				}
				if scenario == "small_to_large" {
					env.writeLocalFile(t, "file", "abcdefghijklmnop")
					requireSyncSave(t, r)
				}
			}
			env.writeLocalFile(t, "file", want)
			info, err := os.Stat(abs)
			if err != nil {
				t.Fatal(err)
			}
			r.handleLocalFile("file", abs, info)
			op := <-r.uploadCh
			results := make(chan uploadResult, 1)
			uploader := newUploader(env.fsClient, results, r.maxFileBytes, false, newSyncLogger(false))
			uploader.process(context.Background(), op)
			result := <-results
			if result.Err != nil || result.Conflict {
				t.Fatalf("post-save chunked edit: error=%v conflict=%v baseline=%+v", result.Err, result.Conflict, op.StoredEntry)
			}
			if got := env.readRemoteFile(t, "file"); got != want {
				t.Fatalf("post-save remote bytes = %q, want %q", got, want)
			}
		})
	}
}
