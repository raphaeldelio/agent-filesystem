package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/redis/agent-filesystem/internal/controlplane"
)

func syncSaveHistoryReconciler(t *testing.T, env *syncTestEnv) *reconciler {
	t.Helper()
	if err := env.cp.PutWorkspaceVersioningPolicy(context.Background(), env.workspace, controlplane.WorkspaceVersioningPolicy{Mode: controlplane.WorkspaceVersioningModeAll}); err != nil {
		t.Fatal(err)
	}
	r := newSyncSaveTestReconciler(t, env)
	r.chunkThreshold, r.chunkSize = 8, 4
	u := newUploader(env.fsClient, nil, r.maxFileBytes, false, nil)
	u.mountChangelog(env.rdb, env.workspace, "save-history", "user", "agent", "save", "test")
	r.recordChange = u.emitChange
	return r
}

func syncSaveHistoryOps(t *testing.T, env *syncTestEnv) map[string][]string {
	t.Helper()
	rows, err := env.rdb.XRange(context.Background(), controlplane.ChangelogStreamKey(env.workspace), "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	ops := make(map[string][]string)
	for _, row := range rows {
		if row.Values["session_id"] != "save-history" || row.Values["agent_id"] != "agent" || row.Values["source"] != controlplane.ChangeSourceAgentSync {
			t.Fatalf("missing save attribution: %+v", row.Values)
		}
		path := row.Values["path"].(string)
		ops[path] = append(ops[path], row.Values["op"].(string))
	}
	return ops
}

func TestSyncSaveHistoryTracksMutationsAndVersionsOnce(t *testing.T) {
	env := newSyncTestEnv(t)
	r := syncSaveHistoryReconciler(t, env)
	file := env.writeLocalFile(t, "dir/file", "first")
	large := "large saved file with exact bytes"
	env.writeLocalFile(t, "large", large)
	link := filepath.Join(env.localRoot, "link")
	if err := os.Symlink("dir/file", link); err != nil {
		t.Fatal(err)
	}
	linkInfo, err := os.Lstat(link)
	if err != nil {
		t.Fatal(err)
	}
	requireSyncSave(t, r)
	env.writeLocalFile(t, "dir/file", "second")
	requireSyncSave(t, r)
	if err := os.Chmod(file, 0o600); err != nil {
		t.Fatal(err)
	}
	requireSyncSave(t, r)
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("large", link); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Dir(file)); err != nil {
		t.Fatal(err)
	}
	requireSyncSave(t, r)
	// No new operations or versions for repeated verification of the same tree.
	requireSyncSave(t, r)
	want := map[string][]string{
		"dir": {"mkdir", "rmdir"}, "dir/file": {"put", "put", "chmod", "delete"},
		"large": {"put"}, "link": {"symlink", "delete", "symlink"},
	}
	if syncSaveMode(linkInfo.Mode()) != 0o777 {
		want["link"] = []string{"symlink", "chmod", "delete", "symlink", "chmod"}
	}
	if got := syncSaveHistoryOps(t, env); !reflect.DeepEqual(got, want) {
		t.Fatalf("save history = %v, want %v", got, want)
	}
	ctx := context.Background()
	for path, bodies := range map[string][]string{"/dir/file": {"first", "second", "second", ""}, "/large": {large}} {
		ids, err := env.cp.ListPathHistoryVersionIDs(ctx, env.workspace, path)
		if err != nil || len(ids) != len(bodies) {
			t.Fatalf("versions for %s = %v, %v", path, ids, err)
		}
		seen := map[string]int{}
		for _, id := range ids {
			version, err := env.cp.GetFileVersion(ctx, env.workspace, id)
			if err != nil || version.SessionID != "save-history" {
				t.Fatalf("version = %+v, %v", version, err)
			}
			body := ""
			if version.Op != controlplane.ChangeOpDelete {
				data, err := env.cp.GetBlob(ctx, env.workspace, version.BlobID)
				if err != nil || sha256Hex(data) != version.ContentHash {
					t.Fatalf("saved version bytes/hash = %q, %+v, %v", data, version, err)
				}
				body = string(data)
			}
			seen[body]++
		}
		expected := map[string]int{}
		for _, body := range bodies {
			expected[body]++
		}
		if !reflect.DeepEqual(seen, expected) {
			t.Fatalf("saved versions for %s = %v, want %v", path, seen, expected)
		}
	}
}

func TestSyncSaveHistoryRetainsPartialProgressWithoutDuplicateRetry(t *testing.T) {
	for _, failure := range []string{"later_write", "readback", "directory_chmod"} {
		t.Run(failure, func(t *testing.T) {
			env := newSyncTestEnv(t)
			r := syncSaveHistoryReconciler(t, env)
			fault := &syncSaveFaultClient{Client: env.fsClient}
			r.fs = fault
			injected := errors.New("temporary save failure")
			want := map[string][]string{"a": {"put"}, "z": {"put"}}
			if failure == "directory_chmod" {
				if err := os.Mkdir(filepath.Join(env.localRoot, "dir"), 0o700); err != nil {
					t.Fatal(err)
				}
				fault.chmod = func(context.Context, string, uint32) error { return injected }
				want = map[string][]string{"dir": {"mkdir", "chmod"}}
			} else {
				env.writeLocalFile(t, "a", "first saved")
				env.writeLocalFile(t, "z", "last saved")
				fault.write = func(ctx context.Context, path string, data []byte, mode uint32) error {
					if failure == "later_write" && path == "/z" {
						return injected
					}
					err := env.fsClient.EchoCreate(ctx, path, data, mode)
					if failure == "readback" {
						fault.cat = func(context.Context, string) ([]byte, error) { return nil, injected }
					}
					return err
				}
			}
			if _, err := scanAndSaveSyncTree(context.Background(), r); !errors.Is(err, injected) {
				t.Fatalf("save error = %v", err)
			}
			if len(syncSaveHistoryOps(t, env)) == 0 {
				t.Fatal("successful mutations before the failure were not recorded")
			}
			r.fs = env.fsClient
			requireSyncSave(t, r)
			requireSyncSave(t, r)
			if got := syncSaveHistoryOps(t, env); !reflect.DeepEqual(got, want) {
				t.Fatalf("history after retry = %v, want %v", got, want)
			}
		})
	}
}
