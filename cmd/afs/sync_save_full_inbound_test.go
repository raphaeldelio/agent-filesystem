package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/agent-filesystem/mount/client"
)

type syncSaveFullInboundClient struct {
	client.Client
	armed   atomic.Bool
	read    chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *syncSaveFullInboundClient) Cat(ctx context.Context, path string) ([]byte, error) {
	data, err := c.Client.Cat(ctx, path)
	if err == nil && path == "/file" && c.armed.Load() {
		c.once.Do(func() {
			close(c.read)
			<-c.release
		})
	}
	return data, err
}

func TestSyncSaveStopsFullInboundReadBeforeLocalMutation(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		name := "already_equal"
		if conflict {
			name = "new_local_edit"
		}
		t.Run(name, func(t *testing.T) {
			env := newSyncTestEnv(t)
			env.writeLocalFile(t, "file", "baseline A")
			gate := &syncSaveFullInboundClient{Client: env.fsClient, read: make(chan struct{}), release: make(chan struct{})}
			var release sync.Once
			defer release.Do(func() { close(gate.release) })
			d := env.startDaemon(t, func(cfg *syncDaemonConfig) { cfg.FS = gate })
			if err := d.watcher.Close(); err != nil {
				t.Fatal(err)
			}
			service := &syncSaveService{ctx: context.Background(), active: d}
			t.Cleanup(func() {
				if service.active != nil {
					service.active.Stop()
				}
			})
			gate.armed.Store(true)
			env.writeRemoteFile(t, "file", "remote planned B")
			d.reconciler.requestFullSweep()
			select {
			case <-gate.read:
			case <-time.After(5 * time.Second):
				t.Fatal("full reconciliation did not reach its inbound read")
			}
			want := "remote planned B"
			if conflict {
				want = "new local C"
			}
			env.writeLocalFile(t, "file", want)
			done := make(chan syncControlResult, 1)
			go func() { done <- service.save(saveRequestForDaemon(d, 5*time.Second)) }()
			select {
			case <-d.uploader.stopCh:
			case <-time.After(5 * time.Second):
				t.Fatal("save did not stop generation work")
			}
			release.Do(func() { close(gate.release) })
			var result syncControlResult
			select {
			case result = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("save did not join its inbound read")
			}
			if got := env.readLocalFile(t, "file"); got != want {
				t.Fatalf("save allowed a stale planned download to replace local intent: got %q want %q; result=%+v", got, want, result)
			}
			if got := env.readRemoteFile(t, "file"); got != "remote planned B" {
				t.Fatalf("save changed the remote conflict: %q", got)
			}
			if conflict {
				if result.Success || !strings.Contains(result.Error, "conflict") {
					t.Fatalf("true conflict accepted: %+v", result)
				}
			} else if !result.Success {
				t.Fatalf("equal local and remote tree rejected: %+v", result)
			}
		})
	}
}

func TestSyncSaveStopsUnstartedFullActions(t *testing.T) {
	for _, kind := range []string{"mkdir-local", "mkdir-remote", "download", "upload", "symlink-download", "symlink-upload", "delete-local", "delete-remote"} {
		t.Run(kind, func(t *testing.T) {
			env := newSyncTestEnv(t)
			abs := env.writeLocalFile(t, "existing", "preserve")
			env.writeRemoteFile(t, "existing", "preserve")
			r := newSyncSaveTestReconciler(t, env)
			stop := make(chan struct{})
			close(stop)
			r.stopCh = stop
			action := syncAction{kind: kind, path: "new", absPath: filepath.Join(env.localRoot, "new"), mode: 0o644, target: "existing"}
			if strings.HasPrefix(kind, "delete-") {
				action.path, action.absPath = "existing", abs
			}
			if kind == "upload" {
				action.absPath = abs
			}
			err := newFullReconciler(r).executePlan(context.Background(), []syncAction{action}, nil)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("executePlan error = %v, want generation cancellation", err)
			}
			if env.localExists("new") || env.remoteExists(t, "new") || !env.localExists("existing") || !env.remoteExists(t, "existing") {
				t.Fatal("stopped full plan mutated the tree")
			}
		})
	}
}

func TestSyncSaveStopsBulkMaterializationAfterManifestRead(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "cold_start"
		if replace {
			name = "root_replace"
		}
		t.Run(name, func(t *testing.T) {
			env := newSyncTestEnv(t)
			env.writeRemoteFile(t, "file", "remote")
			r := newSyncSaveTestReconciler(t, env)
			r.store = env.store
			stop := make(chan struct{})
			r.stopCh = stop
			var once sync.Once
			progress := func(_, _ int64) {
				once.Do(func() {
					env.writeLocalFile(t, "new-local", "preserve")
					close(stop)
				})
			}
			f := newFullReconciler(r)
			var err error
			if replace {
				err = f.replaceFromRemote(context.Background(), progress)
			} else {
				err = f.coldStart(context.Background(), progress)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("bulk materialization error = %v, want generation cancellation", err)
			}
			if data, err := os.ReadFile(filepath.Join(env.localRoot, "new-local")); err != nil || string(data) != "preserve" {
				t.Fatalf("bulk materialization removed local intent: %q, %v", data, err)
			}
			if env.localExists("file") {
				t.Fatal("bulk materialization started after the generation stopped")
			}
		})
	}
}
