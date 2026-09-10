package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/agent-filesystem/mount/client"
)

// Pause only after Redis has stored the upload, while the uploader still owes
// its post-write metadata lookup and completion result.
type syncSaveInflightClient struct {
	client.Client
	stored   chan struct{}
	release  chan struct{}
	postStat chan error
	once     sync.Once
	afterErr error
}

func (c *syncSaveInflightClient) Stat(ctx context.Context, path string) (*client.StatResult, error) {
	stat, err := c.Client.Stat(ctx, path)
	if path == "/old" {
		select {
		case <-c.stored:
			select {
			case c.postStat <- err:
			default:
			}
		default:
		}
	}
	return stat, err
}

func (c *syncSaveInflightClient) Echo(ctx context.Context, path string, data []byte) error {
	if err := c.Client.Echo(ctx, path, data); err != nil {
		return err
	}
	if path == "/old" {
		c.once.Do(func() {
			close(c.stored)
			<-c.release
		})
		return c.afterErr
	}
	return nil
}

func TestSyncSaveRetainsInflightUploadBeforeLocalRename(t *testing.T) {
	for _, scenario := range []string{"completed_write", "real_remote_edit", "write_reply_error"} {
		t.Run(scenario, func(t *testing.T) {
			env := newSyncTestEnv(t)
			gate := &syncSaveInflightClient{Client: env.fsClient, stored: make(chan struct{}), release: make(chan struct{}), postStat: make(chan error, 1)}
			if scenario == "write_reply_error" {
				gate.afterErr = errors.New("injected ambiguous write reply")
			}
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
			old := env.writeLocalFile(t, "old", "local intent")
			d.reconciler.queueUpload(uploadOp{Kind: opUploadFile, Path: "old", AbsPath: old,
				Content: []byte("local intent"), LocalHash: sha256Hex([]byte("local intent")), Mode: 0o644})
			select {
			case <-gate.stored:
			case <-time.After(5 * time.Second):
				t.Fatal("upload never reached Redis")
			}
			if err := os.Rename(old, filepath.Join(env.localRoot, "new")); err != nil {
				t.Fatal(err)
			}
			if scenario == "real_remote_edit" {
				env.writeRemoteFile(t, "old", "peer intent")
			}
			done := make(chan syncControlResult, 1)
			go func() { done <- service.save(saveRequestForDaemon(d, 5*time.Second)) }()
			select {
			case <-d.uploader.stopCh:
			case <-time.After(5 * time.Second):
				t.Fatal("save never stopped the old generation")
			}
			release.Do(func() { close(gate.release) })
			var result syncControlResult
			select {
			case result = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("save did not join the completed upload")
			}
			if scenario == "completed_write" {
				if !result.Success {
					var statErr error
					select {
					case statErr = <-gate.postStat:
					default:
					}
					t.Fatalf("landed upload lost its completion baseline: %+v; post-write Stat error: %v", result, statErr)
				}
				if env.remoteExists(t, "old") || env.readRemoteFile(t, "new") != "local intent" {
					t.Fatal("save did not preserve the offline rename")
				}
			} else {
				if result.Success || !strings.Contains(result.Error, "conflict") {
					t.Fatalf("ambiguous or divergent remote state was accepted: %+v", result)
				}
				want := "local intent"
				if scenario == "real_remote_edit" {
					want = "peer intent"
				}
				if got := env.readRemoteFile(t, "old"); got != want {
					t.Fatalf("conflicting remote bytes changed: %q", got)
				}
			}
			if got := env.readLocalFile(t, "new"); got != "local intent" {
				t.Fatalf("save changed local intent: %q", got)
			}
		})
	}
}
