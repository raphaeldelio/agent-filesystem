package main

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type syncSaveFullDrainClient struct {
	*syncSaveInflightClient
	chmodDone chan error
}

func (c *syncSaveFullDrainClient) Chmod(ctx context.Context, path string, mode uint32) error {
	err := c.Client.Chmod(ctx, path, mode)
	if path == "/old" {
		select {
		case c.chmodDone <- err:
		default:
		}
	}
	return err
}

func TestSyncSaveJoinsInflightFullReconciliation(t *testing.T) {
	env := newSyncTestEnv(t)
	gate := &syncSaveFullDrainClient{
		syncSaveInflightClient: &syncSaveInflightClient{
			Client: env.fsClient, stored: make(chan struct{}), release: make(chan struct{}), postStat: make(chan error, 1),
		},
		chmodDone: make(chan error, 1),
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
	old := env.writeLocalFile(t, "old", "full reconciliation intent")
	// Echo creates at 0644. Cancelling the following Chmod leaves a remote
	// file that cannot be proved equal to the full reconciler's 0751 baseline.
	if err := os.Chmod(old, 0o751); err != nil {
		t.Fatal(err)
	}
	d.reconciler.requestFullSweep()
	select {
	case <-gate.stored:
	case <-time.After(5 * time.Second):
		t.Fatal("full reconciliation never reached its upload")
	}
	if err := os.Rename(old, filepath.Join(env.localRoot, "new")); err != nil {
		t.Fatal(err)
	}
	done := make(chan syncControlResult, 1)
	go func() { done <- service.save(saveRequestForDaemon(d, 5*time.Second)) }()
	select {
	case <-d.uploader.stopCh:
	case <-time.After(5 * time.Second):
		t.Fatal("save never stopped generation dequeues")
	}
	select {
	case result := <-done:
		t.Fatalf("save completed while full reconciliation was active: %+v", result)
	default:
	}
	release.Do(func() { close(gate.release) })
	var result syncControlResult
	select {
	case result = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("save did not join the full reconciliation")
	}
	var chmodErr error
	select {
	case chmodErr = <-gate.chmodDone:
	default:
		t.Fatal("full reconciliation did not finish its permission update")
	}
	if !result.Success || chmodErr != nil {
		t.Fatalf("in-flight full reconciliation lost its result: save=%+v, Chmod error=%v", result, chmodErr)
	}
	if env.remoteExists(t, "old") {
		t.Fatal("save retained the offline rename's old path")
	}
	if got := env.readRemoteFile(t, "new"); got != "full reconciliation intent" {
		t.Fatalf("saved bytes = %q", got)
	}
	stat, err := env.fsClient.Stat(context.Background(), "/new")
	if err != nil || stat == nil || stat.Mode != 0o751 {
		t.Fatalf("saved permissions = %+v, %v", stat, err)
	}
	if got := env.readLocalFile(t, "new"); got != "full reconciliation intent" {
		t.Fatalf("local intent changed: %q", got)
	}
}
