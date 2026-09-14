package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/agent-filesystem/internal/controlplane"
)

func TestSyncSaveFailureResumesUnsavedWork(t *testing.T) {
	env := newSyncTestEnv(t)
	d := env.startDaemon(t, func(cfg *syncDaemonConfig) { cfg.WatcherDebounce = 300 * time.Millisecond })
	s := &syncSaveService{ctx: context.Background(), active: d}
	t.Cleanup(func() {
		if s.active != nil {
			s.active.Stop()
		}
	})
	env.writeLocalFile(t, "pending.txt", "must eventually sync")
	s.active.reconciler.fs = &syncSaveFaultClient{Client: env.fsClient,
		write: func(context.Context, string, []byte, uint32) error { return errors.New("transient write failure") },
	}
	result := s.save(saveRequestForDaemon(s.active, 5*time.Second))
	if result.Success || s.active == nil {
		t.Fatalf("expected failed save and resumed daemon: %+v", result)
	}
	// The pending watcher timer is cancelled by save. The replacement generation
	// uses the original healthy client. No further
	// application write or second explicit save should be needed to resume sync.
	assertEventually(t, 2*time.Second, "pending file after save failure", func() bool {
		got, err := env.fsClient.Cat(context.Background(), "/pending.txt")
		return err == nil && string(got) == "must eventually sync"
	})
}

func TestSyncSaveWritesSessionHistory(t *testing.T) {
	env := newSyncTestEnv(t)
	d := env.startDaemon(t, func(cfg *syncDaemonConfig) {
		cfg.Rdb = env.rdb
		cfg.StorageID = env.mountKey
		cfg.SessionID = "review-save-history"
	})
	if err := d.watcher.Close(); err != nil {
		t.Fatal(err)
	}
	s := &syncSaveService{ctx: context.Background(), active: d}
	t.Cleanup(func() {
		if s.active != nil {
			s.active.Stop()
		}
	})
	env.writeLocalFile(t, "saved.txt", "saved without watcher notification")
	result := s.save(saveRequestForDaemon(d, 5*time.Second))
	if !result.Success {
		t.Fatalf("save failed: %+v", result)
	}
	rows, err := env.rdb.XRange(context.Background(), controlplane.ChangelogStreamKey(env.mountKey), "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Values["path"] == "saved.txt" && row.Values["op"] == "put" {
			return
		}
	}
	t.Fatalf("successful save produced no file history; rows=%+v", rows)
}
