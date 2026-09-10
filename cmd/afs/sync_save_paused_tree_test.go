package main

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSyncSaveRejectsLocalChangesDuringDrain(t *testing.T) {
	env := newSyncTestEnv(t)
	gate := &syncSaveInflightClient{Client: env.fsClient, stored: make(chan struct{}), release: make(chan struct{}), postStat: make(chan error, 1)}
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
	abs := env.writeLocalFile(t, "old", "before pause")
	d.reconciler.queueUpload(uploadOp{Kind: opUploadFile, Path: "old", AbsPath: abs, Content: []byte("before pause"), LocalHash: sha256Hex([]byte("before pause")), Mode: 0o644})
	select {
	case <-gate.stored:
	case <-time.After(5 * time.Second):
		t.Fatal("upload never started")
	}
	done := make(chan syncControlResult, 1)
	go func() { done <- service.save(saveRequestForDaemon(d, 5*time.Second)) }()
	select {
	case <-d.uploader.stopCh:
	case <-time.After(5 * time.Second):
		t.Fatal("save did not pause sync")
	}
	env.writeLocalFile(t, "old", "changed during pause")
	release.Do(func() { close(gate.release) })
	select {
	case result := <-done:
		if result.Success || result.Save != nil || !strings.Contains(result.Error, "local tree changed while pausing") {
			t.Fatalf("changed tree acknowledged: %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("save did not finish")
	}
	if env.readLocalFile(t, "old") != "changed during pause" {
		t.Fatal("local change was overwritten")
	}
	if env.readRemoteFile(t, "old") != "before pause" {
		t.Fatal("failed save published a different local tree")
	}
}
