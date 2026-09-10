package main

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/agent-filesystem/mount/client"
)

type syncSaveDrainClient struct {
	client.Client
	echo func(context.Context, string, []byte) error
}

func (c *syncSaveDrainClient) Echo(ctx context.Context, path string, data []byte) error {
	return c.echo(ctx, path, data)
}

func TestSyncSaveDrainDiscardsQueuedUpload(t *testing.T) {
	env := newSyncTestEnv(t)
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var queuedCalls atomic.Int32
	gate := &syncSaveDrainClient{Client: env.fsClient}
	gate.echo = func(ctx context.Context, path string, data []byte) error {
		if path == "/queued" {
			queuedCalls.Add(1)
		} else if path == "/active" {
			entered <- ctx
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
			}
		}
		return gate.Client.Echo(ctx, path, data)
	}
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
	abs := env.writeLocalFile(t, "active", "saved content")
	d.reconciler.queueUpload(uploadOp{Kind: opUploadFile, Path: "active", AbsPath: abs,
		Content: []byte("saved content"), LocalHash: sha256Hex([]byte("saved content")), Mode: 0o644})
	var operationCtx context.Context
	select {
	case operationCtx = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	// This stale queued write has no local file. Save must discard it.
	d.reconciler.queueUpload(uploadOp{Kind: opUploadFile, Path: "queued", Content: []byte("stale"),
		LocalHash: sha256Hex([]byte("stale")), Mode: 0o644})
	done := make(chan syncControlResult, 1)
	go func() { done <- service.save(saveRequestForDaemon(d, 5*time.Second)) }()
	select {
	case <-d.uploader.stopCh:
	case <-time.After(5 * time.Second):
		t.Fatal("save did not stop dequeueing")
	}
	if err := operationCtx.Err(); err != nil {
		t.Fatalf("save cancelled the active upload before its deadline: %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case result := <-done:
		if !result.Success || result.Save == nil {
			t.Fatalf("save = %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("save did not finish after active upload")
	}
	if queuedCalls.Load() != 0 || env.remoteExists(t, "queued") {
		t.Fatal("old queued upload ran during save")
	}
	if env.readRemoteFile(t, "active") != "saved content" {
		t.Fatal("active upload did not finish")
	}
}

func TestSyncSaveDrainDeadlineStillJoinsUpload(t *testing.T) {
	env := newSyncTestEnv(t)
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	gate := &syncSaveDrainClient{Client: env.fsClient}
	gate.echo = func(ctx context.Context, path string, data []byte) error {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		// Simulate bounded client cleanup after cancellation is observed.
		<-release
		return ctx.Err()
	}
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
	abs := env.writeLocalFile(t, "active", "unsaved content")
	d.reconciler.queueUpload(uploadOp{Kind: opUploadFile, Path: "active", AbsPath: abs,
		Content: []byte("unsaved content"), LocalHash: sha256Hex([]byte("unsaved content")), Mode: 0o644})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	done := make(chan syncControlResult, 1)
	go func() { done <- service.save(saveRequestForDaemon(d, 100*time.Millisecond)) }()
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("save deadline did not cancel the upload")
	}
	select {
	case result := <-done:
		t.Fatalf("save returned before the cancelled upload exited: %+v", result)
	case <-d.done:
		t.Fatal("generation joined before the cancelled upload exited")
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case result := <-done:
		if result.Success || result.Save != nil || !strings.Contains(result.Error, "deadline exceeded") {
			t.Fatalf("expired save = %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("save did not finish joining the cancelled upload")
	}
	select {
	case <-d.done:
	default:
		t.Fatal("save returned before its old generation joined")
	}
	if service.active == nil || service.active == d {
		t.Fatal("save did not resume with a new generation after joining")
	}
	if env.remoteExists(t, "active") {
		t.Fatal("expired save applied a new write")
	}
}
