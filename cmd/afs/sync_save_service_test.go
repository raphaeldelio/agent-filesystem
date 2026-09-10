package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func saveRequestForDaemon(d *syncDaemon, timeout time.Duration) syncControlRequest {
	return syncControlRequest{Version: syncControlVersion, Operation: syncControlOpSave,
		Volume: d.cfg.Workspace, LocalRoot: d.cfg.LocalRoot,
		DeadlineUnixMilli: time.Now().Add(timeout).UnixMilli()}
}

func newDirectSaveService(t *testing.T, env *syncTestEnv) *syncSaveService {
	t.Helper()
	d := env.startDaemon(t)
	// No watcher may upload the test changes before the explicit save.
	if err := d.watcher.Close(); err != nil {
		t.Fatal(err)
	}
	s := &syncSaveService{ctx: context.Background(), active: d}
	t.Cleanup(func() {
		if s.active != nil {
			s.active.Stop()
		}
	})
	return s
}

func TestSyncSaveServicePollsWithoutWatcherAndResumes(t *testing.T) {
	env := newSyncTestEnv(t)
	d := env.startDaemon(t)
	if err := d.watcher.Close(); err != nil {
		t.Fatal(err)
	}
	service := startSyncSaveService(context.Background(), d)
	t.Cleanup(service.Stop)
	env.writeLocalFile(t, ".venv/pkg/data", strings.Repeat("x", (1<<20)+1))
	result, err := runSyncSaveControlRequest(env.localRoot, saveRequestForDaemon(d, 10*time.Second))
	if err != nil || !result.Success || result.Save == nil || result.Save.Files != 1 {
		t.Fatalf("save = %+v, %v", result, err)
	}
	if got := env.readRemoteFile(t, ".venv/pkg/data"); len(got) != (1<<20)+1 {
		t.Fatalf("saved %d bytes", len(got))
	}
	env.writeLocalFile(t, "after", "watcher resumed")
	assertEventually(t, 5*time.Second, "fresh daemon must upload subsequent writes", func() bool {
		return env.remoteExists(t, "after") && env.readRemoteFile(t, "after") == "watcher resumed"
	})
}

func TestSyncSaveServiceWaitsForRemoteWriteAndRestartsAfterTimeout(t *testing.T) {
	env := newSyncTestEnv(t)
	s := newDirectSaveService(t, env)
	env.writeLocalFile(t, "file", "saved bytes")
	entered, release := make(chan struct{}), make(chan struct{})
	fault := &syncSaveFaultClient{Client: env.fsClient, write: func(ctx context.Context, p string, data []byte, mode uint32) error {
		close(entered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
		}
		return env.fsClient.EchoCreate(ctx, p, data, mode)
	}}
	s.active.reconciler.fs = fault
	request := saveRequestForDaemon(s.active, 200*time.Millisecond)
	done := make(chan syncControlResult, 1)
	go func() { done <- s.save(request) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("save never reached write")
	}
	select {
	case result := <-done:
		t.Fatalf("save completed before upload: %+v", result)
	default:
	}
	result := <-done
	if result.Success || !strings.Contains(result.Error, "deadline exceeded") {
		t.Fatalf("save = %+v", result)
	}
	if s.active == nil || env.remoteExists(t, "file") {
		t.Fatal("timeout must restart without claiming a write")
	}
	close(release)
	result = s.save(saveRequestForDaemon(s.active, 5*time.Second))
	if !result.Success || env.readRemoteFile(t, "file") != "saved bytes" {
		t.Fatalf("retry = %+v", result)
	}
}

func TestSyncSaveServiceRejectsInvalidRequestWithoutStopping(t *testing.T) {
	env := newSyncTestEnv(t)
	s := newDirectSaveService(t, env)
	original := s.active
	cases := []func(*syncControlRequest){
		func(r *syncControlRequest) { r.Volume = "wrong" },
		func(r *syncControlRequest) { r.LocalRoot = filepath.Join(env.localRoot, "child") },
		func(r *syncControlRequest) { r.Version++ },
		func(r *syncControlRequest) { r.Path = "subset" },
		func(r *syncControlRequest) { r.DeadlineUnixMilli = 0 },
		func(r *syncControlRequest) { r.DeadlineUnixMilli = time.Now().Add(-time.Second).UnixMilli() },
	}
	for _, change := range cases {
		request := saveRequestForDaemon(original, time.Second)
		change(&request)
		result := s.save(request)
		if result.Success || result.Error == "" || s.active != original {
			t.Fatalf("invalid request changed generation: %+v", result)
		}
		select {
		case <-original.done:
			t.Fatal("invalid request stopped daemon")
		default:
		}
	}
}

func TestSyncSaveServiceCancelsFullQueuesAndDelayedDeletes(t *testing.T) {
	env := newSyncTestEnv(t)
	s := newDirectSaveService(t, env)
	d := s.active
	// A delayed operation from the previous generation must not delete a saved file.
	env.writeLocalFile(t, "keep", "new content")
	d.reconciler.enqueueUploadOpAsync(uploadOp{Kind: opUploadDelete, Path: "keep"}, time.Hour, nil)
	result := s.save(saveRequestForDaemon(d, 5*time.Second))
	if !result.Success {
		t.Fatalf("save = %+v", result)
	}
	if env.readRemoteFile(t, "keep") != "new content" {
		t.Fatal("delayed delete ran")
	}
	// Fill both directions with no consumer. Cancellation must unblock every sender.
	stop := make(chan struct{})
	r := &reconciler{stopCh: stop, uploadCh: make(chan uploadOp, 1), downloadCh: make(chan downloadOp, 1)}
	r.uploadCh <- uploadOp{}
	r.downloadCh <- downloadOp{}
	up := make(chan uploadResult, 1)
	up <- uploadResult{}
	down := make(chan downloadResult, 1)
	down <- downloadResult{}
	u := &uploader{stopCh: stop, results: up}
	downloader := &downloader{stopCh: stop, results: down}
	finished := make(chan struct{}, 4)
	go func() { r.queueUpload(uploadOp{}); finished <- struct{}{} }()
	go func() { r.queueDownload(downloadOp{}); finished <- struct{}{} }()
	go func() { u.send(uploadResult{}); finished <- struct{}{} }()
	go func() { downloader.send(downloadResult{}); finished <- struct{}{} }()
	close(stop)
	for i := 0; i < 4; i++ {
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("cancelled sender blocked")
		}
	}
	if len(u.stoppedResults) != 1 {
		t.Fatal("in-flight upload result was lost")
	}
}

func TestSyncSaveServiceRetainsCompletedUploadOwnership(t *testing.T) {
	env := newSyncTestEnv(t)
	s := newDirectSaveService(t, env)
	d := s.active
	d.Stop()
	env.writeRemoteFile(t, "temporary", "uploaded before stop")
	st, err := env.fsClient.Stat(context.Background(), "/temporary")
	if err != nil {
		t.Fatal(err)
	}
	d.uploader.stoppedResults = []uploadResult{{Op: uploadOp{Kind: opUploadFile, Path: "temporary", LocalHash: sha256Hex([]byte("uploaded before stop")), Mode: 0o644, Content: []byte("uploaded before stop")}, RemoteStat: st}}
	s.applyStoppedUploadResults(d)
	env.writeLocalFile(t, "renamed", "uploaded before stop")
	result := s.save(saveRequestForDaemon(d, 5*time.Second))
	if !result.Success {
		t.Fatalf("save = %+v", result)
	}
	if env.remoteExists(t, "temporary") || env.readRemoteFile(t, "renamed") != "uploaded before stop" {
		t.Fatal("rename did not converge")
	}
	if _, err := os.Stat(filepath.Join(env.localRoot, "renamed")); err != nil {
		t.Fatal(err)
	}
}
