package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/agent-filesystem/mount/client"
)

func TestSyncWatcherOverflowRecoversMissedLocalChanges(t *testing.T) {
	env := newSyncTestEnv(t)
	// Hydration records the actual remote mtimes for an accurate baseline.
	env.writeRemoteFile(t, "edited.txt", "before")
	env.writeRemoteFile(t, "deleted.txt", "remove me")
	d := env.startDaemon(t, func(cfg *syncDaemonConfig) {
		cfg.WatcherDebounce = time.Hour
	})

	// Losing a directory watch models missed kernel events and leaves the
	// watcher's directory bookkeeping stale until recovery repairs it.
	if err := d.watcher.w.Remove(env.localRoot); err != nil {
		t.Fatal(err)
	}
	env.writeLocalFile(t, "edited.txt", "after the missed write")
	if err := os.Remove(filepath.Join(env.localRoot, "deleted.txt")); err != nil {
		t.Fatal(err)
	}
	env.writeLocalFile(t, "new/nested/created.txt", "missed creation")
	triggerSyncWatcherOverflow(d.watcher, "new/nested/created.txt")

	assertEventually(t, 5*time.Second, "missed local changes to recover", func() bool {
		for rel, want := range map[string]string{
			"edited.txt":             "after the missed write",
			"new/nested/created.txt": "missed creation",
		} {
			got, err := env.fsClient.Cat(context.Background(), "/"+rel)
			if err != nil || string(got) != want {
				return false
			}
		}
		stat, err := env.fsClient.Stat(context.Background(), "/deleted.txt")
		return err == nil && stat == nil
	})
	if env.localExists("deleted.txt") {
		t.Fatal("recovery recreated the locally deleted file")
	}

	// A subsequent native event in the previously unwatched directory must
	// propagate without another recovery request.
	d.watcher.mu.Lock()
	d.watcher.debounce = 20 * time.Millisecond
	d.watcher.mu.Unlock()
	env.writeLocalFile(t, "new/nested/followup.txt", "native event after recovery")
	assertEventually(t, 5*time.Second, "new directory watch to observe later writes", func() bool {
		got, err := env.fsClient.Cat(context.Background(), "/new/nested/followup.txt")
		return err == nil && string(got) == "native event after recovery"
	})
}

func TestSyncWatcherOverflowPreservesHiddenLocalTree(t *testing.T) {
	env := newSyncTestEnv(t)
	d := env.startDaemon(t, func(cfg *syncDaemonConfig) {
		cfg.WatcherDebounce = time.Hour
	})
	if err := d.watcher.w.Remove(env.localRoot); err != nil {
		t.Fatal(err)
	}
	const rel = ".venv/package.py"
	const content = "value = 'unsynced local package'\n"
	env.writeLocalFile(t, rel, content)
	if len(d.Snapshot().Entries) != 0 {
		t.Fatal("expected no synced entries before recovering the hidden tree")
	}
	triggerSyncWatcherOverflow(d.watcher, rel)

	assertEventually(t, 5*time.Second, "hidden local tree to upload during recovery", func() bool {
		got, err := env.fsClient.Cat(context.Background(), "/"+rel)
		return err == nil && string(got) == content
	})
	if got := env.readLocalFile(t, rel); got != content {
		t.Fatalf("local hidden file = %q, want %q", got, content)
	}
}

func TestSyncWatcherOverflowDuringRecoveryRunsAgain(t *testing.T) {
	env := newSyncTestEnv(t)
	env.writeLocalFile(t, "anchor.txt", "already synced")
	gate := &overflowScanGate{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(gate.release) }) }
	t.Cleanup(unblock)
	d := env.startDaemon(t, func(cfg *syncDaemonConfig) {
		cfg.WatcherDebounce = time.Hour
		gate.Client = cfg.FS
		cfg.FS = gate
	})
	if err := d.watcher.w.Remove(env.localRoot); err != nil {
		t.Fatal(err)
	}
	gate.armed.Store(true)
	triggerSyncWatcherOverflow(d.watcher, "first.txt")
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not reach the remote scan")
	}

	// The first pass already scanned local metadata. This file can only be
	// discovered by the recovery request made while that pass is active.
	env.writeLocalFile(t, "created-during-recovery.txt", "requires another pass")
	triggerSyncWatcherOverflow(d.watcher, "created-during-recovery.txt")
	unblock()
	assertEventually(t, 5*time.Second, "overflow during recovery to schedule another pass", func() bool {
		got, err := env.fsClient.Cat(context.Background(), "/created-during-recovery.txt")
		return err == nil && string(got) == "requires another pass"
	})
}

func TestSyncWatcherOverflowRetriesFailedRecovery(t *testing.T) {
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
	env.writeLocalFile(t, "missed.txt", "survives a failed scan")
	failed.armed.Store(true)
	triggerSyncWatcherOverflow(d.watcher, "missed.txt")
	select {
	case <-failed.failed:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not encounter the injected scan failure")
	}
	assertEventually(t, 5*time.Second, "failed recovery to retry without another event", func() bool {
		got, err := env.fsClient.Cat(context.Background(), "/missed.txt")
		return err == nil && string(got) == "survives a failed scan"
	})
}

func TestSyncWatcherOverflowShutdownCancelsRetry(t *testing.T) {
	env := newSyncTestEnv(t)
	failed := &overflowScanFailure{failed: make(chan struct{})}
	d := env.startDaemon(t, func(cfg *syncDaemonConfig) {
		cfg.WatcherDebounce = time.Hour
		failed.Client = cfg.FS
		cfg.FS = failed
	})
	failed.armed.Store(true)
	triggerSyncWatcherOverflow(d.watcher, "missed.txt")
	select {
	case <-failed.failed:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not encounter the injected scan failure")
	}
	done := make(chan struct{})
	go func() {
		d.Stop()
		close(done)
	}()
	select {
	case <-done:
		env.daemon = nil
	case <-time.After(500 * time.Millisecond):
		t.Fatal("shutdown waited for the recovery retry delay")
	}
}

// Use the real flush path with a detached, saturated event queue so a
// concurrent reconciler cannot drain the queue before the simulated drop.
// The recovery channel is the one consumed by the running daemon.
func triggerSyncWatcherOverflow(w *syncWatcher, rel string) {
	saturated := &syncWatcher{
		pending: map[string]*pendingEvent{rel: {kindHint: "write"}},
		out:     make(chan LocalEvent, 1),
		rescan:  w.rescan,
	}
	saturated.out <- LocalEvent{Path: "queued.txt", KindHint: "write"}
	saturated.flush(rel)
}

type overflowScanGate struct {
	client.Client
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (g *overflowScanGate) LsLong(ctx context.Context, path string) ([]client.LsEntry, error) {
	if path == "/" && g.armed.CompareAndSwap(true, false) {
		close(g.entered)
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.Client.LsLong(ctx, path)
}

type overflowScanFailure struct {
	client.Client
	armed  atomic.Bool
	failed chan struct{}
}

func (f *overflowScanFailure) LsLong(ctx context.Context, path string) ([]client.LsEntry, error) {
	if path == "/" && f.armed.CompareAndSwap(true, false) {
		close(f.failed)
		return nil, errors.New("injected temporary scan failure")
	}
	return f.Client.LsLong(ctx, path)
}
