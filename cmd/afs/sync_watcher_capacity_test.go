package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestWatcherConfiguredCapacityAndRecovery(t *testing.T) {
	for _, capacity := range []int{0, 1, 7, 2048} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			w, err := newSyncWatcher(t.TempDir(), nil, time.Hour, capacity)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = w.Close() })
			want := capacity
			if want == 0 {
				want = defaultSyncWatcherQueueCapacity
			}
			if cap(w.Events()) != want || cap(w.Rescans()) != 1 {
				t.Fatalf("event/recovery capacities = %d/%d, want %d/1", cap(w.Events()), cap(w.Rescans()), want)
			}
			// No consumer is running, so saturation is deterministic.
			for i := 0; i < want; i++ {
				w.out <- LocalEvent{Path: fmt.Sprint(i)}
			}
			w.pending["lost"] = &pendingEvent{kindHint: "write"}
			w.flush("lost")
			if len(w.Events()) != want || len(w.Rescans()) != 1 {
				t.Fatal("configured event queue saturation did not request recovery")
			}
		})
	}
}

func TestWatcherInvalidCapacityRejectedBeforeSetup(t *testing.T) {
	for _, capacity := range []int{-1, maxSyncWatcherQueueCapacity + 1} {
		_, err := newSyncWatcher("", nil, 0, capacity)
		if err == nil || !strings.Contains(err.Error(), "sync.watcherQueueCapacity") {
			t.Fatalf("capacity %d: expected configuration error, got %v", capacity, err)
		}
		_, err = newSyncDaemon(syncDaemonConfig{WatcherQueueCapacity: capacity})
		if err == nil || !strings.Contains(err.Error(), "sync.watcherQueueCapacity") {
			t.Fatalf("daemon capacity %d: expected configuration error, got %v", capacity, err)
		}
	}
}

func TestSyncDaemonConfiguredWatcherCapacity(t *testing.T) {
	env := newSyncTestEnv(t)
	d := env.startDaemon(t, func(cfg *syncDaemonConfig) {
		cfg.WatcherQueueCapacity = 7
	})
	if cap(d.watcher.Events()) != 7 {
		t.Fatalf("daemon watcher capacity = %d, want 7", cap(d.watcher.Events()))
	}
}
