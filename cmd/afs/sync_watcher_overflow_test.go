package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

func TestWatcherOverflowQueuesRecoveryOutsideEventQueue(t *testing.T) {
	w := &syncWatcher{
		out: make(chan LocalEvent, 1), rescan: make(chan struct{}, 1),
		pending: make(map[string]*pendingEvent),
	}
	w.out <- LocalEvent{Path: "already-queued"}
	for _, name := range []string{"lost-first", "lost-second"} {
		w.pending[name] = &pendingEvent{kindHint: "write"}
		w.flush(name)
	}
	select {
	case <-w.Rescans():
	default:
		t.Fatal("full event queue lost the recovery request")
	}
	if len(w.Rescans()) != 0 {
		t.Fatal("overflows before recovery should coalesce")
	}
	// The consumer has started recovery, but the event queue is still full.
	// A further loss must remain pending for a second scan.
	w.pending["lost-during-scan"] = &pendingEvent{kindHint: "write"}
	w.flush("lost-during-scan")
	select {
	case <-w.Rescans():
	default:
		t.Fatal("overflow during recovery did not queue another scan")
	}
	if got := <-w.Events(); got.Path != "already-queued" {
		t.Fatalf("existing queued event changed: %#v", got)
	}
}

func TestWatcherNormalEventDoesNotRequestRescan(t *testing.T) {
	w := &syncWatcher{
		out: make(chan LocalEvent, 1), rescan: make(chan struct{}, 1),
		pending: map[string]*pendingEvent{"file": {kindHint: "write"}},
	}
	w.flush("file")
	if len(w.Events()) != 1 || len(w.Rescans()) != 0 {
		t.Fatal("ordinary event should use only the event queue")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w.requestRescan()
	if len(w.Rescans()) != 0 {
		t.Fatal("closed watcher requested recovery")
	}
}

func TestWatcherKernelOverflowRequestsRescan(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"overflow", fsnotify.ErrEventOverflow, true},
		{"wrapped-overflow", fmt.Errorf("watch failed: %w", fsnotify.ErrEventOverflow), true},
		{"other-error", errors.New("ordinary watcher error"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &syncWatcher{
				w:       &fsnotify.Watcher{Events: make(chan fsnotify.Event), Errors: make(chan error)},
				pending: make(map[string]*pendingEvent), rescan: make(chan struct{}, 1),
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); w.run(ctx) }()
			defer func() { cancel(); <-done }()
			select {
			case w.w.Errors <- tc.err:
			case <-time.After(time.Second):
				t.Fatal("watcher did not receive error")
			}
			// A second receive ensures the first error has been processed.
			select {
			case w.w.Errors <- nil:
			case <-time.After(time.Second):
				t.Fatal("watcher did not finish processing error")
			}
			if got := len(w.Rescans()) != 0; got != tc.want {
				t.Fatalf("rescan requested = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWatcherConcurrentOverflowRequestsCoalesce(t *testing.T) {
	w := &syncWatcher{
		out: make(chan LocalEvent, 1), rescan: make(chan struct{}, 1),
		pending: make(map[string]*pendingEvent),
	}
	w.out <- LocalEvent{Path: "queued"}
	for i := 0; i < 32; i++ {
		w.pending[fmt.Sprint(i)] = &pendingEvent{kindHint: "write"}
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(rel string) { defer wg.Done(); w.flush(rel) }(fmt.Sprint(i))
	}
	wg.Wait()
	if len(w.Rescans()) != 1 {
		t.Fatal("concurrent drops should leave one pending recovery")
	}
	<-w.Rescans()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w.requestRescan()
	if len(w.Rescans()) != 0 {
		t.Fatal("shutdown should stop new recovery requests")
	}
}
