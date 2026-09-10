package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

func TestWatcherDirectoryWatchFailureRequestsRecovery(t *testing.T) {
	root := t.TempDir()
	w, err := newSyncWatcher(root, nil, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	// Close only the native watcher to make Add fail. The sync watcher is
	// still active and must retain a recovery request for its consumer.
	if err := w.w.Close(); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "new", "nested")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	w.handleEvent(fsnotify.Event{Name: filepath.Join(root, "new"), Op: fsnotify.Create})
	select {
	case <-w.Rescans():
	default:
		t.Fatal("failed directory watch installation did not request recovery")
	}
}

func TestWatcherStaleDirectoryEventRequestsRecovery(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   fsnotify.Op
	}{
		{"remove", fsnotify.Remove},
		{"rename", fsnotify.Rename},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "tree")
			nested := filepath.Join(dir, "nested")
			if err := os.MkdirAll(nested, 0o755); err != nil {
				t.Fatal(err)
			}
			w, err := newSyncWatcher(root, nil, 10*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = w.Close() })
			if err := w.resetRecursive(root); err != nil {
				t.Fatal(err)
			}

			// A notification from the old directory can arrive after recovery
			// has installed fresh watches on the directory now at this path.
			w.handleEvent(fsnotify.Event{Name: dir, Op: tc.op})
			select {
			case <-w.Rescans():
			default:
				t.Fatal("stale directory event removed fresh watches without requesting recovery")
			}
			if err := w.resetRecursive(root); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); w.run(ctx) }()
			t.Cleanup(func() { cancel(); <-done })
			if err := os.WriteFile(filepath.Join(nested, "followup.txt"), []byte("native event after refresh"), 0o644); err != nil {
				t.Fatal(err)
			}
			deadline := time.NewTimer(3 * time.Second)
			defer deadline.Stop()
			for {
				select {
				case event := <-w.Events():
					if event.Path == "tree/nested/followup.txt" {
						return
					}
				case <-deadline.C:
					t.Fatal("refreshed descendant watch missed the next native write")
				}
			}
		})
	}
}
