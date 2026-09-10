package main

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestSyncWatcherOverflowPreservesEditAfterInitialUpload(t *testing.T) {
	env := newSyncTestEnv(t)
	const rel = "edited.txt"
	abs := env.writeLocalFile(t, rel, "before")
	oldTime := time.Unix(10, 0)
	if err := os.Chtimes(abs, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	d := env.startDaemon(t, func(cfg *syncDaemonConfig) {
		cfg.WatcherDebounce = time.Hour
	})
	if err := d.watcher.w.Remove(env.localRoot); err != nil {
		t.Fatal(err)
	}
	remoteStat, err := env.fsClient.Stat(context.Background(), "/"+rel)
	if err != nil || remoteStat == nil {
		t.Fatalf("initial upload stat = %+v, err = %v", remoteStat, err)
	}
	t.Logf("initial remote mtime = %d, stored remote mtime = %d", remoteStat.Mtime, d.Snapshot().Entries[rel].RemoteMtimeMs)
	t.Cleanup(func() {
		if t.Failed() {
			remote, remoteErr := env.fsClient.Cat(context.Background(), "/"+rel)
			local, localErr := os.ReadFile(abs)
			t.Logf("after recovery: remote = %q (err %v), local = %q (err %v)", remote, remoteErr, local, localErr)
		}
	})

	const updated = "after the missed local edit"
	env.writeLocalFile(t, rel, updated)
	triggerSyncWatcherOverflow(d.watcher, rel)
	assertEventually(t, 5*time.Second, "missed edit after initial upload to reach remote", func() bool {
		got, err := env.fsClient.Cat(context.Background(), "/"+rel)
		return err == nil && string(got) == updated
	})
	if got := env.readLocalFile(t, rel); got != updated {
		t.Fatalf("active local file = %q, want the local edit %q", got, updated)
	}
}
