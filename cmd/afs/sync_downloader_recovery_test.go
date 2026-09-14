package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/agent-filesystem/mount/client"
)

func TestSyncRecoveryDownloaderIdenticalFileDoesNotRewrite(t *testing.T) {
	env, d, results := newRecoveryDownloader(t)
	abs := env.writeLocalFile(t, "same.txt", "identical bytes")
	env.writeRemoteFile(t, "same.txt", "identical bytes")
	if err := os.Chmod(abs, 0644); err != nil {
		t.Fatal(err)
	}
	old := time.Unix(10, 0)
	if err := os.Chtimes(abs, old, old); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Lstat(abs)
	d.processFile(context.Background(), downloadOp{Kind: opDownloadFile, Path: "same.txt", AbsPath: abs, Conflict: true})
	res := <-results
	if res.Err != nil || res.Skipped || res.ConflictPath != "" {
		t.Fatalf("identical download result: %+v", res)
	}
	after, _ := os.Lstat(abs)
	if !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("identical file was replaced or its mtime changed")
	}
	if res.LocalMtimeMs != old.UnixMilli() || res.RemoteHash != sha256Hex([]byte("identical bytes")) {
		t.Fatal("result did not retain actual local mtime and content hash")
	}
	assertNoRecoveryConflictCopies(t, abs)
}

func TestSyncRecoveryDownloaderPreservesRealConflicts(t *testing.T) {
	for _, kind := range []string{"content", "mode", "type"} {
		t.Run(kind, func(t *testing.T) {
			env, d, results := newRecoveryDownloader(t)
			const remote = "remote data"
			local := "local edits"
			if kind == "mode" {
				local = remote
			}
			abs := env.writeLocalFile(t, "conflict.txt", local)
			if kind == "mode" {
				if err := os.Chmod(abs, 0755); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "type" {
				if err := os.Remove(abs); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("original-target", abs); err != nil {
					t.Fatal(err)
				}
			}
			env.writeRemoteFile(t, "conflict.txt", remote)
			d.processFile(context.Background(), downloadOp{Kind: opDownloadFile, Path: "conflict.txt", AbsPath: abs, Conflict: true})
			res := <-results
			if res.Err != nil || res.Skipped || res.ConflictPath == "" {
				t.Fatalf("real conflict result: %+v", res)
			}
			if got := env.readLocalFile(t, "conflict.txt"); got != remote {
				t.Fatalf("active file = %q", got)
			}
			if kind == "type" {
				if target, err := os.Readlink(res.ConflictPath); err != nil || target != "original-target" {
					t.Fatalf("conflicting symlink not preserved: %q, %v", target, err)
				}
			} else {
				if copyBytes, err := os.ReadFile(res.ConflictPath); err != nil || string(copyBytes) != local {
					t.Fatal("conflicting local bytes not preserved")
				}
				if kind == "mode" {
					copyInfo, _ := os.Stat(res.ConflictPath)
					activeInfo, _ := os.Stat(abs)
					if copyInfo.Mode().Perm() != 0755 || activeInfo.Mode().Perm() != 0644 {
						t.Fatal("mode difference was dismissed as an equal file")
					}
				}
			}
		})
	}
}

func TestSyncRecoveryDownloaderChunkedConflictRetainsAllChunks(t *testing.T) {
	env, d, results := newRecoveryDownloader(t)
	abs := env.writeLocalFile(t, "chunked.bin", "AAAAXXXXCCCC")
	env.writeRemoteFile(t, "chunked.bin", "AAAABBBBCCCC")
	hashes := []string{sha256Hex([]byte("AAAA")), sha256Hex([]byte("BBBB")), sha256Hex([]byte("CCCC"))}
	d.processChunkedFile(context.Background(), downloadOp{
		Kind: opDownloadFile, Path: "chunked.bin", AbsPath: abs, Conflict: true,
		Chunked: true, ChunkSize: 4, FileSize: 12, ChunkHashes: hashes, DirtyChunks: []int{1},
	})
	res := <-results
	if res.Err != nil || res.Skipped || res.ConflictPath == "" {
		t.Fatalf("chunked conflict result: %+v", res)
	}
	if got := env.readLocalFile(t, "chunked.bin"); got != "AAAABBBBCCCC" {
		t.Fatalf("unchanged remote chunks lost: %q", got)
	}
	if copyBytes, err := os.ReadFile(res.ConflictPath); err != nil || string(copyBytes) != "AAAAXXXXCCCC" {
		t.Fatal("chunked local conflict not preserved")
	}
	if res.Op.Chunked || res.Op.ChunkSize != 0 || len(res.Op.ChunkHashes) != 0 {
		t.Fatal("full download retained stale delta metadata")
	}
}

func TestSyncRecoveryDownloaderAppliesCurrentChunkDelta(t *testing.T) {
	env, d, results := newRecoveryDownloader(t)
	abs := env.writeLocalFile(t, "delta.bin", "AAAAXXXXCCCC")
	if err := os.Chmod(abs, 0644); err != nil {
		t.Fatal(err)
	}
	env.writeRemoteFile(t, "delta.bin", "AAAABBBBCCCC")
	before := []string{sha256Hex([]byte("AAAA")), sha256Hex([]byte("XXXX")), sha256Hex([]byte("CCCC"))}
	after := []string{sha256Hex([]byte("AAAA")), sha256Hex([]byte("BBBB")), sha256Hex([]byte("CCCC"))}
	d.processFile(context.Background(), downloadOp{
		Kind: opDownloadFile, Path: "delta.bin", AbsPath: abs, HasStored: true,
		StoredEntry: SyncEntry{Type: "file", Mode: 0644, Size: 12, LocalHash: compositeHash(before), ChunkSize: 4, ChunkHashes: before},
		Chunked:     true, ChunkSize: 4, FileSize: 12, ChunkHashes: after, DirtyChunks: []int{1},
	})
	res := <-results
	if res.Err != nil || res.Skipped || res.ConflictPath != "" || !res.Op.Chunked {
		t.Fatalf("current delta result: %+v", res)
	}
	if got := env.readLocalFile(t, "delta.bin"); got != "AAAABBBBCCCC" {
		t.Fatalf("delta contents: %q", got)
	}
	info, _ := os.Stat(abs)
	if res.RemoteHash != compositeHash(after) || res.LocalMtimeMs != info.ModTime().UnixMilli() {
		t.Fatal("delta result has stale hash or local mtime")
	}
}

func TestSyncRecoveryDownloaderSkipsNewerLocalEdits(t *testing.T) {
	for _, duringRead := range []bool{false, true} {
		t.Run(map[bool]string{false: "queued", true: "during_remote_read"}[duringRead], func(t *testing.T) {
			env, d, results := newRecoveryDownloader(t)
			abs := env.writeLocalFile(t, "edited.txt", "before")
			if err := os.Chmod(abs, 0644); err != nil {
				t.Fatal(err)
			}
			env.writeRemoteFile(t, "edited.txt", "remote update")
			op := downloadOp{
				Kind: opDownloadFile, Path: "edited.txt", AbsPath: abs, HasStored: true,
				StoredEntry: SyncEntry{Type: "file", Mode: 0644, Size: 6, LocalHash: sha256Hex([]byte("before"))},
			}
			edit := func() { env.writeLocalFile(t, "edited.txt", "latest local edit") }
			if duringRead {
				d.fs = &recoveryDownloadReadClient{Client: d.fs, afterCat: edit}
			} else {
				edit()
			}
			d.processFile(context.Background(), op)
			res := <-results
			if res.Err != nil || !res.Skipped || res.ConflictPath != "" {
				t.Fatalf("stale download was not skipped: %+v", res)
			}
			if got := env.readLocalFile(t, "edited.txt"); got != "latest local edit" {
				t.Fatalf("newer local edit overwritten: %q", got)
			}
			assertNoRecoveryConflictCopies(t, abs)
		})
	}
}

type recoveryDownloadReadClient struct {
	client.Client
	afterCat func()
}

func (c *recoveryDownloadReadClient) Cat(ctx context.Context, path string) ([]byte, error) {
	data, err := c.Client.Cat(ctx, path)
	if c.afterCat != nil {
		c.afterCat()
	}
	return data, err
}

func newRecoveryDownloader(t *testing.T) (*syncTestEnv, *downloader, chan downloadResult) {
	t.Helper()
	env := newSyncTestEnv(t)
	results := make(chan downloadResult, 1)
	d := newDownloader(env.fsClient, results, env.localRoot, newConflictNamer(), newEchoSuppressor(), false, nil)
	return env, d, results
}

func assertNoRecoveryConflictCopies(t *testing.T, abs string) {
	t.Helper()
	copies, err := filepath.Glob(abs + ".conflict-*")
	if err != nil || len(copies) != 0 {
		t.Fatalf("unexpected conflict copies: %v, %v", copies, err)
	}
}
