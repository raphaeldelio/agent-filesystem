package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestUploaderChunkedFileRecreatesMissingRemoteContent(t *testing.T) {
	env := newSyncTestEnv(t)
	ctx := context.Background()
	const rel = "large.bin"
	chunkSize := defaultChunkSize
	baseline := make([]byte, 5*chunkSize+17)
	for i := range baseline {
		baseline[i] = byte(i/chunkSize + 1)
	}
	abs := filepath.Join(env.localRoot, rel)
	if err := os.WriteFile(abs, baseline, 0o644); err != nil {
		t.Fatal(err)
	}
	oldHashes, _, err := streamChunkHashes(abs, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	env.writeRemoteFile(t, rel, string(baseline))
	if err := env.fsClient.Rm(ctx, "/"+rel); err != nil {
		t.Fatalf("remove remote baseline: %v", err)
	}
	if _, _, err := env.fsClient.ChunkMeta(ctx, "/"+rel); !errors.Is(err, redis.Nil) {
		t.Fatalf("missing remote ChunkMeta error = %v, want redis.Nil", err)
	}

	updated := bytes.Clone(baseline)
	updated[chunkSize+7] ^= 0xff
	if err := os.WriteFile(abs, updated, 0o644); err != nil {
		t.Fatal(err)
	}
	newHashes, size, err := streamChunkHashes(abs, chunkSize)
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan uploadResult, 1)
	u := newUploader(env.fsClient, results, 16*1024*1024, false, nil)
	u.processChunkedFile(ctx, uploadOp{
		Kind:        opUploadFile,
		Path:        rel,
		AbsPath:     abs,
		Mode:        0o644,
		LocalHash:   compositeHash(newHashes),
		HasStored:   true,
		Chunked:     true,
		FileSize:    size,
		ChunkSize:   chunkSize,
		ChunkHashes: newHashes,
		DirtyChunks: []int{1},
		StoredEntry: SyncEntry{
			Type:        "file",
			Size:        int64(len(baseline)),
			LocalHash:   compositeHash(oldHashes),
			RemoteHash:  compositeHash(oldHashes),
			ChunkSize:   chunkSize,
			ChunkHashes: oldHashes,
		},
	})
	select {
	case result := <-results:
		if result.Err != nil || result.Conflict {
			t.Fatalf("recreate upload failed: err=%v conflict=%v", result.Err, result.Conflict)
		}
	default:
		t.Fatal("uploader returned without a result")
	}
	actual, err := env.fsClient.Cat(ctx, "/"+rel)
	if err != nil {
		t.Fatalf("read recreated file: %v", err)
	}
	if !bytes.Equal(actual, updated) {
		t.Fatalf("recreated file differs: got %d bytes, want all %d bytes including unchanged chunks", len(actual), len(updated))
	}
}
