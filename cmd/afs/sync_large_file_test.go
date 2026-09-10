package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSyncNewLargeFileUploadsAfterMount(t *testing.T) {
	env := newSyncTestEnv(t)
	env.startDaemon(t)
	defer env.stopDaemon()

	// Create the file after startup so it uses the background uploader,
	// including a partial final chunk, rather than startup reconciliation.
	content := make([]byte, 2*defaultChunkThreshold+17)
	for i := range content {
		content[i] = byte(i*31 + 7)
	}
	const name = "new-large-file.bin"
	if err := os.WriteFile(filepath.Join(env.localRoot, name), content, 0o644); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	assertEventually(t, 5*time.Second, "complete large file to upload", func() bool {
		got, err := env.fsClient.Cat(ctx, "/"+name)
		return err == nil && bytes.Equal(got, content)
	})
	chunkSize, hashes, err := env.fsClient.ChunkMeta(ctx, "/"+name)
	if err != nil {
		t.Fatal(err)
	}
	if chunkSize != defaultChunkSize {
		t.Fatalf("chunk size = %d, want %d", chunkSize, defaultChunkSize)
	}
	wantChunks := (len(content) + defaultChunkSize - 1) / defaultChunkSize
	if len(hashes) != wantChunks {
		t.Fatalf("chunk count = %d, want %d", len(hashes), wantChunks)
	}
}
