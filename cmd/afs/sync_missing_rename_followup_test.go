package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/redis/agent-filesystem/mount/client"
)

func TestSyncRecoveryKeepsDestinationAfterMissingRenameFollowup(t *testing.T) {
	for _, tc := range []struct {
		cause   string
		chunked bool
	}{
		{cause: "transient upload failure"},
		{cause: "newer local edit"},
		{cause: "chunked newer local edit", chunked: true},
	} {
		t.Run(tc.cause, func(t *testing.T) {
			env, d := recoveryBaselineDiagnostic(t)
			if tc.chunked {
				d.reconciler.chunkThreshold = 8
				d.reconciler.chunkSize = 8
			}
			ctx := context.Background()
			oldPath := env.writeLocalFile(t, "old.txt", "original content")
			if err := d.full.warmStart(ctx, nil); err != nil {
				t.Fatal(err)
			}
			newPath := filepath.Join(env.localRoot, "new.txt")
			if err := os.Rename(oldPath, newPath); err != nil {
				t.Fatal(err)
			}
			want := "edited content queued after rename"
			env.writeLocalFile(t, "new.txt", want)
			// The rename event discovers new.txt, installs the destination
			// baseline, and queues both rename and its content update.
			d.reconciler.handleLocalEvent(ctx, LocalEvent{Path: "old.txt", KindHint: "rename"})
			renameOp := nextDiagnosticUpload(t, d)
			fileOp := nextDiagnosticUpload(t, d)
			if renameOp.Kind != opUploadRename || fileOp.Kind != opUploadFile || !renameOp.Tracked || !fileOp.Tracked {
				t.Fatalf("expected tracked rename then upload; got kinds %v, %v", renameOp.Kind, fileOp.Kind)
			}
			if fileOp.Chunked != tc.chunked {
				t.Fatalf("followup chunked=%v, want %v", fileOp.Chunked, tc.chunked)
			}
			t.Logf("rename version=%d, staged destination version=%d", renameOp.RenameVersion, d.Snapshot().Entries["new.txt"].Version)
			if err := env.fsClient.Rm(ctx, "/old.txt"); err != nil {
				t.Fatal(err)
			}
			d.uploader.process(ctx, renameOp)
			renameResult := <-d.reconciler.uploadResCh
			if !renameResult.Skipped || renameResult.Err != nil {
				t.Fatalf("missing source rename result: skipped=%v, error=%v", renameResult.Skipped, renameResult.Err)
			}
			d.reconciler.handleUploadResult(ctx, renameResult)

			writeFailure := errors.New("injected transient upload failure")
			if tc.cause == "transient upload failure" {
				d.uploader.fs = &missingRenameFollowupClient{Client: d.uploader.fs, writeErr: writeFailure}
			} else {
				want = "newest content after the upload was queued"
				env.writeLocalFile(t, "new.txt", want)
			}
			d.uploader.process(ctx, fileOp)
			fileResult := <-d.reconciler.uploadResCh
			if tc.cause == "transient upload failure" && !errors.Is(fileResult.Err, writeFailure) {
				t.Fatalf("followup upload error=%v, want injected failure", fileResult.Err)
			}
			if tc.cause != "transient upload failure" && (!fileResult.Skipped || fileResult.Err != nil || fileResult.Conflict) {
				t.Fatalf("obsolete followup: skipped=%v, conflict=%v, error=%v", fileResult.Skipped, fileResult.Conflict, fileResult.Err)
			}
			d.reconciler.handleUploadResult(ctx, fileResult)
			if len(d.reconciler.pendingUploads) != 0 {
				t.Fatal("finished rename/upload retained pending paths")
			}
			// Recovery now uses the healthy filesystem client and must upload
			// the active local destination, never delete it as remotely absent.
			if err := d.full.warmStart(ctx, nil); err != nil {
				t.Fatal(err)
			}
			local, localErr := os.ReadFile(newPath)
			remote, remoteErr := env.fsClient.Cat(ctx, "/new.txt")
			if localErr != nil || string(local) != want {
				t.Errorf("active local destination=%q, error=%v, want %q", local, localErr, want)
			}
			if remoteErr != nil || string(remote) != want {
				t.Errorf("remote destination=%q, error=%v, want %q", remote, remoteErr, want)
			}
		})
	}
}

type missingRenameFollowupClient struct {
	client.Client
	writeErr error
}

func (c *missingRenameFollowupClient) Echo(context.Context, string, []byte) error {
	return c.writeErr
}
