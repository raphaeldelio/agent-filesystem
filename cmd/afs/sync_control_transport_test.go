package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func respondToSyncControlFile(root string, request syncControlRequest, data []byte) <-chan error {
	done := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			paths, err := filepath.Glob(filepath.Join(root, syncControlRequestsDirName, "*.json"))
			if err != nil {
				done <- err
				return
			}
			if len(paths) > 0 {
				body, err := os.ReadFile(paths[0])
				if err != nil {
					done <- err
					return
				}
				var got syncControlRequest
				if err := json.Unmarshal(body, &got); err != nil || got != request {
					done <- fmt.Errorf("request = %+v, want %+v: %v", got, request, err)
					return
				}
				id := strings.TrimSuffix(filepath.Base(paths[0]), ".json")
				done <- writeAtomicFile(syncControlResultPath(root, id), data, 0o600)
				return
			}
			time.Sleep(time.Millisecond)
		}
		done <- errors.New("sync control request was never written")
	}()
	return done
}

func TestSyncControlFileOperationResults(t *testing.T) {
	for _, operation := range []string{syncControlOpCreateExclusive, syncControlOpUndelete} {
		for _, outcome := range []string{"success", "error", "empty error"} {
			t.Run(operation+"/"+outcome, func(t *testing.T) {
				root := t.TempDir()
				request := syncControlRequest{Version: syncControlVersion, Operation: operation,
					Path: "/notes.txt", Content: "notes", VersionID: "version-1", FileID: "file-1", Ordinal: 2}
				reply := syncControlResult{Version: syncControlVersion, Operation: operation,
					Path: request.Path, Bytes: 5, VersionID: "version-2", SourceID: "file-1", Success: outcome == "success"}
				wantErr := ""
				if outcome == "error" {
					reply.Error, wantErr = "file already exists", "file already exists"
				} else if outcome == "empty error" {
					reply.Error, wantErr = "  ", operation+" failed"
				}
				data, err := json.Marshal(reply)
				if err != nil {
					t.Fatal(err)
				}
				done := respondToSyncControlFile(root, request, data)
				result, err := runSyncControlRequest(root, request, time.Second)
				if responseErr := <-done; responseErr != nil {
					t.Fatal(responseErr)
				}
				if wantErr == "" {
					if err != nil || !reflect.DeepEqual(result, reply) {
						t.Fatalf("result = %+v, %v; want %+v", result, err, reply)
					}
				} else if err == nil || err.Error() != wantErr || !reflect.DeepEqual(result, syncControlResult{}) {
					t.Fatalf("result = %+v, %v; want empty result, %q", result, err, wantErr)
				}
				entries, err := os.ReadDir(filepath.Join(root, syncControlResultsDirName))
				if err != nil || len(entries) != 0 {
					t.Fatalf("consumed result left behind: %v", err)
				}
			})
		}
	}
}

func TestSyncControlFileTimeoutRetainsPendingRequest(t *testing.T) {
	root := t.TempDir()
	request := syncControlRequest{Version: syncControlVersion, Operation: syncControlOpCreateExclusive, Path: "/notes.txt"}
	result, err := runSyncControlRequest(root, request, 10*time.Millisecond)
	if err == nil || err.Error() != "timed out waiting for sync control result for /notes.txt" || result.Success {
		t.Fatalf("timeout result = %+v, %v", result, err)
	}
	entries, err := os.ReadDir(filepath.Join(root, syncControlRequestsDirName))
	if err != nil || len(entries) != 1 {
		t.Fatalf("legacy pending request removed: %d entries, %v", len(entries), err)
	}
}

func TestSyncControlRejectsInvalidJSON(t *testing.T) {
	for _, operation := range []string{syncControlOpCreateExclusive, syncControlOpUndelete, syncControlOpSave} {
		t.Run(operation, func(t *testing.T) {
			root := t.TempDir()
			request := syncControlRequest{Version: syncControlVersion, Operation: operation, Path: "/notes.txt"}
			if operation == syncControlOpSave {
				request.Path, request.Volume, request.LocalRoot = "", "notes", root
				request.DeadlineUnixMilli = time.Now().Add(time.Second).UnixMilli()
			}
			done := respondToSyncControlFile(root, request, []byte("{invalid"))
			var result syncControlResult
			var err error
			label := "file operation"
			if operation == syncControlOpSave {
				label = "save"
				result, err = runSyncSaveControlRequest(root, request)
			} else {
				result, err = runSyncControlRequest(root, request, time.Second)
			}
			if responseErr := <-done; responseErr != nil {
				t.Fatal(responseErr)
			}
			if err == nil || !strings.HasPrefix(err.Error(), "parse "+label+" result:") || result.Success {
				t.Fatalf("invalid JSON result = %+v, %v", result, err)
			}
			entries, err := os.ReadDir(filepath.Join(root, syncControlResultsDirName))
			if err != nil || len(entries) != 0 {
				t.Fatalf("invalid result left behind: %v", err)
			}
			if operation == syncControlOpSave {
				entries, err = os.ReadDir(filepath.Join(root, syncControlRequestsDirName))
				if err != nil || len(entries) != 0 {
					t.Fatalf("save request left behind: %v", err)
				}
			}
		})
	}
}
