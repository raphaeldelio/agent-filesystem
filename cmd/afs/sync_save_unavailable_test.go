package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSyncSaveServiceRepliesWhenGenerationUnavailable(t *testing.T) {
	for _, captureFromGeneration := range []bool{false, true} {
		name := "retained_identity"
		if captureFromGeneration {
			name = "capture_before_generation_stops"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			s := &syncSaveService{ctx: context.Background(), workspace: "notes", localRoot: root}
			if captureFromGeneration {
				s.workspace, s.localRoot = "", ""
				s.active = &syncDaemon{cfg: syncDaemonConfig{Workspace: "notes", LocalRoot: root}}
				s.poll()
				s.active = nil
			}
			request := syncControlRequest{Version: syncControlVersion, Operation: syncControlOpSave,
				Volume: "notes", LocalRoot: root, DeadlineUnixMilli: time.Now().Add(time.Minute).UnixMilli()}
			requestPath := syncControlRequestPath(root, "unavailable")
			if err := writeSyncControlJSON(requestPath, request, 0o600); err != nil {
				t.Fatal(err)
			}
			s.poll()
			raw, err := os.ReadFile(syncControlResultPath(root, "unavailable"))
			if err != nil {
				t.Fatalf("poll did not reply while unavailable: %v", err)
			}
			var result syncControlResult
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatal(err)
			}
			if result.Success || result.Save != nil || result.Error != "sync daemon is unavailable" ||
				result.Volume != request.Volume || result.LocalRoot != root ||
				result.Version != syncControlVersion || result.Operation != syncControlOpSave {
				t.Fatalf("unexpected unavailable response: %+v", result)
			}
			if _, err := os.Stat(requestPath); !os.IsNotExist(err) {
				t.Fatalf("handled request remains: %v", err)
			}
			if s.active != nil || s.workspace != "notes" || s.localRoot != root {
				t.Fatal("reply changed the unavailable mount identity")
			}
		})
	}
}

func TestSyncSaveServiceUnavailableRetainsIdentityValidation(t *testing.T) {
	root := t.TempDir()
	s := &syncSaveService{ctx: context.Background(), workspace: "notes", localRoot: root}
	for _, request := range []syncControlRequest{
		{Version: syncControlVersion, Volume: "another", LocalRoot: root},
		{Version: syncControlVersion, Volume: "notes", LocalRoot: filepath.Join(root, "child")},
	} {
		result := s.save(request)
		if result.Success || !strings.Contains(result.Error, "does not match") {
			t.Fatalf("unavailable mount accepted another identity: %+v", result)
		}
	}
}
