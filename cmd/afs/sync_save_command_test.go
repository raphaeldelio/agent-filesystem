package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSyncSaveOptions(t *testing.T) {
	for _, args := range [][]string{{"notes"}, {"--timeout", "3s", "--json", "notes"}, {"notes", "--json", "--timeout=3s"}} {
		opts, err := parseSyncSaveOptions(args)
		if err != nil || opts.target != "notes" {
			t.Fatalf("parse %q: %+v, %v", args, opts, err)
		}
		if len(args) > 1 && (!opts.json || opts.timeout != 3*time.Second) {
			t.Fatalf("options = %+v", opts)
		}
		if len(args) == 1 && opts.timeout != 2*time.Minute {
			t.Fatalf("default timeout = %s", opts.timeout)
		}
	}
	for _, args := range [][]string{nil, {"one", "two"}, {"notes", "--timeout", "0"}, {"--timeout=-1s", "notes"}, {"notes", "--timeout", "later"}, {"notes", "--timeout"}, {"notes", "--path", "file.txt"}} {
		if _, err := parseSyncSaveOptions(args); err == nil {
			t.Errorf("accepted invalid options %q", args)
		}
	}
}

func TestSyncSaveDispatchAndHelp(t *testing.T) {
	out, err := captureStdout(t, func() error { return cmdVolume([]string{"vol", "save", "--help"}) })
	if err != nil || !strings.Contains(out, "vol save") || !strings.Contains(out, "Redis disk") {
		t.Fatalf("help = %q, %v", out, err)
	}
	if !strings.Contains(volumeUsageText("afs"), "save [--timeout 2m]") {
		t.Fatal("volume help omits save")
	}
	out, err = captureStdout(t, func() error { return cmdVolume([]string{"vol", "save", "--json"}) })
	var result syncControlResult
	if err == nil || json.Unmarshal([]byte(out), &result) != nil || result.Success || result.Error == "" {
		t.Fatalf("invalid request must produce JSON failure: %q, %v", out, err)
	}
}

func TestSyncSaveMountLookup(t *testing.T) {
	withTempHome(t)
	cfg := defaultConfig()
	saveTempConfig(t, cfg)
	rec := mountRecord{Workspace: "notes", WorkspaceID: "volume-id", LocalPath: t.TempDir(), Mode: modeSync, PID: os.Getpid()}
	if err := saveMountRegistry(mountRegistry{Mounts: []mountRecord{rec}}); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"notes", "volume-id", rec.LocalPath, filepath.Join(rec.LocalPath, ".")} {
		got, err := resolveSyncSaveMount(cfg, target)
		if err != nil || got.LocalPath != rec.LocalPath {
			t.Fatalf("lookup %q = %+v, %v", target, got, err)
		}
	}
	if _, err := resolveSyncSaveMount(cfg, filepath.Join(rec.LocalPath, "child")); err == nil {
		t.Fatal("accepted a path subset")
	}
	other := rec
	other.LocalPath = t.TempDir()
	if err := saveMountRegistry(mountRegistry{Mounts: []mountRecord{rec, other}}); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveSyncSaveMount(cfg, "notes"); err == nil || !strings.Contains(err.Error(), "multiple") {
		t.Fatalf("ambiguous lookup = %v", err)
	}
	if got, err := resolveSyncSaveMount(cfg, rec.LocalPath); err != nil || got.LocalPath != rec.LocalPath {
		t.Fatalf("explicit lookup = %+v, %v", got, err)
	}
}

func TestSyncSaveLegacyLookupIsConfigScoped(t *testing.T) {
	withTempHome(t)
	cfg := defaultConfig()
	saveTempConfig(t, cfg)
	st := state{CurrentWorkspace: "notes", CurrentWorkspaceID: "volume-id", LocalPath: t.TempDir(),
		Mode: modeSync, SyncPID: os.Getpid(), ProductMode: cfg.ProductMode, RedisAddr: cfg.RedisAddr, RedisDB: cfg.RedisDB}
	if err := saveState(st); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{st.CurrentWorkspace, st.CurrentWorkspaceID, st.LocalPath} {
		if got, err := resolveSyncSaveMount(cfg, target); err != nil || got.PID != os.Getpid() {
			t.Fatalf("legacy lookup = %+v, %v", got, err)
		}
	}
	// An explicit config must not silently select the default config's daemon.
	if err := writeSyncControlJSON(defaultStatePath(), st, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(statePath()); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveSyncSaveMount(cfg, "notes"); err == nil {
		t.Fatal("used another config's default state")
	}
	st.RedisAddr = "another-redis:6379"
	if err := saveState(st); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveSyncSaveMount(cfg, "notes"); err == nil {
		t.Fatal("used a daemon for another Redis configuration")
	}
}

func TestSyncSaveRejectsInvalidMount(t *testing.T) {
	base := mountRecord{Workspace: "notes", LocalPath: t.TempDir(), Mode: modeSync, PID: os.Getpid()}
	for name, change := range map[string]func(*mountRecord){
		"readonly": func(r *mountRecord) { r.ReadOnly = true },
		"stopped":  func(r *mountRecord) { r.PID = 0 },
		"live":     func(r *mountRecord) { r.Mode = modeMount },
		"missing":  func(r *mountRecord) { r.LocalPath = filepath.Join(t.TempDir(), "missing") },
		"empty":    func(r *mountRecord) { r.LocalPath = "" },
		"relative": func(r *mountRecord) { r.LocalPath = "." },
		"identity": func(r *mountRecord) { r.Workspace = "" },
		"symlink": func(r *mountRecord) {
			p := filepath.Join(t.TempDir(), "link")
			if err := os.Symlink(r.LocalPath, p); err != nil {
				t.Fatal(err)
			}
			r.LocalPath = p
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := base
			change(&rec)
			if err := validateSyncSaveMount(rec); err == nil {
				t.Fatal("invalid mount accepted")
			}
		})
	}
	if err := validateSyncSaveMount(base); err != nil {
		t.Fatal(err)
	}
}

// This responder exercises the real on-disk transport without a Redis daemon.
func respondToSyncSave(root string, reply func(syncControlRequest) syncControlResult) <-chan error {
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
				data, err := os.ReadFile(paths[0])
				if err != nil {
					done <- err
					return
				}
				var req syncControlRequest
				if err := json.Unmarshal(data, &req); err != nil {
					done <- err
					return
				}
				if req.Operation != syncControlOpSave || req.Version != syncControlVersion || req.Volume != "notes" || req.LocalRoot != root || req.Path != "" || req.Content != "" || req.DeadlineUnixMilli <= time.Now().UnixMilli() {
					done <- fmt.Errorf("invalid request identity or deadline: %+v", req)
					return
				}
				info, err := os.Stat(paths[0])
				if err != nil {
					done <- err
					return
				}
				if info.Mode().Perm() != 0o600 {
					done <- errors.New("request permissions are not 0600")
					return
				}
				id := strings.TrimSuffix(filepath.Base(paths[0]), ".json")
				done <- writeSyncControlJSON(syncControlResultPath(root, id), reply(req), 0o600)
				return
			}
			time.Sleep(time.Millisecond)
		}
		done <- errors.New("save request was never written")
	}()
	return done
}

func TestSyncSaveCommandStructuredResults(t *testing.T) {
	for _, success := range []bool{true, false} {
		t.Run(fmt.Sprint(success), func(t *testing.T) {
			withTempHome(t)
			saveTempConfig(t, defaultConfig())
			root := t.TempDir()
			if err := saveMountRegistry(mountRegistry{Mounts: []mountRecord{{Workspace: "notes", LocalPath: root, Mode: modeSync, PID: os.Getpid()}}}); err != nil {
				t.Fatal(err)
			}
			done := respondToSyncSave(root, func(req syncControlRequest) syncControlResult {
				result := syncControlResult{Version: syncControlVersion, Operation: syncControlOpSave, Volume: req.Volume, LocalRoot: req.LocalRoot, Success: success}
				if success {
					result.Save = &syncSaveReceipt{Entries: 2, Files: 1, Bytes: 7, TreeSHA256: strings.Repeat("a", 64), CompletedAt: time.Now().UTC()}
				} else {
					result.Error = "local and remote edits conflict"
				}
				return result
			})
			out, err := captureStdout(t, func() error { return cmdVolume([]string{"vol", "save", "notes", "--json", "--timeout", "1s"}) })
			if replyErr := <-done; replyErr != nil {
				t.Fatal(replyErr)
			}
			var result syncControlResult
			if decodeErr := json.Unmarshal([]byte(out), &result); decodeErr != nil {
				t.Fatalf("stdout is not one JSON result: %q, %v", out, decodeErr)
			}
			if result.Success != success || result.Volume != "notes" || result.LocalRoot != root || (err == nil) != success {
				t.Fatalf("result = %+v, %v", result, err)
			}
			if success && (result.Save == nil || result.Save.Bytes != 7) {
				t.Fatalf("receipt missing: %+v", result)
			}
			if !success && result.Error != "local and remote edits conflict" {
				t.Fatalf("error lost: %+v", result)
			}
			for _, rel := range []string{syncControlRequestsDirName, syncControlResultsDirName} {
				entries, err := os.ReadDir(filepath.Join(root, rel))
				if err != nil || len(entries) != 0 {
					t.Fatalf("control files left behind: %s, %v", rel, err)
				}
			}
		})
	}
}

func TestSyncSaveRejectsWrongReplyOrMissingReceipt(t *testing.T) {
	for _, invalid := range []string{"volume", "root", "operation", "version", "receipt"} {
		t.Run(invalid, func(t *testing.T) {
			root := t.TempDir()
			done := respondToSyncSave(root, func(req syncControlRequest) syncControlResult {
				r := syncControlResult{Version: syncControlVersion, Operation: syncControlOpSave, Volume: req.Volume, LocalRoot: req.LocalRoot, Success: true, Save: &syncSaveReceipt{}}
				switch invalid {
				case "volume":
					r.Volume = "another"
				case "root":
					r.LocalRoot = "/other"
				case "operation":
					r.Operation = syncControlOpUndelete
				case "version":
					r.Version++
				case "receipt":
					r.Save = nil
				}
				return r
			})
			_, err := runSyncSaveControlRequest(root, syncControlRequest{Version: syncControlVersion, Operation: syncControlOpSave, Volume: "notes", LocalRoot: root, DeadlineUnixMilli: time.Now().Add(time.Second).UnixMilli()})
			if replyErr := <-done; replyErr != nil {
				t.Fatal(replyErr)
			}
			if err == nil {
				t.Fatal("accepted invalid success reply")
			}
		})
	}
}

func TestSyncSaveTimeoutAndControlScope(t *testing.T) {
	root := t.TempDir()
	started := time.Now()
	result, err := runSyncSaveControlRequest(root, syncControlRequest{Version: syncControlVersion, Operation: syncControlOpSave, Volume: "notes", LocalRoot: root, DeadlineUnixMilli: time.Now().Add(60 * time.Millisecond).UnixMilli()})
	if err == nil || result.Success || !strings.Contains(err.Error(), "unconfirmed") || time.Since(started) > time.Second {
		t.Fatalf("timeout = %+v, %v, after %s", result, err, time.Since(started))
	}
	entries, err := os.ReadDir(filepath.Join(root, syncControlRequestsDirName))
	if err != nil || len(entries) != 0 {
		t.Fatalf("queued request was not removed: %v", err)
	}
	other := t.TempDir()
	root = t.TempDir()
	if err := os.Symlink(other, filepath.Join(root, syncControlDirName)); err != nil {
		t.Fatal(err)
	}
	if _, err := runSyncSaveControlRequest(root, syncControlRequest{Volume: "notes", LocalRoot: root, DeadlineUnixMilli: time.Now().Add(time.Second).UnixMilli()}); err == nil {
		t.Fatal("followed control directory symlink")
	}
	if entries, err := os.ReadDir(other); err != nil || len(entries) != 0 {
		t.Fatalf("wrote outside mount: %v", err)
	}
}
