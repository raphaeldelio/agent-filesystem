package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestConfigWatcherQueueCapacityCommands(t *testing.T) {
	withTempHome(t)
	cfg := defaultConfig()
	cfg.SyncFileSizeCapMB = 4096
	saveTempConfig(t, cfg)
	const key = "sync.watcherQueueCapacity"
	for _, capacity := range []int{1, 8192, maxSyncWatcherQueueCapacity, 0} {
		if _, err := captureStdout(t, func() error {
			return cmdConfig([]string{"config", "set", "SYNC.WATCHERQUEUECAPACITY", strconv.Itoa(capacity)})
		}); err != nil {
			t.Fatalf("set capacity %d: %v", capacity, err)
		}
		want := capacity
		if want == 0 {
			want = defaultSyncWatcherQueueCapacity
		}
		loaded, err := loadConfig()
		if err != nil {
			t.Fatal(err)
		}
		if syncWatcherQueueCapacity(loaded) != want || loaded.SyncFileSizeCapMB != 4096 {
			t.Fatalf("loaded sync settings = %+v, want capacity %d and file cap 4096", loaded.syncSettings, want)
		}
		for _, command := range [][]string{
			{"config", "get", "--json", key},
			{"config", "list", "--json"},
		} {
			out, err := captureStdout(t, func() error { return cmdConfig(command) })
			if err != nil {
				t.Fatal(err)
			}
			var values map[string]string
			if err := json.Unmarshal([]byte(out), &values); err != nil {
				t.Fatal(err)
			}
			if values[key] != strconv.Itoa(want) {
				t.Fatalf("%v returned capacity %q, want %d", command, values[key], want)
			}
		}
	}
	if _, err := captureStdout(t, func() error {
		return cmdConfig([]string{"config", "set", key, "8192"})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := captureStdout(t, func() error {
		return cmdConfig([]string{"config", "unset", key})
	}); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if syncWatcherQueueCapacity(loaded) != defaultSyncWatcherQueueCapacity || loaded.SyncFileSizeCapMB != 4096 {
		t.Fatalf("unset changed unrelated sync settings: %+v", loaded.syncSettings)
	}
}

func TestConfigWatcherQueueCapacityRoundTripPreservesFileCap(t *testing.T) {
	for _, fileCap := range []int{defaultSyncFileSizeCapMB, 4096} {
		t.Run(strconv.Itoa(fileCap), func(t *testing.T) {
			withTempHome(t)
			cfg := defaultConfig()
			cfg.SyncFileSizeCapMB = fileCap
			cfg.SyncWatcherQueueCapacity = 8192
			saveTempConfig(t, cfg)
			// Changing another setting must preserve both sync values.
			if _, err := captureStdout(t, func() error {
				return cmdConfig([]string{"config", "set", "agent.name", "capacity test"})
			}); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(configPath())
			if err != nil {
				t.Fatal(err)
			}
			var saved struct {
				Sync map[string]int `json:"sync"`
			}
			if err := json.Unmarshal(raw, &saved); err != nil {
				t.Fatal(err)
			}
			if saved.Sync["watcherQueueCapacity"] != 8192 || saved.Sync["fileSizeCapMB"] != fileCap {
				t.Fatalf("saved sync settings = %v, want capacity 8192 and file cap %d", saved.Sync, fileCap)
			}
			loaded, err := loadConfig()
			if err != nil {
				t.Fatal(err)
			}
			if syncWatcherQueueCapacity(loaded) != 8192 || syncSizeCapBytes(loaded) != int64(fileCap)*1024*1024 {
				t.Fatalf("loaded sync settings changed: %+v", loaded.syncSettings)
			}
		})
	}
}

func TestConfigWatcherQueueCapacityRejectsInvalidValues(t *testing.T) {
	withTempHome(t)
	cfg := defaultConfig()
	cfg.SyncWatcherQueueCapacity = 8192
	saveTempConfig(t, cfg)
	before, err := os.ReadFile(configPath())
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"-1", "1.5", "abc", "", strconv.Itoa(maxSyncWatcherQueueCapacity + 1), "999999999999999999999999"} {
		if err := cmdConfig([]string{"config", "set", "sync.watcherQueueCapacity", value}); err == nil {
			t.Fatalf("accepted invalid capacity %q", value)
		}
		after, err := os.ReadFile(configPath())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before, after) {
			t.Fatalf("invalid capacity %q changed the saved config", value)
		}
	}
	for _, value := range []string{"-1", "1.5", `"8192"`, "true", strconv.Itoa(maxSyncWatcherQueueCapacity + 1)} {
		raw := fmt.Sprintf(`{"sync":{"watcherQueueCapacity":%s}}`, value)
		if err := os.WriteFile(configPath(), []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := cmdConfig([]string{"config", "get", "sync.watcherQueueCapacity"}); err == nil {
			t.Fatalf("accepted invalid saved capacity %s", value)
		}
	}
}

func TestConfigWatcherQueueCapacityHelp(t *testing.T) {
	for _, help := range []string{configUsageText("afs"), configSetUsageText("afs")} {
		if !strings.Contains(help, "sync.watcherQueueCapacity") {
			t.Fatal("config help does not expose watcher queue capacity")
		}
		if strings.Contains(help, "%!") {
			t.Fatalf("config help has a formatting error: %s", help)
		}
	}
}
