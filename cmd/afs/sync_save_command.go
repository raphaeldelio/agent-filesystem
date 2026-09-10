package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const defaultSyncSaveTimeout = 2 * time.Minute

type syncSaveOptions struct {
	target  string
	timeout time.Duration
	json    bool
	help    bool
}

func parseSyncSaveOptions(args []string) (syncSaveOptions, error) {
	opts := syncSaveOptions{timeout: defaultSyncSaveTimeout}
	for _, arg := range args {
		if arg == "--json" {
			opts.json = true
		}
	}
	positional := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !positional {
			switch {
			case arg == "--":
				positional = true
				continue
			case isHelpArg(arg):
				opts.help = true
				continue
			case arg == "--json":
				continue
			case arg == "--timeout" || strings.HasPrefix(arg, "--timeout="):
				value := strings.TrimPrefix(arg, "--timeout=")
				if arg == "--timeout" {
					i++
					if i == len(args) {
						return opts, errors.New("--timeout requires a duration")
					}
					value = args[i]
				}
				var err error
				opts.timeout, err = time.ParseDuration(value)
				if err != nil || opts.timeout <= 0 {
					return opts, errors.New("--timeout must be a positive duration, such as 2m")
				}
				continue
			case strings.HasPrefix(arg, "-"):
				return opts, fmt.Errorf("unknown save option %q", arg)
			}
		}
		if opts.target != "" {
			return opts, errors.New("save requires exactly one volume or mount directory")
		}
		opts.target = strings.TrimSpace(arg)
	}
	if !opts.help && opts.target == "" {
		return opts, errors.New("save requires a volume or mount directory")
	}
	return opts, nil
}

func cmdVolumeSave(args []string) error {
	opts, err := parseSyncSaveOptions(args)
	result := syncControlResult{Version: syncControlVersion, Operation: syncControlOpSave}
	if err != nil {
		return printSyncSaveResult(result, err, opts.json)
	}
	if opts.help {
		fmt.Fprint(os.Stdout, syncSaveUsageText(filepath.Base(os.Args[0])))
		return nil
	}
	cfg, err := loadAFSConfig()
	if err != nil {
		return printSyncSaveResult(result, err, opts.json)
	}
	rec, err := resolveSyncSaveMount(cfg, opts.target)
	if err != nil {
		return printSyncSaveResult(result, err, opts.json)
	}
	result.Volume, result.LocalRoot = syncSaveVolume(rec), rec.LocalPath
	if err := validateSyncSaveMount(rec); err != nil {
		return printSyncSaveResult(result, err, opts.json)
	}
	request := syncControlRequest{
		Version: syncControlVersion, Operation: syncControlOpSave,
		Volume: result.Volume, LocalRoot: result.LocalRoot,
		DeadlineUnixMilli: time.Now().Add(opts.timeout).UnixMilli(),
	}
	result, err = runSyncSaveControlRequest(rec.LocalPath, request)
	return printSyncSaveResult(result, err, opts.json)
}

// Registry mounts identify their own daemon. Legacy starts instead record one
// daemon in the selected config's state file, so never fall back to another
// config's default state for a save request.
func resolveSyncSaveMount(cfg config, target string) (mountRecord, error) {
	reg, err := loadMountRegistry()
	if err != nil {
		return mountRecord{}, err
	}
	localPath, pathErr := normalizeMountPath(target)
	var matches []mountRecord
	for _, rec := range reg.Mounts {
		if pathErr == nil && filepath.Clean(rec.LocalPath) == localPath {
			matches = append(matches, rec)
		}
	}
	if len(matches) == 0 && !unmountTargetLooksLikePath(target) {
		for _, rec := range reg.Mounts {
			if target == rec.Workspace || target == rec.WorkspaceID {
				matches = append(matches, rec)
			}
		}
	}
	if len(matches) > 1 {
		return mountRecord{}, fmt.Errorf("volume %q matches multiple mounts; specify the exact mount directory", target)
	}
	if len(matches) == 1 {
		rec := matches[0]
		if strings.TrimSpace(rec.LocalPath) != "" {
			rec.LocalPath = filepath.Clean(rec.LocalPath)
		}
		return rec, nil
	}
	st, err := loadStateFromPath(statePath())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return mountRecord{}, err
	}
	if err == nil && runtimeStateMatchesConfig(cfg, st) &&
		((pathErr == nil && strings.TrimSpace(st.LocalPath) != "" && filepath.Clean(st.LocalPath) == localPath) ||
			(!unmountTargetLooksLikePath(target) && (target == st.CurrentWorkspace || target == st.CurrentWorkspaceID))) {
		root := strings.TrimSpace(st.LocalPath)
		if root != "" {
			root = filepath.Clean(root)
		}
		return mountRecord{Workspace: st.CurrentWorkspace, WorkspaceID: st.CurrentWorkspaceID,
			LocalPath: root, Mode: st.Mode, PID: st.SyncPID,
			ReadOnly: st.ReadOnly}, nil
	}
	return mountRecord{}, fmt.Errorf("no mount found for %q in the mount registry or current config; specify its exact directory or use its --config", target)
}

func syncSaveVolume(rec mountRecord) string {
	if name := strings.TrimSpace(rec.Workspace); name != "" {
		return name
	}
	return strings.TrimSpace(rec.WorkspaceID)
}

func validateSyncSaveMount(rec mountRecord) error {
	if rec.Mode != modeSync {
		return errors.New("save requires a sync mount; live FUSE and NFS mounts are not supported")
	}
	if rec.ReadOnly {
		return errors.New("cannot save a readonly sync mount")
	}
	if rec.PID <= 0 || !processAlive(rec.PID) {
		return errors.New("sync daemon is not running; mount the volume before saving")
	}
	if syncSaveVolume(rec) == "" {
		return errors.New("mounted volume identity is missing")
	}
	if strings.TrimSpace(rec.LocalPath) == "" {
		return errors.New("local sync root is missing")
	}
	if !filepath.IsAbs(rec.LocalPath) {
		return errors.New("local sync root must be an absolute directory")
	}
	info, err := os.Lstat(rec.LocalPath)
	if err != nil {
		return fmt.Errorf("local sync root: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("local sync root must be an existing directory, not a symlink")
	}
	return nil
}

func runSyncSaveControlRequest(localRoot string, request syncControlRequest) (syncControlResult, error) {
	result := syncControlResult{Version: syncControlVersion, Operation: syncControlOpSave,
		Volume: request.Volume, LocalRoot: request.LocalRoot}
	deadline := time.UnixMilli(request.DeadlineUnixMilli)
	if request.DeadlineUnixMilli <= 0 || !time.Now().Before(deadline) {
		return result, errors.New("save deadline has expired")
	}
	// Do not follow a user-created control directory symlink outside the mount.
	for _, rel := range []string{syncControlDirName, syncControlRequestsDirName, syncControlResultsDirName} {
		path := filepath.Join(localRoot, rel)
		if err := os.Mkdir(path, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
			return result, err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return result, err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return result, fmt.Errorf("save control path %s must be a directory, not a symlink", rel)
		}
	}
	id, err := randomSuffix()
	if err != nil {
		return result, err
	}
	requestPath, resultPath := syncControlRequestPath(localRoot, id), syncControlResultPath(localRoot, id)
	defer func() { _ = os.Remove(requestPath); _ = os.Remove(resultPath) }()
	if err := writeSyncControlJSON(requestPath, request, 0o600); err != nil {
		return result, err
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	timeoutErr := errors.New("timed out waiting for save; completion is unconfirmed and partial work may have occurred")
	for {
		if !time.Now().Before(deadline) {
			return result, timeoutErr
		}
		data, err := os.ReadFile(resultPath)
		if err == nil {
			var reply syncControlResult
			if err := json.Unmarshal(data, &reply); err != nil {
				return result, fmt.Errorf("parse save result: %w", err)
			}
			if reply.Version != syncControlVersion || reply.Operation != syncControlOpSave ||
				reply.Volume != request.Volume || reply.LocalRoot != request.LocalRoot {
				return result, errors.New("save result does not match the requested volume and local root")
			}
			if !reply.Success {
				if reply.Error == "" {
					reply.Error = "save failed; completion was not confirmed"
				}
				return reply, errors.New(reply.Error)
			}
			if reply.Save == nil {
				return result, errors.New("save result is missing its verification receipt")
			}
			return reply, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return result, err
		}
		select {
		case <-timer.C:
			return result, timeoutErr
		case <-ticker.C:
		}
	}
}

func printSyncSaveResult(result syncControlResult, err error, jsonOut bool) error {
	if err != nil {
		result.Success = false
		result.Error = err.Error()
	}
	if jsonOut {
		if outputErr := json.NewEncoder(os.Stdout).Encode(result); outputErr != nil {
			return outputErr
		}
		return err
	}
	if err != nil {
		return err
	}
	printSection("Volume saved", []outputRow{
		{Label: "volume", Value: result.Volume}, {Label: "path", Value: result.LocalRoot},
		{Label: "files", Value: fmt.Sprint(result.Save.Files)}, {Label: "bytes", Value: fmt.Sprint(result.Save.Bytes)},
		{Label: "verified", Value: "Redis visibility"},
	})
	return nil
}

func syncSaveUsageText(bin string) string {
	return fmt.Sprintf(`Usage:
  %s vol save [--timeout 2m] [--json] <volume|directory>

Stop all application and remote writers, then save one complete mounted sync
volume. A successful result verifies actual file bytes and metadata in Redis.
Ignored paths are excluded. The sync daemon resumes after the operation.

Errors, conflicts, changes during verification, or timeout return failure.
Partial work may have occurred on failure. Save does not create a checkpoint,
provide an atomic snapshot during concurrent writes, or guarantee Redis disk
durability. Live mounts and readonly sync mounts are not supported.
`, bin)
}
