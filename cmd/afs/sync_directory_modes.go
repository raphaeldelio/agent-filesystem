package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// A full reconcile may create or update children of a read-only directory.
// Keep owner access until all local mutations finish, then restore modes from
// deepest to shallowest, including when the pass fails or is cancelled. The
// sync baseline continues to hold the final mode, never the temporary one.
type syncDirectoryModes struct {
	r        *reconciler
	prepared map[string]bool
	changed  map[string]fs.FileInfo
}

func newSyncDirectoryModes(r *reconciler) *syncDirectoryModes {
	return &syncDirectoryModes{r: r, prepared: make(map[string]bool), changed: make(map[string]fs.FileInfo)}
}

func (m *syncDirectoryModes) prepare(abs string) error {
	abs = filepath.Clean(abs)
	// The mount root's access policy is managed by the mount lifecycle.
	if abs == m.r.root || m.prepared[abs] {
		return nil
	}
	rel, err := filepath.Rel(m.r.root, abs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("directory %s is outside sync root", abs)
	}
	if err := m.prepare(filepath.Dir(abs)); err != nil {
		return err
	}
	info, err := os.Lstat(abs)
	if os.IsNotExist(err) {
		// A later mkdir action creates it; prepare it after that action.
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("sync directory %s is not a directory", rel)
	}
	if info.Mode().Perm()&0o700 != 0o700 {
		m.r.echo.markDir(filepath.ToSlash(rel))
		if err := os.Chmod(abs, info.Mode()|0o700); err != nil {
			return fmt.Errorf("prepare sync directory %s: %w", rel, err)
		}
		m.changed[abs] = info
	}
	m.prepared[abs] = true
	return nil
}

func (m *syncDirectoryModes) restore() error {
	paths := make([]string, 0, len(m.changed))
	for abs := range m.changed {
		paths = append(paths, abs)
	}
	// Descendants precede ancestors even when final modes remove search access.
	sort.Sort(sort.Reverse(sort.StringSlice(paths)))
	var result error
	for _, abs := range paths {
		before := m.changed[abs]
		now, err := os.Lstat(abs)
		if os.IsNotExist(err) {
			continue // The plan may have deleted this directory.
		}
		if err != nil {
			result = errors.Join(result, fmt.Errorf("stat sync directory %s: %w", abs, err))
			continue
		}
		if !os.SameFile(before, now) || now.Mode() != before.Mode()|0o700 {
			// Preserve application chmods and replacement directories. Their
			// new metadata belongs to a subsequent reconciliation pass.
			m.r.requestFullSweep()
			continue
		}
		rel, _ := filepath.Rel(m.r.root, abs)
		m.r.echo.markDir(filepath.ToSlash(rel))
		if err := os.Chmod(abs, before.Mode()); err != nil {
			result = errors.Join(result, fmt.Errorf("restore sync directory %s: %w", rel, err))
		}
	}
	return result
}
