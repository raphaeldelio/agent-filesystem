package main

import (
	"context"
	"os"
)

type pendingSyncUpload struct {
	count  int
	rescan bool
}

// Only paths represented in the bounded upload/result channels are tracked.
// A scan must wait for their results, including chunked creates whose remote
// inode exists briefly before all content has been written.
func (r *reconciler) enqueueTrackedUpload(op uploadOp) {
	r.state.mu.Lock()
	if r.pendingUploads == nil {
		r.pendingUploads = make(map[string]pendingSyncUpload)
	}
	pending := r.pendingUploads[op.Path]
	pending.count++
	r.pendingUploads[op.Path] = pending
	if op.Kind == opUploadRename && op.PrevPath != "" {
		previous := r.pendingUploads[op.PrevPath]
		previous.count++
		r.pendingUploads[op.PrevPath] = previous
	}
	r.state.mu.Unlock()
	op.Tracked = true
	r.uploadCh <- op
}

func (r *reconciler) deferScanForPendingUpload(path string) bool {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	pending, exists := r.pendingUploads[path]
	if exists {
		pending.rescan = true
		r.pendingUploads[path] = pending
	}
	return exists
}

func (r *reconciler) finishPendingUpload(path string) {
	r.state.mu.Lock()
	pending, exists := r.pendingUploads[path]
	if exists {
		pending.count--
		if pending.count == 0 {
			delete(r.pendingUploads, path)
		} else {
			r.pendingUploads[path] = pending
		}
	}
	r.state.mu.Unlock()
	if exists && pending.count == 0 && pending.rescan {
		r.requestFullSweep()
	}
}

func (f *fullReconciler) remoteStillCurrent(ctx context.Context, a syncAction) (bool, error) {
	if !a.checkState {
		return true, nil
	}
	stat, err := f.r.fs.Stat(ctx, absoluteRemotePath(a.path))
	if err != nil && !isClientNotFound(err) {
		return false, err
	}
	current := stat == nil && a.remoteMeta == nil
	if stat != nil && a.remoteMeta != nil {
		r := a.remoteMeta
		current = stat.Type == r.kind && stat.Size == r.size && stat.Mtime == r.mtimeMs && stat.Mode == r.mode
	}
	if !current {
		f.r.requestFullSweep()
	}
	return current, nil
}

// An upload can finish before recovery records a newer version, while its
// result is still queued. Do not replace that newer baseline with the delayed
// result when the current local file still agrees with the newer baseline.
func (r *reconciler) uploadResultObsolete(res uploadResult) bool {
	r.state.mu.Lock()
	entry, exists := r.state.state.Entries[res.Op.Path]
	r.state.mu.Unlock()
	if !exists {
		return false
	}
	if entry.Deleted {
		return true
	}
	if res.RemoteStat != nil && entry.RemoteMtimeMs > res.RemoteStat.Mtime {
		return true
	}
	if entry.Version == res.Op.StoredEntry.Version || entry.RemoteHash == "" ||
		entry.RemoteHash == res.Op.LocalHash || entry.LocalHash != entry.RemoteHash {
		return false
	}
	info, err := os.Lstat(res.Op.AbsPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() != entry.Size ||
		info.ModTime().UnixMilli() != entry.LocalMtimeMs || uint32(info.Mode().Perm()) != entry.Mode {
		return false
	}
	if entry.ChunkSize > 0 {
		hashes, _, err := streamChunkHashes(res.Op.AbsPath, entry.ChunkSize)
		return err == nil && compositeHash(hashes) == entry.LocalHash
	}
	data, err := os.ReadFile(res.Op.AbsPath)
	return err == nil && sha256Hex(data) == entry.LocalHash
}

// A full scan runs beside the event worker. Its observations may be obsolete
// by the time a worker reaches an action. Keep the next scan pending whenever
// either the baseline or the local path changed, rather than applying that plan.
func (f *fullReconciler) actionStillCurrent(a syncAction) bool {
	if !a.checkState {
		return true
	}
	if f.r.deferScanForPendingUpload(a.path) {
		return false
	}
	f.r.state.mu.Lock()
	entry, exists := f.r.state.state.Entries[a.path]
	current := exists == a.hasStored && (!exists || entry.Version == a.storedEntry.Version)
	f.r.state.mu.Unlock()
	if current {
		info, err := os.Lstat(a.absPath)
		if a.localMeta == nil {
			current = os.IsNotExist(err)
		} else if err != nil {
			current = false
		} else {
			l := a.localMeta
			switch l.kind {
			case "file":
				current = info.Mode().IsRegular() && info.Size() == l.size &&
					uint32(info.Mode().Perm()) == l.mode && info.ModTime().UnixMilli() == l.mtimeMs
				if l.mtimeNs != 0 {
					current = current && info.ModTime().UnixNano() == l.mtimeNs
				}
			case "dir":
				current = info.IsDir()
			case "symlink":
				target, err := os.Readlink(a.absPath)
				current = info.Mode()&os.ModeSymlink != 0 && err == nil && target == l.target
			}
		}
	}
	if !current {
		f.r.requestFullSweep()
	}
	return current
}

func (f *fullReconciler) updateActionState(a syncAction, entry SyncEntry) {
	f.r.state.mu.Lock()
	current, exists := f.r.state.state.Entries[a.path]
	if a.checkState && (exists != a.hasStored || exists && current.Version != a.storedEntry.Version) {
		f.r.state.mu.Unlock()
		f.r.requestFullSweep()
		return
	}
	entry.Version = f.r.state.nextVersion()
	f.r.state.state.Entries[a.path] = entry
	f.r.state.dirty = true
	f.r.state.mu.Unlock()
}
