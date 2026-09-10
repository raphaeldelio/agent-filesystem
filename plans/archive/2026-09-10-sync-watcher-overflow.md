# Sync watcher overflow recovery

Status: initial implementation complete; see subsequent recovery validation below
Owner: Codex
Created: 2026-09-10
Updated: 2026-09-10

This archive records the initial implementation stage. Subsequent uploader and
reconciliation changes and final validation are recorded in
[Concurrent overflow recovery](2026-09-10-sync-overflow-races.md). The scope and
publication status below describe that earlier stage. The user has since
authorized creating the PR.

## Goal

Recover automatically when the local watcher loses events, without depending on the chunked upload PR.

## Scope

Signal output queue saturation and fsnotify overflow through a separate coalesced channel. Refresh directory watches and run an existing warm reconciliation. Preserve a recovery request received during a scan and retry failed recovery with a bounded delay. Record actual remote modification times after reconciliation uploads so later local edits do not cause false conflicts. No save API or changes to the background chunked uploader.

## Checklist

1. Implement notification and daemon recovery.
2. Add deterministic queue and kernel overflow tests, recovery integration, and hidden directory coverage.
3. Run targeted race tests, the CLI suite, builds, and vet.
4. Review the independent diff and prepare a local PR description.

## In flight

Implementation, tests, and independent code review are complete. Publication awaits user approval.

## Decisions and blockers

Based directly on main at c23daa6552fb6e7c5e795ae5689c5cff6499b2c5. Do not push or open a PR until the user approves. Reuse warm reconciliation because cold hydration can replace a local tree containing only hidden files. Recovery is eventual and does not establish a save completion or crash durability guarantee.

## Verification

Queue saturation and kernel overflow tests failed before wiring notifications. A deterministic initial upload test also reproduced a false edit conflict caused by recording the local mtime as the remote mtime.

Final validation on macOS ARM64 with Go 1.26.5 passed the complete CLI suite, targeted watcher and overflow race tests, make commands, and go vet ./cmd/afs. All 12 targeted watcher and overflow tests passed in an isolated Linux ARM64 container with networking disabled.

## Result

Watcher loss now requests warm reconciliation through a separate coalesced channel. Recovery repairs watches, retries failures, retains requests received during an active scan, preserves hidden local trees, and records accurate remote timestamps after uploads. The background chunked uploader is unchanged. No remote branch or pull request has been created for this change.
