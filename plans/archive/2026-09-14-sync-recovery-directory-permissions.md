# Sync recovery directory permissions

Status: complete
Owner: Codex
Created: 2026-09-14
Updated: 2026-09-14

## Goal

Fix the PR #29 regression where restrictive local directory modes prevent
reconciliation from materializing their children. Preserve Raphael's commits
and add the fix as a separate commit on `codex/pr29-permission-fix`.

## Scope

Full reconciliation's local directory access, regression tests, and current
documentation. PR #30, publishing, and merging are outside this step.

## Checklist

- [x] Reproduce new and pre-existing restrictive directory failures.
- [x] Restore directory permissions after descendants finish, including failure.
- [x] Verify nested directories, retries, updates, and concurrent mode changes.
- [x] Run targeted tests, the full CLI race suite, and `make commands`.
- [x] Record the repo lesson and archive this plan with the separate fix commit.

## In Flight

- No implementation or validation work remains.

## Decisions / Blockers

- Preserve permissions outside the short period needed to apply local changes.
- Restore only the directory inode and temporary mode owned by this pass;
  preserve later application changes.
- No blockers.

## Verification

- Prior review's minimal test failed on #29 and passed on its base commit.
- New nested-directory tests fail on unmodified #29 with permission denied for
  both new and existing local directories. Error/cancellation tests also fail
  before they can reach the injected remote read.
- Targeted permission, existing mode-convergence, and stale-plan tests pass
  after the fix, including read-error/cancellation retries and local chmods.
- `go test -race ./cmd/afs -count=1` passed (11.631s), including nested new and
  pre-existing read-only directories, later remote updates/deletes, retries
  after read errors/cancellation, application chmods, and inode replacement.
- `make commands` built both `afs` and `afs-control-plane` successfully.
- `git diff --check` passed. Validation used local miniredis fixtures on macOS;
  no user's control plane or mount was restarted.

## Result

Reconciliation now prepares directories parent-first, temporarily grants owner
access to directories needed by local mutations, and restores their modes after
all workers join. Cleanup runs on failure and cancellation and preserves later
application chmods and replacement directories. Existing directories without a
mkdir action are included, so retries and later updates can make progress.

The fix, tests, current documentation, and repo lesson are recorded in a separate
commit on `codex/pr29-permission-fix`, directly after Raphael's `7664019` head.
Publishing, merging, and PR #30 remain outside this step.
