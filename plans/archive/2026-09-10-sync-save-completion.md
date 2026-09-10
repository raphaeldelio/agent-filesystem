# Explicit sync save completion

Status: completed. Owner: Codex. Created and updated: September 10, 2026.

## Result

Added `afs vol save [--timeout 2m] [--json] <volume|directory>` for a complete
mounted sync volume. An independent local control supervisor pauses and joins
the daemon's background work. Active outbound writes finish within the request
deadline; cancelled inbound work preserves local edits. Save compares the local
tree before and after the pause, applies pending changes, verifies Redis bytes
and metadata, persists checked sync state and resumes fresh workers before
returning success with a receipt.

The caller must stop application and remote writers. Ignore rules and the file
size limit apply. Failures and timeouts do not confirm completion and can leave
partial remote writes. Success establishes Redis visibility and does not promise
Redis disk durability, concurrent atomic snapshots or checkpoint creation.

Implementation starts directly from main c23daa6552fb6e7c5e795ae5689c5cff6499b2c5.
It requires neither the chunk upload PR28 nor watcher overflow PR29. No
implementation work remains. The CLI reference and user guide describe the
command, failure behavior, mount identity and restart requirements.

## Verification

The complete CLI race suite passed in 15.400 seconds. `make commands`,
`go vet ./cmd/afs` and Linux ARM64 save tests passed. Regressions cover missed
events, large files and subsequent chunk edits, modes, symlinks, deletes,
renames, conflicts, failed and ambiguous writes, readback and state persistence
failures, deadlines and worker shutdown. The existing rename history test now
waits for its asynchronous history write; the timing issue was also reproduced
on unchanged main. The external acceptance harness passed all 26 tests.

Real AWS AgentCore microVMs with Redis 8 passed standalone save, writer stop
and fresh reader restoration. Stress contained 1,755 entries and 1,752 files;
save took 27.332 seconds. The Python environment contained 7,081 entries,
6,444 files and 148,868,251 bytes, including nine files above 1 MiB. Save took
228.206 seconds with a ten minute deadline. The fresh environment passed its
analytics and all 4,142 package RECORD hashes. Complete writer and reader trees
matched, and actual Redis bytes and metadata were audited with zero mismatches.
The writer daemon was confirmed stopped before the audit. The package run
recorded 7,904 watcher backpressure events and still saved every included file.

Four successive large file saves passed after creation, editing, truncation and
expansion. Each receipt matched the expected native tree hash and passed a
complete Redis byte audit. These saves took 0.15 to 0.20 seconds each.

A separate checkout combining this implementation with unchanged PR28 and PR29
passed the complete CLI race suite in 17.721 seconds. Its real AgentCore stress
save took 23.083 seconds and fresh restoration matched all entries, bytes and
metadata. All acceptance sessions were stopped. Raw outputs and image/source
provenance are retained outside this repository.
