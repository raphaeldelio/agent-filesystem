# Save API simplification and profiling

Status: completed. Owner: Codex. Created and updated: 2026-09-10.

## Result

Save reuses the manifest verified after worker shutdown, reducing full local
tree scans from five to four. The checks before applying changes and before
success remain. Shared request file transport removes a duplicated polling
loop while preserving the distinct save and legacy file operation contracts.
The save lifecycle and the independent base of PR30 are unchanged.

Production changes are pinned at `202ac685`. No permanent profiling hooks were
added. Independent review found no actionable issue. The source performance
notes describe the measured phases and limits; raw evidence remains external.

## Verification

Full CLI race suite passed in 15.268 seconds. Command builds, go vet, Linux ARM64
save/control tests and 26 POC harness tests passed. New regressions cover local
edits after manifest capture, existing file operation responses and pending
requests on timeout, malformed JSON, save expiry and protected control paths.
A temporary combination with unchanged PR28 and PR29 passed the full CLI race suite
in 18.183 seconds, with no conflicts applying this simplification.

The exact production binary passed AgentCore stress save and fresh restoration
of 1,755 entries and 1,752 files in 22.949 seconds. Four successive native saves passed
after large file creation, editing, truncation and expansion.

A temporary instrumented build of the same source saved the original Python
package workload in 266.699 seconds, with a ten minute timeout. It saved 7,081
entries, 6,444 files and 148,888,880 bytes. Frozen byte auditing and fresh restoration
matched the complete tree; all 4,142 package RECORD hashes and analytics passed.
All five sessions started for this validation were stopped.

Profiling measured 169.309 seconds applying changes and 93.827 seconds reading back
Redis. All four local scans together took 0.935 seconds. Client method counts and
phase totals were checked. The Redis host was resized to 8 GiB to provide memory
headroom while retaining prior test data, so comparison with the earlier
228.206 second run does not isolate the simplification's performance effect.

No implementation or validation work remains. The changes are ready to update
PR30; merging remains a separate review decision.
