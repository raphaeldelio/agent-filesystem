# Concurrent overflow recovery

Status: complete. Owner: Codex. Created and updated: September 10, 2026.

Goal: repair the false conflicts, stale queued work and duplicate deletion errors reproduced when watcher overflow recovery runs beside background transfers.

Scope: sync uploader, downloader, event result handling, full reconciliation and regression tests. Preserve true conflict copies, retry stale observations and preserve separate local and remote timestamps. Keep this branch independently applicable to main and composable with PR 28 in either order. Do not publish.

Validation: deterministic regressions for both reproduced interleavings and real conflicts, full CLI suite, race detector, make commands, vet, patch composition, Linux ARM64 build, real AgentCore package installation and exact stress tree restoration with no manual reconciliation.

Execution: uploader and downloader file ownership delegated independently; root owns full and event reconciliation and integration. Primary workspace plans/packets/overflow-races contains contracts. Original failing AgentCore evidence is retained in the primary workspace results.

Result: production commit d81eddd9e60219826a7aaab7b1f21cb4a5453d27 repairs the reproduced races. Full CLI race tests passed three repetitions in 38.520 seconds; commands, vet and focused regressions passed. The final combined Linux ARM64 build passed 51 focused tests twice. Both independent patches apply in either order with identical trees.

AgentCore runtime version 6 used the final combined binary. The queue capacity 8 stress workload recovered its exact independent 1,750 file tree after dropped notifications. The default queue package workload restored all 7,081 entries and 6,444 files, verified all 4,142 package RECORD hashes and executed pandas and NumPy. Guarded remote audits compared all file bytes. Both tests observed writer stop completion before empty-directory restoration in a fresh session; all four test sessions were stopped. No manual reconciliation, unwanted conflict copies, Redis missing key log entries or failed conflict checkpoints were observed in the final runs.

A late review regression reproduced local data loss after a missing remote rename followed by a failed or obsolete edited upload. Staging and capturing the final provisional destination before enqueueing the rename fixed both failures. The chunked stale-followup regression also passed independently of PR 28. Original failed and intermediate cloud evidence is preserved in the POC workspace.

The final package background window still had sync activity after 120 seconds; subsequent remote byte equality and successful restoration establish the accepted result. These tests do not establish atomic saves, durability for unsynced local writes, Redis disk durability, or behavior under arbitrary sustained load. Save completion APIs remain outside this change. No GitHub changes were published.
