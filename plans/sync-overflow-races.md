# Concurrent overflow recovery

Status: active. Owner: Codex. Created and updated: September 10, 2026.

Goal: repair the false conflicts, stale queued work and duplicate deletion errors reproduced when watcher overflow recovery runs beside background transfers.

Scope: sync uploader, downloader, event result handling, full reconciliation and regression tests. Preserve true conflict copies, retry stale observations and preserve separate local and remote timestamps. Keep this branch independently applicable to main and composable with PR 28 in either order. Do not publish.

Validation: deterministic regressions for both reproduced interleavings and real conflicts, full CLI suite, race detector, make commands, vet, patch composition, Linux ARM64 build, real AgentCore package installation and exact stress tree restoration with no manual reconciliation.

Execution: uploader and downloader file ownership delegated independently; root owns full and event reconciliation and integration. Primary workspace plans/packets/overflow-races contains contracts. Original failing AgentCore evidence is retained in the primary workspace results.

Progress: reproductions confirmed; repairs in progress.
