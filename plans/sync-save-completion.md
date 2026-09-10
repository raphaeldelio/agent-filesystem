# Explicit sync save completion

Status: active. Owner: Codex. Created and updated: September 10, 2026.

Goal: expose a native operation that synchronizes a mounted local sync volume
and returns a reliable success or error once its contents are visible in Redis.

Scope: local CLI and daemon integration, completion checks, tests and current
documentation. Application writes must stop while saving. This does not promise
atomic snapshots under concurrent writes or Redis disk durability. Preserve
independence from the large file and watcher overflow PRs where correctness
permits. The user authorized implementation, validation and opening this PR.

Checklist: inspect existing surfaces and queues; choose the smallest completion
contract; implement it; test success, failures, timeout and concurrent changes;
validate immediate save and fresh restoration in AgentCore; update docs and
open the PR after verification.

In flight: implementation is complete and validation is in progress. The native
CLI uses existing local request files. An independent supervisor pauses and
joins each daemon generation, saves and verifies the included local tree, then
resumes fresh workers. The engine rejects divergent Redis state before writes
and checks bytes, modes, symlinks, the final local tree and state persistence.

Completed: engine, CLI, lifecycle cancellation, timeout and unavailable daemon
regressions; command builds and static analysis; independent test harness that
freezes the daemon immediately after success before auditing Redis.

Remaining: chunk metadata compatibility regression, final CLI race suite,
Linux ARM64 and Redis 8 validation, AgentCore package and stress restoration,
publication. Existing rename history assertion can race its asynchronous write;
this was reproduced on unchanged upstream main as well as the first full run.

Verification: targeted regressions and CLI suite with race detection, make
commands, vet, Linux ARM64 tests, real AgentCore package save and restoration,
failure responses, private evidence and public PR description review.
