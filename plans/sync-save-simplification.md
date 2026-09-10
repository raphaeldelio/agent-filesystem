# Save API simplification and profiling

Status: active. Owner: Codex. Created and updated: September10,2026.

Goal: remove the duplicate local scan and duplicated request file transport
while preserving the validated save guarantees, then profile package save.

Scope: reuse the local manifest verified after worker shutdown; share mechanical
control transport with operation-specific validation; temporary external profiling.
Keep the current lifecycle design, existing PR independence and all error contracts.

In flight: transport helper and profiling preparation run independently while
root changes manifest ownership. Remaining: review, focused/full CLI validation,
real AgentCore package save and restoration, profiling, docs and PR30 update.

Verification: race tests, make commands, vet, exact native binary AgentCore
acceptance and separately identified temporary instrumentation. No known blockers.
