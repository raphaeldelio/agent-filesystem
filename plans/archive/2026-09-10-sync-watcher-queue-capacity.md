# Configurable sync watcher queue

Status: implementation complete, unpublished
Owner: Codex
Created: 2026-09-10
Updated: 2026-09-10

## Goal

Let users tune the watcher event queue without rebuilding AFS.

## Scope

Add sync.watcherQueueCapacity to saved configuration and wire all sync daemon creation paths. Keep 1024 as the default and the recovery notification channel at capacity one. Accept zero as the default and positive capacities through 1048576; reject invalid values before allocation. Changes apply when the daemon next starts.

## Checklist

1. Implement configuration persistence, commands, validation, and runtime wiring.
2. Test configuration round trips and actual watcher allocation and overflow.
3. Update docs, run CLI and race tests, build, and verify Linux behavior.
4. Refresh the unpublished independent overflow PR artifacts.

## In Flight

Implementation and independent review are complete. No remote branch or PR was created.

## Decisions / Blockers

No hot resizing. Do not publish or change PR 28. Larger buffers absorb bursts but cannot replace overflow recovery or guarantee crash durability.

## Verification

The full CLI suite passed in 8.743 seconds. Targeted watcher, recovery, daemon capacity, and config race tests passed in 3.502 seconds. All 19 targeted tests passed in an isolated Linux ARM64 container with networking disabled. The additional background bootstrap handoff regression passed on macOS. make commands and go vet ./cmd/afs passed.

## Result

Configuration commands now support a validated persistent queue capacity. All three production daemon creation paths use it. Tests cover saturation at configured capacities, defaults, invalid values, configuration preservation, and the background daemon handoff. Docs describe resource costs, restart behavior, and durability limits.
