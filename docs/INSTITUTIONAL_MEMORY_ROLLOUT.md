# Institutional Memory Rollout Gates

This rollout is staged to keep live execution safety unchanged until explicit enablement.

## Feature Flags

Under `strategy`:

- `memory_enabled`: master switch for thesis persistence and telemetry.
- `thesis_observe_only`: when `true`, only writes/logs conviction telemetry.
- `thesis_score_enabled`: enables conviction score adjustment in signal scoring (Phase 2).
- `thesis_management_enabled`: enables conviction-driven management behavior (Phase 3).
- `thesis_canary_symbols`: optional symbol allowlist for score/management impact.

## Deployment Order

1. **Phase 1 (observe-only)**
   - `memory_enabled: true`
   - `thesis_observe_only: true`
   - `thesis_score_enabled: false`
   - `thesis_management_enabled: false`
2. **Phase 2 (scoring canary)**
   - `thesis_observe_only: false`
   - `thesis_score_enabled: true`
   - `thesis_management_enabled: false`
   - set `thesis_canary_symbols` to a small subset.
3. **Phase 3 (management canary)**
   - keep Phase 2 flags.
   - set `thesis_management_enabled: true` for canary subset.

## Runtime Validation Checklist

- `thesis_conviction` logs appear with decay components.
- `DECISION_TRACE` includes:
  - `thesis_conviction`
  - `thesis_status`
  - `thesis_decay.time_decay`
  - `thesis_decay.zone_rejection`
  - `thesis_decay.volume_fade`
- `theses` table receives rows and conviction updates over time.
- In Phase 2, `score_breakdown` contains:
  - `thesis_conviction`
  - `thesis_score_adj`
- In Phase 3, conviction-driven exits/re-entry blocks only occur for canary symbols.

## Abort Conditions

Immediately roll back to observe-only if any of these regress:

- `INVARIANT K` increases.
- hard `FILL_ID_COLLISION` reappears.
- `ORPHANED` trend increases.
- unexpected rise in opens without corresponding thesis/conviction rationale.

Rollback config:

- `thesis_score_enabled: false`
- `thesis_management_enabled: false`
- `thesis_observe_only: true`
