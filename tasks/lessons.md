## 2026-03-04
- Pattern: Report generation from logs can undercount/misrepresent signal volume; prefer canonical DB event tables for signal audits.
- Guardrail: For "every signal" requests, source from `system_events` (`SIGNAL_GENERATED`) first, then cross-check logs only for supplemental fields.

## 2026-03-08
- Pattern: Returning SQLAlchemy ORM rows outside session scope can trigger high-volume `DetachedInstanceError` in live loops when fields are accessed later.
- Guardrail: Repository read APIs that feed runtime managers should return detached-safe payloads (dict/dataclass), not live ORM model objects.
- Pattern: `make deploy` is blocked by any untracked local files (for example `tasks/todo.md`) due the clean-working-tree gate.
- Guardrail: Before deploy, either commit/stash/remove untracked files or use `./scripts/deploy.sh --skip-commit` when code is already pushed and server-safe.
