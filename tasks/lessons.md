## 2026-03-04
- Pattern: Report generation from logs can undercount/misrepresent signal volume; prefer canonical DB event tables for signal audits.
- Guardrail: For "every signal" requests, source from `system_events` (`SIGNAL_GENERATED`) first, then cross-check logs only for supplemental fields.

## 2026-03-08
- Pattern: Returning SQLAlchemy ORM rows outside session scope can trigger high-volume `DetachedInstanceError` in live loops when fields are accessed later.
- Guardrail: Repository read APIs that feed runtime managers should return detached-safe payloads (dict/dataclass), not live ORM model objects.
- Pattern: `make deploy` is blocked by any untracked local files (for example `tasks/todo.md`) due the clean-working-tree gate.
- Guardrail: Before deploy, either commit/stash/remove untracked files or use `./scripts/deploy.sh --skip-commit` when code is already pushed and server-safe.

## 2026-03-15
- Pattern: When research quality is low due to missing timeframe coverage, proposing gating/bypass without first exhausting data acquisition violates user intent.
- Guardrail: For replay/data-quality failures, always prioritize fetching required candles (all required symbols/timeframes with freshness targets), verify coverage evidence, and only then run optimization.

## 2026-03-24
- Pattern: A local `TradingSystem` clone can have **broken git objects** (`unable to read tree`, `bad object` on fetch, `fsck` broken links) while the working tree still has valid files—`git pull`/`rebase` then cannot integrate `origin/main`.
- Guardrail: After committing fixes locally, export a **unified diff against `origin/main`** (see `patches/kbo-47-against-origin-main.patch`; apply with `git apply` on a healthy clone checked out to GitHub `main`). If `.git` is corrupted (`unable to read tree`), re-clone or repair before `push`/`pull`; `git format-patch` from a broken repo can misrepresent history.
