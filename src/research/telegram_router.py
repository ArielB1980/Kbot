"""Telegram command router for sandbox autoresearch control plane."""

from __future__ import annotations

from datetime import datetime, timezone

from src.research.state_store import ResearchStateStore


class ResearchTelegramRouter:
    """Handles `/research_*` and decision commands for sandbox runs."""

    def __init__(self, store: ResearchStateStore):
        self.store = store

    async def handle_command(self, text: str) -> str | None:
        """Return message string for supported command, else None."""
        cmd = (text or "").strip()
        if not cmd:
            return None

        if cmd.startswith("/research_status"):
            return self._research_status()
        if cmd.startswith("/research_top"):
            return self._research_top()
        if cmd.startswith("/research_diff"):
            return self._research_diff(cmd)
        if cmd == "/research_pause":
            self.store.set_control(paused=True)
            return "⏸️ Research paused."
        if cmd == "/research_resume":
            self.store.set_control(paused=False)
            return "▶️ Research resumed."
        if cmd == "/research_stop":
            self.store.set_control(stop_requested=True, paused=False)
            return "🛑 Stop requested. Loop will exit after current candidate."
        if cmd.startswith("/research_promote"):
            return self._research_promote(cmd)
        if cmd.startswith("/research_mark_replay_pass"):
            return self._research_mark_replay_pass(cmd)
        if cmd.startswith("/approve "):
            token = cmd.split(" ", 1)[1].strip()
            return "✅ Approved." if self.store.resolve_prompt(token, "approve") else "❌ Invalid token."
        if cmd.startswith("/reject "):
            token = cmd.split(" ", 1)[1].strip()
            return "🧯 Rejected." if self.store.resolve_prompt(token, "reject") else "❌ Invalid token."
        return None

    def _research_status(self) -> str:
        state = self.store.read_state()
        phase = state.get("phase", "idle")
        cur = int(state.get("iteration", 0))
        total = int(state.get("total_iterations", 0))
        best = state.get("best_candidate_id") or "-"
        err = state.get("last_error") or "-"
        updated = state.get("updated_at") or datetime.now(timezone.utc).isoformat()
        return (
            "🔬 <b>Research Status</b>\n"
            f"Phase: <b>{phase}</b>\n"
            f"Progress: {cur}/{total}\n"
            f"Best: <code>{best}</code>\n"
            f"Last Error: <code>{err}</code>\n"
            f"Updated: {updated}"
        )

    def _research_top(self) -> str:
        state = self.store.read_state()
        board = sorted(
            list(state.get("leaderboard") or []),
            key=lambda x: float(x.get("score", float("-inf"))),
            reverse=True,
        )[:3]
        if not board:
            return "📋 No evaluated candidates yet."
        lines = ["🏆 <b>Top Candidates</b>"]
        for row in board:
            metrics = row.get("metrics") or {}
            lines.append(
                (
                    f"\n• <code>{row.get('candidate_id')}</code> score={row.get('score', 0):.2f}\n"
                    f"  return={metrics.get('net_return_pct', 0):+.2f}% "
                    f"dd={metrics.get('max_drawdown_pct', 0):.2f}% "
                    f"sharpe={metrics.get('sharpe', 0):.2f} "
                    f"win={metrics.get('win_rate_pct', 0):.1f}% "
                    f"trades={int(metrics.get('trade_count', 0))}"
                )
            )
        return "\n".join(lines)

    def _research_diff(self, cmd: str) -> str:
        parts = cmd.split(maxsplit=1)
        if len(parts) < 2:
            return "Usage: /research_diff <candidate_id>"
        candidate_id = parts[1].strip()
        state = self.store.read_state()
        board = list(state.get("leaderboard") or [])
        baseline = next((x for x in board if x.get("candidate_id") == "baseline"), None)
        target = next((x for x in board if x.get("candidate_id") == candidate_id), None)
        if not baseline or not target:
            return f"Candidate not found: {candidate_id}"
        b_metrics = baseline.get("metrics") or {}
        t_metrics = target.get("metrics") or {}
        delta_return = float(t_metrics.get("net_return_pct", 0.0)) - float(b_metrics.get("net_return_pct", 0.0))
        delta_dd = float(t_metrics.get("max_drawdown_pct", 0.0)) - float(b_metrics.get("max_drawdown_pct", 0.0))
        delta_sharpe = float(t_metrics.get("sharpe", 0.0)) - float(b_metrics.get("sharpe", 0.0))
        return (
            f"🧪 <b>Diff {candidate_id} vs baseline</b>\n"
            f"Δ return: {delta_return:+.2f}%\n"
            f"Δ max_dd: {delta_dd:+.2f}%\n"
            f"Δ sharpe: {delta_sharpe:+.2f}\n"
            f"Param changes: <code>{target.get('params', {})}</code>"
        )

    def _research_promote(self, cmd: str) -> str:
        parts = cmd.split(maxsplit=1)
        if len(parts) < 2:
            return "Usage: /research_promote <candidate_id>"
        candidate_id = parts[1].strip()
        state = self.store.read_state()
        board = list(state.get("leaderboard") or [])
        target = next((x for x in board if x.get("candidate_id") == candidate_id), None)
        if not target:
            return f"Candidate not found: {candidate_id}"
        metadata = dict(target.get("metadata") or {})
        if not bool(metadata.get("replay_gate_passed", False)):
            return (
                f"⛔ Promotion blocked for {candidate_id}: replay gate not marked as passed.\n"
                f"Run replay verification, then use /research_mark_replay_pass {candidate_id}."
            )
        if self.store.queue_promotion(candidate_id):
            return f"📌 Queued {candidate_id} for human review promotion."
        return f"Candidate {candidate_id} is already queued."

    def _research_mark_replay_pass(self, cmd: str) -> str:
        parts = cmd.split(maxsplit=1)
        if len(parts) < 2:
            return "Usage: /research_mark_replay_pass <candidate_id>"
        candidate_id = parts[1].strip()
        if self.store.mark_replay_gate_passed(candidate_id):
            return f"✅ Replay gate marked as passed for {candidate_id}. Promotion is now allowed."
        return f"Candidate not found: {candidate_id}"

