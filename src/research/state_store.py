"""JSON-backed run state store for sandbox autoresearch."""

from __future__ import annotations

import json
import threading
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.research.models import DecisionPrompt


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ResearchStateStore:
    """Persists run state and command signals for Telegram interactions."""

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.write_state(
                {
                    "run_id": None,
                    "phase": "idle",
                    "iteration": 0,
                    "total_iterations": 0,
                    "best_candidate_id": None,
                    "leaderboard": [],
                    "control": {"paused": False, "stop_requested": False},
                    "pending_prompt": None,
                    "promotion_queue": [],
                    "last_error": None,
                    "updated_at": _utc_now().isoformat(),
                }
            )

    def read_state(self) -> dict[str, Any]:
        """Read the full store payload."""
        with self._lock:
            return json.loads(self.path.read_text(encoding="utf-8"))

    def write_state(self, state: dict[str, Any]) -> None:
        """Write state atomically."""
        with self._lock:
            state["updated_at"] = _utc_now().isoformat()
            temp = self.path.with_suffix(f"{self.path.suffix}.tmp")
            temp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
            temp.replace(self.path)

    def update(self, **patch: Any) -> dict[str, Any]:
        """Merge top-level keys and persist."""
        state = self.read_state()
        state.update(patch)
        self.write_state(state)
        return state

    def set_control(self, *, paused: bool | None = None, stop_requested: bool | None = None) -> dict[str, Any]:
        """Update control flags for pause/resume/stop operations."""
        state = self.read_state()
        control = dict(state.get("control") or {})
        if paused is not None:
            control["paused"] = paused
        if stop_requested is not None:
            control["stop_requested"] = stop_requested
        state["control"] = control
        self.write_state(state)
        return state

    def create_prompt(self, *, prompt_type: str, message: str, ttl_seconds: int = 120) -> DecisionPrompt:
        """Create a pending decision prompt token."""
        now = _utc_now()
        token = f"d{int(now.timestamp())}"
        prompt = DecisionPrompt(
            token=token,
            prompt_type=prompt_type,
            message=message,
            created_at=now.isoformat(),
            expires_at=(now + timedelta(seconds=ttl_seconds)).isoformat(),
        )
        state = self.read_state()
        state["pending_prompt"] = asdict(prompt)
        self.write_state(state)
        return prompt

    def resolve_prompt(self, token: str, resolution: str) -> bool:
        """Resolve a pending prompt by token."""
        state = self.read_state()
        prompt = state.get("pending_prompt")
        if not prompt or prompt.get("token") != token:
            return False
        prompt["resolved"] = True
        prompt["resolution"] = resolution
        state["pending_prompt"] = prompt
        self.write_state(state)
        return True

    def queue_promotion(self, candidate_id: str) -> bool:
        """Queue a candidate for human review promotion."""
        state = self.read_state()
        queue = list(state.get("promotion_queue") or [])
        if candidate_id in queue:
            return False
        queue.append(candidate_id)
        state["promotion_queue"] = queue
        self.write_state(state)
        return True

    def mark_replay_gate_passed(self, candidate_id: str) -> bool:
        """Mark a leaderboard candidate as replay-gate passed."""
        state = self.read_state()
        board = list(state.get("leaderboard") or [])
        updated = False
        for row in board:
            if row.get("candidate_id") != candidate_id:
                continue
            metadata = dict(row.get("metadata") or {})
            metadata["replay_gate_passed"] = True
            metadata["promotion_ready"] = True
            row["metadata"] = metadata
            updated = True
            break
        if not updated:
            return False
        state["leaderboard"] = board
        self.write_state(state)
        return True

