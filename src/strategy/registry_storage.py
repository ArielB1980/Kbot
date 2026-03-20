"""
File-based YAML storage backend for the strategy registry.

Designed for easy DB migration later: all persistence goes through
the ``RegistryStorage`` interface.
"""

from datetime import UTC, datetime
from pathlib import Path

import yaml

from src.strategy.registry_models import (
    PerformanceMetrics,
    StrategyLifecycle,
    StrategyRecord,
    StrategyType,
    TransitionRecord,
    ValidationCriteria,
)


def _dt_representer(dumper: yaml.Dumper, data: datetime) -> yaml.Node:
    return dumper.represent_str(data.isoformat())


yaml.add_representer(datetime, _dt_representer)


def _strategy_to_dict(rec: StrategyRecord) -> dict:
    """Serialize a StrategyRecord to a plain dict for YAML."""
    return {
        "name": rec.name,
        "strategy_type": rec.strategy_type.value,
        "universe": list(rec.universe),
        "parameters": dict(rec.parameters),
        "lifecycle": rec.lifecycle.value,
        "author": rec.author,
        "description": rec.description,
        "regime_applicability": list(rec.regime_applicability),
        "created_at": rec.created_at.isoformat(),
        "updated_at": rec.updated_at.isoformat(),
        "performance": {stage: _perf_to_dict(m) for stage, m in rec.performance.items()},
        "transitions": [_transition_to_dict(t) for t in rec.transitions],
        "custom_criteria": {
            stage: _criteria_to_dict(c) for stage, c in rec.custom_criteria.items()
        },
    }


def _perf_to_dict(m: PerformanceMetrics) -> dict:
    return {
        "sharpe_ratio": m.sharpe_ratio,
        "max_drawdown_pct": m.max_drawdown_pct,
        "win_rate": m.win_rate,
        "profit_factor": m.profit_factor,
        "total_trades": m.total_trades,
        "total_pnl": m.total_pnl,
        "regime_correlation": m.regime_correlation,
        "recovery_time_hours": m.recovery_time_hours,
        "evaluated_at": m.evaluated_at.isoformat(),
    }


def _criteria_to_dict(c: ValidationCriteria) -> dict:
    return {
        "min_sharpe": c.min_sharpe,
        "max_drawdown_pct": c.max_drawdown_pct,
        "min_win_rate": c.min_win_rate,
        "min_profit_factor": c.min_profit_factor,
        "min_trades": c.min_trades,
        "min_evaluation_days": c.min_evaluation_days,
    }


def _transition_to_dict(t: TransitionRecord) -> dict:
    d: dict = {
        "from_state": t.from_state.value,
        "to_state": t.to_state.value,
        "timestamp": t.timestamp.isoformat(),
        "reason": t.reason,
        "approved_by": t.approved_by,
    }
    if t.metrics_snapshot is not None:
        d["metrics_snapshot"] = _perf_to_dict(t.metrics_snapshot)
    return d


def _parse_dt(raw: str | datetime) -> datetime:
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return raw.replace(tzinfo=UTC)
        return raw
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _dict_to_perf(d: dict) -> PerformanceMetrics:
    return PerformanceMetrics(
        sharpe_ratio=float(d.get("sharpe_ratio", 0)),
        max_drawdown_pct=float(d.get("max_drawdown_pct", 0)),
        win_rate=float(d.get("win_rate", 0)),
        profit_factor=float(d.get("profit_factor", 0)),
        total_trades=int(d.get("total_trades", 0)),
        total_pnl=float(d.get("total_pnl", 0)),
        regime_correlation=float(d.get("regime_correlation", 0)),
        recovery_time_hours=float(d.get("recovery_time_hours", 0)),
        evaluated_at=_parse_dt(d.get("evaluated_at", datetime.now(UTC))),
    )


def _dict_to_criteria(d: dict) -> ValidationCriteria:
    return ValidationCriteria(
        **{k: d[k] for k in d if k in ValidationCriteria.__dataclass_fields__}
    )


def _dict_to_transition(d: dict) -> TransitionRecord:
    snap = d.get("metrics_snapshot")
    return TransitionRecord(
        from_state=StrategyLifecycle(d["from_state"]),
        to_state=StrategyLifecycle(d["to_state"]),
        timestamp=_parse_dt(d.get("timestamp", datetime.now(UTC))),
        reason=d.get("reason", ""),
        metrics_snapshot=_dict_to_perf(snap) if snap else None,
        approved_by=d.get("approved_by", ""),
    )


def _dict_to_strategy(d: dict) -> StrategyRecord:
    return StrategyRecord(
        name=d["name"],
        strategy_type=StrategyType(d["strategy_type"]),
        universe=d.get("universe", []),
        parameters=d.get("parameters", {}),
        lifecycle=StrategyLifecycle(d.get("lifecycle", "draft")),
        author=d.get("author", ""),
        description=d.get("description", ""),
        regime_applicability=d.get("regime_applicability", []),
        created_at=_parse_dt(d.get("created_at", datetime.now(UTC))),
        updated_at=_parse_dt(d.get("updated_at", datetime.now(UTC))),
        performance={k: _dict_to_perf(v) for k, v in d.get("performance", {}).items()},
        transitions=[_dict_to_transition(t) for t in d.get("transitions", [])],
        custom_criteria={k: _dict_to_criteria(v) for k, v in d.get("custom_criteria", {}).items()},
    )


class RegistryStorage:
    """YAML-file backed strategy store.

    Each strategy is stored in its own file: ``<base_dir>/<name>.yaml``.
    """

    def __init__(self, base_dir: str | Path) -> None:
        self._base = Path(base_dir)
        self._base.mkdir(parents=True, exist_ok=True)

    def _path_for(self, name: str) -> Path:
        safe = name.replace("/", "_").replace(" ", "_").lower()
        return self._base / f"{safe}.yaml"

    def save(self, record: StrategyRecord) -> None:
        """Persist a strategy record to disk."""
        path = self._path_for(record.name)
        data = _strategy_to_dict(record)
        path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))

    def load(self, name: str) -> StrategyRecord | None:
        """Load a single strategy by name. Returns None if not found."""
        path = self._path_for(name)
        if not path.exists():
            return None
        data = yaml.safe_load(path.read_text())
        if data is None:
            return None
        return _dict_to_strategy(data)

    def list_all(self) -> list[StrategyRecord]:
        """Load every strategy in the store."""
        results: list[StrategyRecord] = []
        for p in sorted(self._base.glob("*.yaml")):
            data = yaml.safe_load(p.read_text())
            if data is not None:
                results.append(_dict_to_strategy(data))
        return results

    def delete(self, name: str) -> bool:
        """Remove a strategy file. Returns True if deleted."""
        path = self._path_for(name)
        if path.exists():
            path.unlink()
            return True
        return False
