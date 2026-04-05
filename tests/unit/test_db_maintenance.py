"""
Tests for DatabasePruner and connection-pool observability.

Validates:
  - Tiered candle retention (15m/30d, 1h/90d, 4h/365d, 1d/keep-all)
  - Decision trace pruning
  - Table stats logging
  - Pool status helper
"""
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

from src.storage.maintenance import (
    CANDLE_RETENTION_DAYS,
    SYSTEM_EVENT_RETENTION_DAYS,
    DatabasePruner,
)


# ---------------------------------------------------------------------------
# Retention policy sanity
# ---------------------------------------------------------------------------

class TestRetentionPolicies:
    """Verify the declared retention policy constants."""

    def test_15m_retention_is_30_days(self):
        assert CANDLE_RETENTION_DAYS["15m"] == 30

    def test_1h_retention_is_90_days(self):
        assert CANDLE_RETENTION_DAYS["1h"] == 90

    def test_4h_retention_is_365_days(self):
        assert CANDLE_RETENTION_DAYS["4h"] == 365

    def test_1d_candles_not_pruned(self):
        assert "1d" not in CANDLE_RETENTION_DAYS

    def test_system_event_retention_covers_counterfactual_decisions(self):
        assert SYSTEM_EVENT_RETENTION_DAYS["COUNTERFACTUAL_DECISION"] == 7

    def test_system_event_retention_keeps_signals_longer(self):
        assert SYSTEM_EVENT_RETENTION_DAYS["SIGNAL_GENERATED"] == 30


# ---------------------------------------------------------------------------
# DatabasePruner unit tests (mocked DB layer)
# ---------------------------------------------------------------------------

class TestDatabasePruner:
    """Test pruning logic with mocked DB sessions."""

    @pytest.fixture()
    def pruner(self):
        p = DatabasePruner()
        p._db = mock.MagicMock()
        return p

    def test_prune_old_candles_iterates_all_timeframes(self, pruner):
        """Each timeframe in CANDLE_RETENTION_DAYS gets a delete query."""
        session = mock.MagicMock()
        pruner.db.get_session.return_value.__enter__ = mock.MagicMock(return_value=session)
        pruner.db.get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

        # Each filter().delete() returns 0
        query = session.query.return_value.filter.return_value
        query.delete.return_value = 0

        result = pruner.prune_old_candles()

        assert result == 0
        # One session.query call per timeframe
        assert session.query.call_count == len(CANDLE_RETENTION_DAYS)

    def test_prune_old_candles_commits_when_rows_deleted(self, pruner):
        session = mock.MagicMock()
        pruner.db.get_session.return_value.__enter__ = mock.MagicMock(return_value=session)
        pruner.db.get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

        query = session.query.return_value.filter.return_value
        query.delete.return_value = 5  # 5 deleted per TF

        result = pruner.prune_old_candles()

        assert result == 5 * len(CANDLE_RETENTION_DAYS)
        session.commit.assert_called_once()

    def test_prune_old_traces_default_3_days(self, pruner):
        with mock.patch.object(pruner, "prune_old_system_events", return_value={"DECISION_TRACE": 10}) as mocked:
            result = pruner.prune_old_traces()

        assert result == 10
        mocked.assert_called_once_with({"DECISION_TRACE": 3})

    def test_prune_old_system_events_commits_per_event_type(self, pruner):
        session = mock.MagicMock()
        pruner.db.get_session.return_value.__enter__ = mock.MagicMock(return_value=session)
        pruner.db.get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

        query = session.query.return_value.filter.return_value
        query.delete.return_value = 10

        result = pruner.prune_old_system_events({"DECISION_TRACE": 3, "COUNTERFACTUAL_DECISION": 7})

        assert result == {"DECISION_TRACE": 10, "COUNTERFACTUAL_DECISION": 10}
        assert session.commit.call_count == 2

    def test_run_maintenance_returns_both_counts(self, pruner):
        """run_maintenance aggregates trace + candle counts."""
        with (
            mock.patch.object(
                pruner,
                "prune_old_system_events",
                return_value={"DECISION_TRACE": 3, "COUNTERFACTUAL_DECISION": 11},
            ),
            mock.patch.object(pruner, "prune_old_candles", return_value=7),
            mock.patch.object(pruner, "log_table_stats", return_value={}),
        ):
            result = pruner.run_maintenance()

        assert result == {
            "traces_deleted": 3,
            "system_events_deleted": {"DECISION_TRACE": 3, "COUNTERFACTUAL_DECISION": 11},
            "candles_deleted": 7,
        }


# ---------------------------------------------------------------------------
# Pool status helper
# ---------------------------------------------------------------------------

class TestPoolStatus:
    """Test the get_pool_status helper."""

    def test_returns_empty_when_no_db(self):
        from src.storage import db as db_module

        original = db_module._db_instance
        try:
            db_module._db_instance = None
            from src.storage.db import get_pool_status
            assert get_pool_status() == {}
        finally:
            db_module._db_instance = original

    def test_returns_pool_metrics_when_db_exists(self):
        from src.storage import db as db_module
        from src.storage.db import get_pool_status

        mock_pool = mock.MagicMock()
        mock_pool.size.return_value = 10
        mock_pool.checkedout.return_value = 2
        mock_pool.overflow.return_value = 0
        mock_pool.checkedin.return_value = 8

        mock_db = mock.MagicMock()
        mock_db.engine.pool = mock_pool

        original = db_module._db_instance
        try:
            db_module._db_instance = mock_db
            status = get_pool_status()
            assert status == {
                "pool_size": 10,
                "checked_out": 2,
                "overflow": 0,
                "checked_in": 8,
            }
        finally:
            db_module._db_instance = original
