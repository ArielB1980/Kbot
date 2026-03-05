"""
Position State Persistence.

SQLite-based persistence for crash recovery with:
1. positions table - current position state
2. position_fills table - fill history
3. position_actions table - audit log

Recovery algorithm:
1. Load last known positions + actions
2. Query exchange open orders + open positions
3. Reconcile → mark inconsistent as ORPHANED
"""
import sqlite3
import json
import threading
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, List, Dict
from pathlib import Path

from src.execution.position_state_machine import (
    ManagedPosition,
    PositionRegistry,
    PositionState,
    ExitReason,
    FillRecord,
    set_position_registry
)
from src.domain.models import Side
from src.monitoring.logger import get_logger

logger = get_logger(__name__)


class PositionPersistence:
    """
    SQLite persistence for position state machine.
    """
    
    def __init__(self, db_path: str = "data/positions.db"):
        """Initialize persistence with database path."""
        self.db_path = db_path
        self._local = threading.local()
        
        # Ensure directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize schema
        self._init_schema()
    
    @property
    def _conn(self) -> sqlite3.Connection:
        """Get thread-local connection."""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn
    
    def _init_schema(self) -> None:
        """Initialize database schema."""
        with self._conn:
            self._conn.executescript("""
                -- Positions table
                CREATE TABLE IF NOT EXISTS positions (
                    position_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    state TEXT NOT NULL,
                    
                    -- Immutable entry parameters
                    initial_size TEXT NOT NULL,
                    initial_entry_price TEXT NOT NULL,
                    initial_stop_price TEXT NOT NULL,
                    initial_tp1_price TEXT,
                    initial_tp2_price TEXT,
                    initial_final_target TEXT,
                    
                    -- Current state
                    current_stop_price TEXT,
                    entry_acknowledged INTEGER DEFAULT 0,
                    tp1_filled INTEGER DEFAULT 0,
                    tp2_filled INTEGER DEFAULT 0,
                    break_even_triggered INTEGER DEFAULT 0,
                    trailing_active INTEGER DEFAULT 0,
                    
                    -- Exit tracking
                    exit_reason TEXT,
                    exit_time TEXT,
                    
                    -- Order tracking
                    entry_order_id TEXT,
                    stop_order_id TEXT,
                    pending_exit_order_id TEXT,
                    
                    -- Metadata
                    setup_type TEXT,
                    regime TEXT,
                    trade_type TEXT,
                    intent_confirmed INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    
                    -- Event idempotency
                    processed_event_hashes TEXT DEFAULT '[]'
                );
                
                CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);
                CREATE INDEX IF NOT EXISTS idx_positions_state ON positions(state);
                
                -- Position fills table
                CREATE TABLE IF NOT EXISTS position_fills (
                    fill_id TEXT PRIMARY KEY,
                    position_id TEXT NOT NULL,
                    order_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    qty TEXT NOT NULL,
                    price TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    is_entry INTEGER NOT NULL,
                    
                    FOREIGN KEY (position_id) REFERENCES positions(position_id)
                );
                
                CREATE INDEX IF NOT EXISTS idx_fills_position ON position_fills(position_id);
                
                -- Position actions audit log
                CREATE TABLE IF NOT EXISTS position_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    position_id TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    payload_json TEXT,
                    status TEXT DEFAULT 'executed',
                    timestamp TEXT NOT NULL,
                    
                    FOREIGN KEY (position_id) REFERENCES positions(position_id)
                );
                
                CREATE INDEX IF NOT EXISTS idx_actions_position ON position_actions(position_id);
                CREATE INDEX IF NOT EXISTS idx_actions_timestamp ON position_actions(timestamp);
                
                -- Pending reversals
                CREATE TABLE IF NOT EXISTS pending_reversals (
                    symbol TEXT PRIMARY KEY,
                    new_side TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                
                -- Action intents (Write-Ahead Log for crash recovery)
                -- Persisted BEFORE sending to exchange to prevent duplicate orders
                CREATE TABLE IF NOT EXISTS action_intents (
                    intent_id TEXT PRIMARY KEY,
                    position_id TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    size TEXT NOT NULL,
                    price TEXT,
                    created_at TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    exchange_order_id TEXT,
                    error TEXT
                );
                
                CREATE INDEX IF NOT EXISTS idx_intents_status ON action_intents(status);
                CREATE INDEX IF NOT EXISTS idx_intents_position ON action_intents(position_id);
            """)
            try:
                self._conn.execute(
                    "ALTER TABLE positions ADD COLUMN intent_confirmed INTEGER DEFAULT 0"
                )
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
            for col in ("entry_size_initial", "tp1_qty_target", "tp2_qty_target"):
                try:
                    self._conn.execute(
                        f"ALTER TABLE positions ADD COLUMN {col} TEXT"
                    )
                except sqlite3.OperationalError as e:
                    if "duplicate column" not in str(e).lower():
                        raise
            # Trade recording flag (v2.1)
            try:
                self._conn.execute(
                    "ALTER TABLE positions ADD COLUMN trade_recorded INTEGER DEFAULT 0"
                )
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
    
    # ========== POSITION CRUD ==========
    
    def save_position(self, position: ManagedPosition) -> None:
        """Save or update position."""
        with self._conn:
            self._conn.execute("""
                INSERT OR REPLACE INTO positions (
                    position_id, symbol, side, state,
                    initial_size, initial_entry_price, initial_stop_price,
                    initial_tp1_price, initial_tp2_price, initial_final_target,
                    current_stop_price, entry_acknowledged,
                    tp1_filled, tp2_filled, break_even_triggered, trailing_active,
                    entry_size_initial, tp1_qty_target, tp2_qty_target,
                    exit_reason, exit_time,
                    entry_order_id, stop_order_id, pending_exit_order_id,
                    setup_type, regime, trade_type, intent_confirmed,
                    created_at, updated_at,
                    processed_event_hashes,
                    trade_recorded
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                position.position_id,
                position.symbol,
                position.side.value,
                position.state.value,
                str(position.initial_size),
                str(position.initial_entry_price),
                str(position.initial_stop_price),
                str(position.initial_tp1_price) if position.initial_tp1_price else None,
                str(position.initial_tp2_price) if position.initial_tp2_price else None,
                str(position.initial_final_target) if position.initial_final_target else None,
                str(position.current_stop_price) if position.current_stop_price else None,
                1 if position.entry_acknowledged else 0,
                1 if position.tp1_filled else 0,
                1 if position.tp2_filled else 0,
                1 if position.break_even_triggered else 0,
                1 if position.trailing_active else 0,
                str(position.entry_size_initial) if position.entry_size_initial else None,
                str(position.tp1_qty_target) if position.tp1_qty_target else None,
                str(position.tp2_qty_target) if position.tp2_qty_target else None,
                position.exit_reason.value if position.exit_reason else None,
                position.exit_time.isoformat() if position.exit_time else None,
                position.entry_order_id,
                position.stop_order_id,
                position.pending_exit_order_id,
                position.setup_type,
                position.regime,
                position.trade_type,
                1 if position.intent_confirmed else 0,
                position.created_at.isoformat(),
                position.updated_at.isoformat(),
                json.dumps(list(position.processed_event_hashes)),
                1 if position.trade_recorded else 0,
            ))
            
            # Save fills
            for fill in position.entry_fills:
                self._save_fill(position.position_id, fill)
            for fill in position.exit_fills:
                self._save_fill(position.position_id, fill)
    
    def _save_fill(self, position_id: str, fill: FillRecord) -> bool:
        """Save a fill record with global fill_id idempotency."""
        existing = self._conn.execute(
            "SELECT position_id FROM position_fills WHERE fill_id = ?",
            (fill.fill_id,),
        ).fetchone()
        if existing is not None:
            logger.warning(
                "FILL_ID_COLLISION_IGNORED",
                fill_id=fill.fill_id,
                position_id=position_id,
                existing_position_id=existing["position_id"],
                reason="already_exists",
                is_entry=fill.is_entry,
                qty=str(fill.qty),
            )
            return False

        cursor = self._conn.execute(
            """
            INSERT OR IGNORE INTO position_fills (
                fill_id, position_id, order_id, side, qty, price, timestamp, is_entry
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fill.fill_id,
                position_id,
                fill.order_id,
                fill.side.value,
                str(fill.qty),
                str(fill.price),
                fill.timestamp.isoformat(),
                1 if fill.is_entry else 0,
            ),
        )
        if cursor.rowcount == 0:
            # Defensive fallback for race windows across concurrent writers.
            existing_after = self._conn.execute(
                "SELECT position_id FROM position_fills WHERE fill_id = ?",
                (fill.fill_id,),
            ).fetchone()
            logger.warning(
                "FILL_ID_COLLISION_IGNORED",
                fill_id=fill.fill_id,
                position_id=position_id,
                existing_position_id=existing_after["position_id"] if existing_after else None,
                reason="already_exists",
                is_entry=fill.is_entry,
                qty=str(fill.qty),
            )
            return False

        # Post-write invariant check: exit_qty must not exceed entry_qty
        if not fill.is_entry:
            try:
                cursor = self._conn.execute(
                    "SELECT SUM(CAST(qty AS REAL)) FROM position_fills WHERE position_id = ? AND is_entry = 1",
                    (position_id,),
                )
                entry_total = cursor.fetchone()[0] or 0.0
                cursor = self._conn.execute(
                    "SELECT SUM(CAST(qty AS REAL)) FROM position_fills WHERE position_id = ? AND is_entry = 0",
                    (position_id,),
                )
                exit_total = cursor.fetchone()[0] or 0.0
                if exit_total > entry_total * 1.001:  # small tolerance for float precision
                    logger.error(
                        "FILL_INVARIANT_VIOLATION: exit_qty exceeds entry_qty",
                        position_id=position_id,
                        entry_total=entry_total,
                        exit_total=exit_total,
                        fill_id=fill.fill_id,
                    )
            except Exception as check_err:
                logger.debug("Fill invariant check failed", error=str(check_err))
        return True
    
    def load_position(self, position_id: str) -> Optional[ManagedPosition]:
        """Load a position by ID."""
        cursor = self._conn.execute(
            "SELECT * FROM positions WHERE position_id = ?",
            (position_id,)
        )
        row = cursor.fetchone()
        if row is None:
            return None
        
        return self._row_to_position(dict(row))
    
    def load_active_positions(self) -> List[ManagedPosition]:
        """Load all non-terminal positions."""
        terminal_states = [
            PositionState.CLOSED.value,
            PositionState.CANCELLED.value,
            PositionState.ERROR.value,
            PositionState.ORPHANED.value
        ]
        
        cursor = self._conn.execute(
            f"SELECT * FROM positions WHERE state NOT IN ({','.join('?' for _ in terminal_states)})",
            terminal_states
        )
        
        positions = []
        for row in cursor:
            pos = self._row_to_position(dict(row))
            if pos:
                positions.append(pos)
        
        return positions
    
    def load_all_positions(self) -> List[ManagedPosition]:
        """Load all positions."""
        cursor = self._conn.execute("SELECT * FROM positions")
        
        positions = []
        for row in cursor:
            pos = self._row_to_position(dict(row))
            if pos:
                positions.append(pos)
        
        return positions
    
    def _row_to_position(self, row: Dict) -> Optional[ManagedPosition]:
        """Convert database row to ManagedPosition."""
        try:
            pos = ManagedPosition(
                symbol=row["symbol"],
                side=Side(row["side"]),
                position_id=row["position_id"],
                initial_size=Decimal(row["initial_size"]),
                initial_entry_price=Decimal(row["initial_entry_price"]),
                initial_stop_price=Decimal(row["initial_stop_price"]),
                initial_tp1_price=Decimal(row["initial_tp1_price"]) if row.get("initial_tp1_price") else None,
                initial_tp2_price=Decimal(row["initial_tp2_price"]) if row.get("initial_tp2_price") else None,
                initial_final_target=Decimal(row["initial_final_target"]) if row.get("initial_final_target") else None,
            )
            
            pos.state = PositionState(row["state"])
            pos.current_stop_price = Decimal(row["current_stop_price"]) if row.get("current_stop_price") else None
            pos.entry_acknowledged = bool(row.get("entry_acknowledged", 0))
            pos.tp1_filled = bool(row.get("tp1_filled", 0))
            pos.tp2_filled = bool(row.get("tp2_filled", 0))
            pos.break_even_triggered = bool(row.get("break_even_triggered", 0))
            pos.trailing_active = bool(row.get("trailing_active", 0))
            if row.get("entry_size_initial"):
                pos.entry_size_initial = Decimal(row["entry_size_initial"])
            if row.get("tp1_qty_target"):
                pos.tp1_qty_target = Decimal(row["tp1_qty_target"])
            if row.get("tp2_qty_target"):
                pos.tp2_qty_target = Decimal(row["tp2_qty_target"])
            if row.get("exit_reason"):
                try:
                    pos.exit_reason = ExitReason(row["exit_reason"])
                except ValueError:
                    logger.warning(
                        "Unknown exit_reason in DB, treating as RECONCILIATION",
                        position_id=row.get("position_id"),
                        raw_value=row["exit_reason"],
                    )
                    pos.exit_reason = ExitReason.RECONCILIATION
            else:
                pos.exit_reason = None
            pos.exit_time = datetime.fromisoformat(row["exit_time"]) if row.get("exit_time") else None
            pos.entry_order_id = row.get("entry_order_id")
            pos.stop_order_id = row.get("stop_order_id")
            pos.pending_exit_order_id = row.get("pending_exit_order_id")
            pos.setup_type = row.get("setup_type")
            pos.regime = row.get("regime")
            pos.trade_type = row.get("trade_type")
            pos.intent_confirmed = bool(row.get("intent_confirmed", 0))
            pos.trade_recorded = bool(row.get("trade_recorded", 0))
            pos.created_at = datetime.fromisoformat(row["created_at"])
            pos.updated_at = datetime.fromisoformat(row["updated_at"])
            
            # Load processed event hashes
            hashes = row.get("processed_event_hashes", "[]")
            pos.processed_event_hashes = set(json.loads(hashes))
            
            # Load fills
            self._load_fills(pos)
            pos.ensure_snapshot_targets()
            
            return pos
            
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Failed to load position {row.get('position_id')}: {e}")
            return None
    
    def _load_fills(self, position: ManagedPosition) -> None:
        """Load fills for a position."""
        cursor = self._conn.execute(
            "SELECT * FROM position_fills WHERE position_id = ? ORDER BY timestamp",
            (position.position_id,)
        )
        
        for row in cursor:
            fill = FillRecord(
                fill_id=row["fill_id"],
                order_id=row["order_id"],
                side=Side(row["side"]),
                qty=Decimal(row["qty"]),
                price=Decimal(row["price"]),
                timestamp=datetime.fromisoformat(row["timestamp"]),
                is_entry=bool(row["is_entry"])
            )
            
            if fill.is_entry:
                position.entry_fills.append(fill)
            else:
                position.exit_fills.append(fill)
    
    # ========== ACTION LOGGING ==========
    
    def log_action(
        self,
        position_id: str,
        action_type: str,
        payload: Optional[Dict] = None,
        status: str = "executed"
    ) -> None:
        """Log an action to the audit trail."""
        with self._conn:
            self._conn.execute("""
                INSERT INTO position_actions (position_id, action_type, payload_json, status, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (
                position_id,
                action_type,
                json.dumps(payload) if payload else None,
                status,
                datetime.now(timezone.utc).isoformat()
            ))
    
    def get_action_history(self, position_id: str, limit: int = 100) -> List[Dict]:
        """Get action history for a position."""
        cursor = self._conn.execute(
            "SELECT * FROM position_actions WHERE position_id = ? ORDER BY timestamp DESC LIMIT ?",
            (position_id, limit)
        )
        
        return [dict(row) for row in cursor]
    
    # ========== PENDING REVERSALS ==========
    
    def save_pending_reversal(self, symbol: str, new_side: Side) -> None:
        """Save pending reversal."""
        with self._conn:
            self._conn.execute("""
                INSERT OR REPLACE INTO pending_reversals (symbol, new_side, created_at)
                VALUES (?, ?, ?)
            """, (symbol, new_side.value, datetime.now(timezone.utc).isoformat()))
    
    def get_pending_reversals(self) -> Dict[str, Side]:
        """Get all pending reversals."""
        cursor = self._conn.execute("SELECT symbol, new_side FROM pending_reversals")
        return {row["symbol"]: Side(row["new_side"]) for row in cursor}
    
    def clear_pending_reversal(self, symbol: str) -> None:
        """Clear pending reversal."""
        with self._conn:
            self._conn.execute("DELETE FROM pending_reversals WHERE symbol = ?", (symbol,))
    
    # ========== REGISTRY PERSISTENCE ==========
    
    def save_registry(self, registry: PositionRegistry) -> None:
        """Save entire registry state, including recently closed positions.
        
        Persists both active positions (from _positions) and recently closed
        positions (from _closed_positions, last 100). This ensures closed
        position history survives restarts for trade recording and audit.
        """
        # Save all active positions
        for position in registry.get_all():
            self.save_position(position)
        
        # Save recently closed positions (last 100) so they survive restarts
        for closed_pos in registry._closed_positions[-100:]:
            self.save_position(closed_pos)
        
        # Save pending reversals
        data = registry.to_dict()
        for symbol, side_str in data.get("pending_reversals", {}).items():
            self.save_pending_reversal(symbol, Side(side_str))
    
    def load_registry(self) -> PositionRegistry:
        """
        Load registry from persistence.
        
        Recovery algorithm:
        1. Load all positions from DB
        2. Load pending reversals
        3. Reconstruct registry state
        """
        registry = PositionRegistry()
        
        # Load all positions
        for pos in self.load_all_positions():
            registry._positions[pos.symbol] = pos
        
        # Load pending reversals
        registry._pending_reversals = self.get_pending_reversals()
        
        # Move terminal and stale zero-qty positions out of active registry
        terminal_symbols: List[str] = []
        stale_zero_symbols: List[str] = []
        corrupted_symbols: List[str] = []
        qty_epsilon = Decimal("0.0001")
        for symbol, pos in registry._positions.items():
            if pos.is_terminal:
                registry._closed_positions.append(pos)
                terminal_symbols.append(symbol)
                continue
            self._repair_missing_entry_fill(pos)
            try:
                rem_qty = pos.remaining_qty
            except Exception as inv_err:
                logger.critical(
                    "CORRUPTED_POSITION: remaining_qty invariant violated — force-closing + alerting",
                    symbol=symbol,
                    position_id=pos.position_id,
                    entry_qty=str(pos.filled_entry_qty),
                    exit_qty=str(pos.filled_exit_qty),
                    error=str(inv_err),
                )
                pos.state = PositionState.CLOSED
                if pos.exit_reason is None:
                    pos.exit_reason = ExitReason.RECONCILIATION
                registry._closed_positions.append(pos)
                corrupted_symbols.append(symbol)
                try:
                    from src.monitoring.alerting import send_alert
                    import asyncio
                    asyncio.get_event_loop().create_task(send_alert(
                        "CORRUPTED_POSITION",
                        f"Position {symbol} ({pos.position_id}) had negative remaining_qty "
                        f"(entry={pos.filled_entry_qty}, exit={pos.filled_exit_qty}). "
                        f"Force-closed during startup. Investigate data inconsistency.",
                        urgent=True,
                    ))
                except Exception:
                    pass
                continue
            if rem_qty <= qty_epsilon:
                old_state = pos.state
                pos.state = PositionState.CLOSED
                if pos.exit_reason is None:
                    pos.exit_reason = ExitReason.RECONCILIATION
                registry._closed_positions.append(pos)
                stale_zero_symbols.append(symbol)
                logger.warning(
                    "Recovered stale zero-qty position from persistence",
                    symbol=symbol,
                    previous_state=old_state.value,
                )
        
        # Remove non-active positions from _positions
        for symbol in terminal_symbols + stale_zero_symbols + corrupted_symbols:
            del registry._positions[symbol]
        
        logger.info(
            "Registry loaded from persistence",
            total_positions=len(registry._positions) + len(terminal_symbols) + len(stale_zero_symbols) + len(corrupted_symbols),
            active_positions=len(registry._positions),
            terminal_moved=len(terminal_symbols),
            stale_zero_moved=len(stale_zero_symbols),
            corrupted_closed=len(corrupted_symbols),
            pending_reversals=len(registry._pending_reversals),
        )
        
        return registry

    def _repair_missing_entry_fill(self, pos: ManagedPosition) -> None:
        """Best-effort startup repair for legacy rows missing entry fills.

        If exits exist but entries are zero, synthesize a single entry fill so
        remaining_qty invariant can be evaluated and reconciled safely.
        """
        if pos.filled_entry_qty > 0 or pos.filled_exit_qty <= 0:
            return
        repaired_entry_qty = max(pos.initial_size, pos.filled_exit_qty)
        now = datetime.now(timezone.utc)
        repair_fill = FillRecord(
            fill_id=f"startup-repair-entry-{pos.position_id}-{int(now.timestamp() * 1000)}",
            order_id="startup-repair",
            side=pos.side,
            qty=repaired_entry_qty,
            price=pos.initial_entry_price,
            timestamp=now,
            is_entry=True,
        )
        pos.entry_fills.append(repair_fill)
        pos.entry_acknowledged = True
        pos.ensure_snapshot_targets()
        logger.warning(
            "Repaired missing entry fills for position",
            symbol=pos.symbol,
            position_id=pos.position_id,
            repaired_entry_qty=str(repaired_entry_qty),
            existing_exit_qty=str(pos.filled_exit_qty),
        )
    
    # ========== CLEANUP ==========
    
    def cleanup_old_positions(self, days: int = 30) -> int:
        """Remove positions older than N days."""
        cutoff = datetime.now(timezone.utc)
        # Calculate cutoff
        from datetime import timedelta
        cutoff = cutoff - timedelta(days=days)
        
        terminal_states = [
            PositionState.CLOSED.value,
            PositionState.CANCELLED.value,
            PositionState.ERROR.value,
            PositionState.ORPHANED.value
        ]
        
        with self._conn:
            # Delete old fills first (foreign key)
            self._conn.execute("""
                DELETE FROM position_fills WHERE position_id IN (
                    SELECT position_id FROM positions 
                    WHERE state IN (?, ?, ?, ?) AND updated_at < ?
                )
            """, (*terminal_states, cutoff.isoformat()))
            
            # Delete old actions
            self._conn.execute("""
                DELETE FROM position_actions WHERE position_id IN (
                    SELECT position_id FROM positions 
                    WHERE state IN (?, ?, ?, ?) AND updated_at < ?
                )
            """, (*terminal_states, cutoff.isoformat()))
            
            # Delete old positions
            cursor = self._conn.execute("""
                DELETE FROM positions 
                WHERE state IN (?, ?, ?, ?) AND updated_at < ?
            """, (*terminal_states, cutoff.isoformat()))
            
            return cursor.rowcount


# ========== RECOVERY FUNCTIONS ==========

def recover_from_persistence(
    db_path: str = "data/positions.db",
    exchange_positions: Optional[Dict[str, Dict]] = None,
    exchange_orders: Optional[List[Dict]] = None
) -> PositionRegistry:
    """
    Full crash recovery procedure.
    
    1. Load registry from DB
    2. Reconcile with exchange (if data provided)
    3. Mark inconsistencies as ORPHANED
    4. Set as singleton registry
    """
    persistence = PositionPersistence(db_path)
    registry = persistence.load_registry()
    
    if exchange_positions is not None:
        issues = registry.reconcile_with_exchange(
            exchange_positions,
            exchange_orders or []
        )
        
        if issues:
            logger.warning(
                "Recovery found reconciliation issues",
                count=len(issues),
                issues=issues
            )
            
            # Save updated state
            for pos in registry.get_all():
                persistence.save_position(pos)
    
    # Set as singleton
    set_position_registry(registry)
    
    return registry


def persist_registry_state(
    registry: PositionRegistry,
    db_path: str = "data/positions.db"
) -> None:
    """Persist current registry state."""
    persistence = PositionPersistence(db_path)
    persistence.save_registry(registry)
