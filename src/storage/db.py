"""
Database engine and session management.

PostgreSQL only - SQLite is no longer supported.
Includes connection-pool observability via SQLAlchemy pool events.
"""
import sqlalchemy
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.pool import Pool
from contextlib import contextmanager
from typing import Generator, Dict, Any
import os
import time

from src.exceptions import OperationalError, DataError
from src.monitoring.logger import get_logger

_pool_logger = get_logger("db.pool")

# Base class for ORM models
Base = declarative_base()


class Database:
    """Database engine and session manager."""

    def __init__(self, database_url: str):
        """
        Initialize database connection.

        Args:
            database_url: PostgreSQL connection string
        """
        if not database_url.startswith("postgresql"):
            raise ValueError(
                f"Only PostgreSQL is supported. Got: {database_url[:30]}... "
                "Set DATABASE_URL to a postgresql:// connection string."
            )

        self.database_url = database_url

        # Optimized connection pooling for PostgreSQL
        self.engine = create_engine(
            database_url,
            echo=False,
            pool_pre_ping=True,  # Verify connections before using
            pool_size=10,  # Base connections
            max_overflow=20,  # Additional connections under load
            pool_recycle=3600,  # Recycle connections after 1 hour
            pool_timeout=30,  # Wait up to 30s for connection
        )

        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

        # Register pool event listeners for observability
        _register_pool_events(self.engine.pool)

    def create_all(self):
        """Create all tables."""
        try:
            Base.metadata.create_all(bind=self.engine)
        except (OperationalError, OSError) as e:
            error_str = str(e).lower()
            # Log but don't fail for permission errors - these need manual fix
            import logging
            if "permission denied" in error_str or "insufficientprivilege" in error_str:
                logging.warning(
                    f"Table creation failed due to insufficient privileges: {e}\n"
                    "Grant CREATE privilege on schema 'public' to your database user.\n"
                    "The app will continue but database operations will fail until fixed."
                )
                # Don't re-raise - app can start but DB ops will fail
                return
            elif "already exists" in error_str or "duplicate" in error_str:
                # Tables already exist - this is fine
                logging.debug(f"Tables already exist (expected): {e}")
                return
            else:
                # Other errors - log and re-raise
                logging.warning(f"Table creation warning (may be expected): {e}")
                raise
        # Run lightweight column migrations for existing tables
        self._run_migrations()
    
    def _run_migrations(self):
        """Add columns that may be missing on existing deployments.
        
        Uses ADD COLUMN IF NOT EXISTS (Postgres >=9.6).  Wrapped in
        try/except per-statement so one failure doesn't block others.
        """
        import logging
        migrations = [
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS size NUMERIC(20,8)",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS maker_fills_count INTEGER",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS taker_fills_count INTEGER",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT 'live'",
        ]
        with self.engine.connect() as conn:
            for stmt in migrations:
                try:
                    conn.execute(sqlalchemy.text(stmt))
                    conn.commit()
                except (OperationalError, OSError) as e:
                    err = str(e).lower()
                    if "already exists" in err or "duplicate" in err:
                        pass  # Column already present
                    else:
                        logging.warning(f"Migration warning: {stmt!r} -> {e}")
                    try:
                        conn.rollback()
                    except (OperationalError, OSError):
                        pass

    def drop_all(self):
        """Drop all tables (use with caution!)."""
        Base.metadata.drop_all(bind=self.engine)

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context manager for database sessions.

        Yields:
            SQLAlchemy Session

        Example:
            with db.get_session() as session:
                session.add(obj)
                session.commit()
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


# Global database instance (initialized on first use)
_db_instance: Database | None = None


def get_db() -> Database:
    """
    Get or create the global database instance.
    
    Uses lazy validation with retry logic for cloud platform secret injection.
    """
    global _db_instance
    if _db_instance is None:
        # CRITICAL: Import all ORM models BEFORE creating database instance
        # This ensures Base.metadata contains all table definitions
        # Import is idempotent (safe to import multiple times)
        import src.storage.repository  # This imports all models (CandleModel, TradeModel, etc.)
        
        # Use lazy validation with retry logic for cloud platforms
        from src.utils.secret_manager import get_database_url
        from src.monitoring.logger import get_logger
        from urllib.parse import urlparse
        import os
        
        logger = get_logger(__name__)
        database_url = get_database_url()
        
        # CRITICAL: Log database connection details (without password) for debugging
        try:
            parsed = urlparse(database_url)
            db_host = parsed.hostname or "unknown"
            db_port = parsed.port or 5432
            db_name = parsed.path.lstrip('/') or "unknown"
            db_user = parsed.username or "unknown"
            
            # Informational: connection target (no password) for debugging deployments.
            logger.info(
                "DATABASE_CONNECTION_INIT",
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                has_password=bool(parsed.password)
            )
            
            # CRITICAL: Fail fast if test_db is detected in production
            if "test_db" in db_name.lower():
                env = os.getenv("ENVIRONMENT", "unknown")
                if env == "prod" or not os.getenv("ENVIRONMENT"):
                    logger.critical(
                        "CRITICAL: test_db detected in production!",
                        database=db_name,
                        environment=env,
                        database_url_preview=database_url[:50] + "..." if len(database_url) > 50 else database_url
                    )
                    raise ValueError(
                        f"CRITICAL: Database 'test_db' detected in production environment. "
                        f"Check DATABASE_URL environment variable. "
                        f"Current database: {db_name}, Environment: {env}"
                    )
        except (ValueError, TypeError, KeyError, OSError) as e:
            logger.warning("Failed to parse DATABASE_URL for logging", error=str(e))
        
        _db_instance = Database(database_url)
        _db_instance.create_all()  # Create all tables on first connection
    return _db_instance


def init_db(database_url: str) -> Database:
    """
    Initialize database with specific URL.

    Args:
        database_url: PostgreSQL connection string

    Returns:
        Database instance
    """
    global _db_instance
    _db_instance = Database(database_url)
    _db_instance.create_all()
    return _db_instance


# ---------------------------------------------------------------------------
# Connection-pool observability
# ---------------------------------------------------------------------------

def _register_pool_events(pool: Pool) -> None:
    """
    Attach SQLAlchemy pool event listeners for observability.

    Logs:
      - ``POOL_CHECKOUT``:   A connection was checked out.
      - ``POOL_CHECKIN``:    A connection was returned.
      - ``POOL_OVERFLOW``:   A new overflow connection was created.
      - ``POOL_TIMEOUT``:    A checkout waited longer than the pool timeout.
      - ``POOL_INVALIDATE``: A connection was invalidated (e.g. stale).
    """

    @event.listens_for(pool, "checkout")
    def _on_checkout(dbapi_connection, connection_record, connection_proxy):
        connection_record.info["checkout_time"] = time.monotonic()
        _pool_logger.debug(
            "POOL_CHECKOUT",
            pool_size=pool.size(),
            checked_out=pool.checkedout(),
            overflow=pool.overflow(),
        )

    @event.listens_for(pool, "checkin")
    def _on_checkin(dbapi_connection, connection_record):
        checkout_time = connection_record.info.pop("checkout_time", None)
        held_ms = (
            round((time.monotonic() - checkout_time) * 1000, 1)
            if checkout_time is not None
            else None
        )
        _pool_logger.debug(
            "POOL_CHECKIN",
            held_ms=held_ms,
            pool_size=pool.size(),
            checked_out=pool.checkedout(),
        )

    @event.listens_for(pool, "invalidate")
    def _on_invalidate(dbapi_connection, connection_record, exception):
        _pool_logger.warning(
            "POOL_INVALIDATE",
            error=str(exception) if exception else None,
        )


def get_pool_status() -> Dict[str, Any]:
    """
    Return a snapshot of connection-pool health metrics.

    Useful for health-check endpoints and periodic monitoring.
    Returns an empty dict if the database has not been initialised yet.
    """
    if _db_instance is None:
        return {}
    pool = _db_instance.engine.pool
    return {
        "pool_size": pool.size(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
        "checked_in": pool.checkedin(),
    }
