"""
Order Intelligence Platform — Phase 8
PostgreSQL Writer
=================
Idempotent UPSERT functions for all Gold layer PostgreSQL tables.

All writes use INSERT ... ON CONFLICT DO UPDATE to handle Spark batch
retries without creating duplicate rows. This is the correct pattern
for at-least-once Spark streaming with a relational sink.

Connection pooling via SQLAlchemy (pool_size=5, max_overflow=2).
"""

import os
from contextlib import contextmanager

from pyspark.sql import DataFrame
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

from utils.logger import configure_logging, get_logger

configure_logging()
log = get_logger("postgres_writer")

# ---------------------------------------------------------------------------
# Connection pool — created once, reused across foreachBatch calls
# ---------------------------------------------------------------------------

_engine = None


def _get_engine():
    global _engine
    if _engine is None:
        dsn = (
            f"postgresql+psycopg2://"
            f"{os.getenv('POSTGRES_USER', 'oip_user')}:"
            f"{os.getenv('POSTGRES_PASSWORD', 'oip_password_dev')}@"
            f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
            f"{os.getenv('POSTGRES_PORT', '5432')}/"
            f"{os.getenv('POSTGRES_DB', 'order_intelligence')}"
        )
        _engine = create_engine(
            dsn,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=2,
            pool_pre_ping=True,          # detect stale connections
            pool_recycle=3600,           # recycle every hour
        )
        log.info("postgres_pool_created",
                 host=os.getenv("POSTGRES_HOST", "localhost"),
                 db=os.getenv("POSTGRES_DB", "order_intelligence"))
    return _engine


@contextmanager
def _conn():
    engine = _get_engine()
    with engine.connect() as conn:
        yield conn


# ---------------------------------------------------------------------------
# UPSERT — gold.revenue_by_minute
# ---------------------------------------------------------------------------

UPSERT_REVENUE_SQL = text("""
    INSERT INTO gold.revenue_by_minute
        (window_start, window_end, region,
         order_count, gross_revenue, refund_amount, cancellation_amount,
         failed_count, avg_order_value, updated_at)
    VALUES
        (:window_start, :window_end, :region,
         :order_count, :gross_revenue, :refund_amount, :cancellation_amount,
         :failed_count, :avg_order_value, NOW())
    ON CONFLICT (window_start, region)
    DO UPDATE SET
        order_count          = EXCLUDED.order_count,
        gross_revenue        = EXCLUDED.gross_revenue,
        refund_amount        = EXCLUDED.refund_amount,
        cancellation_amount  = EXCLUDED.cancellation_amount,
        failed_count         = EXCLUDED.failed_count,
        avg_order_value      = EXCLUDED.avg_order_value,
        updated_at           = NOW()
""")


def upsert_revenue_by_minute(df: DataFrame) -> None:
    """Upsert all rows of the revenue_by_minute Gold DataFrame into PostgreSQL."""
    rows = df.collect()
    if not rows:
        return

    records = [
        {
            "window_start":        r["window_start"],
            "window_end":          r["window_end"],
            "region":              r["region"],
            "order_count":         int(r["order_count"]),
            "gross_revenue":       float(r["gross_revenue"]),
            "refund_amount":       float(r["refund_amount"]),
            "cancellation_amount": float(r["cancellation_amount"]),
            "failed_count":        int(r["failed_count"]),
            "avg_order_value":     float(r["avg_order_value"]),
        }
        for r in rows
    ]

    with _conn() as conn:
        conn.execute(UPSERT_REVENUE_SQL, records)
        conn.commit()

    log.info("upsert_revenue_by_minute", rows=len(records))


# ---------------------------------------------------------------------------
# UPSERT — gold.product_rankings
# ---------------------------------------------------------------------------

UPSERT_PRODUCTS_SQL = text("""
    INSERT INTO gold.product_rankings
        (window_start, window_end, product_id, product_name,
         units_sold, revenue, updated_at)
    VALUES
        (:window_start, :window_end, :product_id, :product_name,
         :units_sold, :revenue, NOW())
    ON CONFLICT (window_start, product_id)
    DO UPDATE SET
        product_name = EXCLUDED.product_name,
        units_sold   = EXCLUDED.units_sold,
        revenue      = EXCLUDED.revenue,
        updated_at   = NOW()
""")


def upsert_product_rankings(df: DataFrame) -> None:
    rows = df.collect()
    if not rows:
        return

    records = [
        {
            "window_start": r["window_start"],
            "window_end":   r["window_end"],
            "product_id":   r["product_id"],
            "product_name": r["product_name"],
            "units_sold":   int(r["units_sold"]),
            "revenue":      float(r["revenue"]),
        }
        for r in rows
    ]

    with _conn() as conn:
        conn.execute(UPSERT_PRODUCTS_SQL, records)
        conn.commit()

    log.info("upsert_product_rankings", rows=len(records))


# ---------------------------------------------------------------------------
# Health check — used by observability module
# ---------------------------------------------------------------------------

def check_connection() -> bool:
    try:
        with _conn() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        log.error("postgres_health_check_failed", error=str(exc))
        return False
