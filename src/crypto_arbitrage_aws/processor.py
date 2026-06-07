import hashlib
import json
import os
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

from .paths import PROJECT_ROOT

# ---------------------------------------------------------------------------
# Configuration via environment variables
# Local defaults use SQLite and a local folder instead of S3.
# Paths are anchored to this file's directory so they resolve correctly
# regardless of the working directory the script is launched from.
# ---------------------------------------------------------------------------
ARBITRAGE_THRESHOLD_PCT = float(os.environ.get("ARBITRAGE_THRESHOLD_PCT", "0.3"))
MAX_PRICE_AGE_SECONDS = int(os.environ.get("MAX_PRICE_AGE_SECONDS", "120"))

DB_TYPE      = os.environ.get("DB_TYPE", "sqlite")
DB_PATH      = os.environ.get("DB_PATH", str(PROJECT_ROOT / "arbitrage.db"))
DB_DSN       = os.environ.get("DB_DSN", "")

S3_BUCKET    = os.environ.get("S3_BUCKET", "")
RAW_DATA_DIR = os.environ.get("RAW_DATA_DIR", str(PROJECT_ROOT / "data" / "raw_ticks"))


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_connection():
    if DB_TYPE == "postgres":
        import psycopg2
        return psycopg2.connect(DB_DSN)
    return sqlite3.connect(DB_PATH)


def _is_postgres(conn) -> bool:
    return not isinstance(conn, sqlite3.Connection)


def _execute(conn, sql: str, params: tuple = ()):
    if _is_postgres(conn):
        cursor = conn.cursor()
        cursor.execute(sql, params)
        return cursor
    return conn.execute(sql, params) if params else conn.execute(sql)


def _executemany(conn, sql: str, rows: list[tuple]):
    if _is_postgres(conn):
        cursor = conn.cursor()
        cursor.executemany(sql, rows)
        return cursor
    return conn.executemany(sql, rows)


def init_db(conn) -> None:
    """Creates and migrates processor-owned tables."""
    if _is_postgres(conn):
        opportunity_sql = """
            CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
                id            SERIAL PRIMARY KEY,
                opportunity_key VARCHAR(64) UNIQUE,
                detected_at   TIMESTAMPTZ    NOT NULL,
                coin          VARCHAR(20)    NOT NULL,
                exchange_low  VARCHAR(20)    NOT NULL,
                exchange_high VARCHAR(20)    NOT NULL,
                price_low     NUMERIC(20, 8) NOT NULL,
                price_high    NUMERIC(20, 8) NOT NULL,
                spread_pct    NUMERIC(8, 4)  NOT NULL,
                source_mode   VARCHAR(10)    NOT NULL
            );
        """
        _execute(conn, opportunity_sql)
        _execute(
            conn,
            "ALTER TABLE arbitrage_opportunities "
            "ADD COLUMN IF NOT EXISTS opportunity_key VARCHAR(64)"
        )
    else:
        opportunity_sql = """
            CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                opportunity_key TEXT UNIQUE,
                detected_at   TEXT NOT NULL,
                coin          TEXT NOT NULL,
                exchange_low  TEXT NOT NULL,
                exchange_high TEXT NOT NULL,
                price_low     REAL NOT NULL,
                price_high    REAL NOT NULL,
                spread_pct    REAL NOT NULL,
                source_mode   TEXT NOT NULL
            );
        """
        _execute(conn, opportunity_sql)
        columns = {
            row[1]
            for row in _execute(conn, "PRAGMA table_info(arbitrage_opportunities)")
        }
        if "opportunity_key" not in columns:
            _execute(
                conn,
                "ALTER TABLE arbitrage_opportunities ADD COLUMN opportunity_key TEXT"
            )

    _execute(
        conn,
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_opportunities_key "
        "ON arbitrage_opportunities (opportunity_key)"
    )
    observed_at_type = "TIMESTAMPTZ" if _is_postgres(conn) else "TEXT"
    _execute(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS latest_prices (
            coin        VARCHAR(20) NOT NULL,
            exchange    VARCHAR(20) NOT NULL,
            observed_at {observed_at_type} NOT NULL,
            price_usd   NUMERIC(20, 8) NOT NULL,
            source_mode VARCHAR(10) NOT NULL,
            PRIMARY KEY (coin, exchange)
        )
        """
    )
    conn.commit()


def save_opportunities(opportunities: list[dict], conn) -> None:
    if not opportunities:
        return
    # SQLite uses "?" placeholders; PostgreSQL uses "%s"
    ph = "%s" if _is_postgres(conn) else "?"
    sql = f"""
        INSERT INTO arbitrage_opportunities
            (opportunity_key, detected_at, coin, exchange_low, exchange_high,
             price_low, price_high, spread_pct, source_mode)
        VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
        ON CONFLICT (opportunity_key) DO NOTHING
    """
    rows = [
        (
            o.get("opportunity_key", opportunity_key(o)),
            o["detected_at"], o["coin"],
            o["exchange_low"], o["exchange_high"],
            o["price_low"], o["price_high"],
            o["spread_pct"], o["source_mode"],
        )
        for o in opportunities
    ]
    _executemany(conn, sql, rows)
    conn.commit()


def opportunity_key(opportunity: dict) -> str:
    identity = {
        "coin": opportunity["coin"],
        "exchange_low": opportunity["exchange_low"],
        "exchange_high": opportunity["exchange_high"],
        "price_low": opportunity["price_low"],
        "price_high": opportunity["price_high"],
        "low_observed_at": opportunity.get(
            "low_observed_at",
            opportunity["detected_at"],
        ),
        "high_observed_at": opportunity.get(
            "high_observed_at",
            opportunity["detected_at"],
        ),
    }
    return hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _utc_timestamp(value: str) -> str:
    observed_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return observed_at.astimezone(timezone.utc).isoformat()


def upsert_latest_prices(ticks: list[dict], conn) -> None:
    if not ticks:
        return

    ph = "%s" if _is_postgres(conn) else "?"
    sql = f"""
        INSERT INTO latest_prices
            (coin, exchange, observed_at, price_usd, source_mode)
        VALUES ({ph}, {ph}, {ph}, {ph}, {ph})
        ON CONFLICT (coin, exchange) DO UPDATE SET
            observed_at = excluded.observed_at,
            price_usd = excluded.price_usd,
            source_mode = excluded.source_mode
        WHERE excluded.observed_at > latest_prices.observed_at
    """
    rows = [
        (
            tick["coin"],
            tick["exchange"],
            _utc_timestamp(tick["timestamp"]),
            tick["price_usd"],
            tick["source_mode"],
        )
        for tick in ticks
    ]
    _executemany(conn, sql, rows)


def lock_coins(coins: set[str], conn) -> None:
    """Serializes concurrent PostgreSQL snapshots for the same coins."""
    if not _is_postgres(conn):
        return
    for coin in sorted(coins):
        _execute(conn, "SELECT pg_advisory_xact_lock(hashtext(%s))", (coin,))


def load_price_snapshot(
    coins: set[str],
    conn,
    max_age_seconds: int = MAX_PRICE_AGE_SECONDS,
) -> dict[str, dict[str, dict]]:
    if not coins:
        return {}

    ph = "%s" if _is_postgres(conn) else "?"
    coin_placeholders = ", ".join(ph for _ in coins)
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=max_age_seconds)).isoformat()
    rows = _execute(
        conn,
        f"""
        SELECT coin, exchange, observed_at, price_usd, source_mode
        FROM latest_prices
        WHERE coin IN ({coin_placeholders}) AND observed_at >= {ph}
        """,
        (*sorted(coins), cutoff),
    ).fetchall()

    snapshot: dict[str, dict[str, dict]] = defaultdict(dict)
    for coin, exchange, observed_at, price_usd, source_mode in rows:
        timestamp = (
            observed_at.isoformat()
            if isinstance(observed_at, datetime)
            else observed_at
        )
        snapshot[coin][exchange] = {
            "timestamp": timestamp,
            "price_usd": float(price_usd),
            "source_mode": source_mode,
        }
    return dict(snapshot)


def detect_snapshot_arbitrage(
    snapshot: dict[str, dict[str, dict]],
    threshold_pct: float = ARBITRAGE_THRESHOLD_PCT,
) -> list[dict]:
    opportunities = []

    for coin, exchange_ticks in snapshot.items():
        if len(exchange_ticks) < 2:
            continue

        low_ex = min(exchange_ticks, key=lambda exchange: exchange_ticks[exchange]["price_usd"])
        high_ex = max(exchange_ticks, key=lambda exchange: exchange_ticks[exchange]["price_usd"])
        low_tick = exchange_ticks[low_ex]
        high_tick = exchange_ticks[high_ex]
        spread_pct = (
            (high_tick["price_usd"] - low_tick["price_usd"])
            / low_tick["price_usd"]
            * 100
        )
        if spread_pct < threshold_pct:
            continue

        source_modes = {tick["source_mode"] for tick in exchange_ticks.values()}
        opportunity = {
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "coin": coin,
            "exchange_low": low_ex,
            "exchange_high": high_ex,
            "price_low": low_tick["price_usd"],
            "price_high": high_tick["price_usd"],
            "spread_pct": round(spread_pct, 4),
            "source_mode": (
                "mixed" if len(source_modes) > 1 else next(iter(source_modes))
            ),
            "low_observed_at": low_tick["timestamp"],
            "high_observed_at": high_tick["timestamp"],
        }
        opportunity["opportunity_key"] = opportunity_key(opportunity)
        opportunities.append(opportunity)

    return opportunities


def process_persistent_tick_batch(
    ticks: list[dict],
    conn,
    max_age_seconds: int = MAX_PRICE_AGE_SECONDS,
) -> list[dict]:
    if not ticks:
        return []

    init_db(conn)
    coins = {tick["coin"] for tick in ticks}
    lock_coins(coins, conn)
    upsert_latest_prices(ticks, conn)
    snapshot = load_price_snapshot(
        coins,
        conn,
        max_age_seconds=max_age_seconds,
    )
    opportunities = detect_snapshot_arbitrage(snapshot)
    save_opportunities(opportunities, conn)
    conn.commit()
    return opportunities


# ---------------------------------------------------------------------------
# Storage: raw ticks → S3 (AWS) or local folder (local dev)
# ---------------------------------------------------------------------------

def save_raw_ticks(ticks: list[dict]) -> None:
    now = (
        datetime.fromisoformat(ticks[0]["timestamp"].replace("Z", "+00:00"))
        if ticks
        else datetime.now(timezone.utc)
    ).astimezone(timezone.utc)
    batch_id = hashlib.sha256(
        json.dumps(ticks, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    if S3_BUCKET:
        import boto3
        key = save_raw_ticks_to_s3(ticks, S3_BUCKET, boto3.client("s3"))
        print(f"  Raw ticks saved to s3://{S3_BUCKET}/{key}")
    else:
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        path = os.path.join(
            RAW_DATA_DIR,
            f"{now.strftime('%Y%m%d_%H%M%S_%f')}_{batch_id}.json",
        )
        with open(path, "w") as f:
            json.dump(ticks, f, indent=2)
        print(f"  Raw ticks saved to {path}")


def save_raw_ticks_to_s3(ticks: list[dict], bucket: str, s3_client) -> str:
    observed_at = (
        datetime.fromisoformat(ticks[0]["timestamp"].replace("Z", "+00:00"))
        if ticks
        else datetime.now(timezone.utc)
    ).astimezone(timezone.utc)
    batch_id = hashlib.sha256(
        json.dumps(ticks, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    key = f"raw_ticks/{observed_at.strftime('%Y/%m/%d')}/{batch_id}.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(ticks),
        ContentType="application/json",
    )
    return key


# ---------------------------------------------------------------------------
# Core arbitrage logic
# ---------------------------------------------------------------------------

def detect_arbitrage(
    prices_by_coin: dict[str, dict[str, float]],
    source_mode: str,
    threshold_pct: float = ARBITRAGE_THRESHOLD_PCT,
) -> list[dict]:
    """
    Compares prices across exchanges for each coin.
    Returns opportunities where the spread exceeds threshold_pct.
    """
    opportunities = []
    now = datetime.now(timezone.utc).isoformat()

    for coin, exchange_prices in prices_by_coin.items():
        valid = {ex: p for ex, p in exchange_prices.items() if p is not None}
        if len(valid) < 2:
            continue

        low_ex  = min(valid, key=valid.__getitem__)
        high_ex = max(valid, key=valid.__getitem__)
        price_low  = valid[low_ex]
        price_high = valid[high_ex]

        spread_pct = (price_high - price_low) / price_low * 100

        if spread_pct >= threshold_pct:
            opportunities.append({
                "detected_at":  now,
                "coin":         coin,
                "exchange_low":  low_ex,
                "exchange_high": high_ex,
                "price_low":    price_low,
                "price_high":   price_high,
                "spread_pct":   round(spread_pct, 4),
                "source_mode":  source_mode,
            })

    return opportunities


def process_tick_batch(ticks: list[dict]) -> list[dict]:
    """
    Groups ticks by coin, keeps the latest price per exchange,
    then runs arbitrage detection over the snapshot.
    """
    if not ticks:
        return []

    latest_ticks: dict[str, dict[str, dict]] = defaultdict(dict)
    source_modes: set[str] = set()

    for tick in ticks:
        previous = latest_ticks[tick["coin"]].get(tick["exchange"])
        if previous is None or tick.get("timestamp", "") >= previous.get("timestamp", ""):
            latest_ticks[tick["coin"]][tick["exchange"]] = tick
        source_modes.add(tick.get("source_mode", "rest"))

    latest: dict[str, dict[str, Optional[float]]] = {
        coin: {
            exchange: tick["price_usd"]
            for exchange, tick in exchange_ticks.items()
        }
        for coin, exchange_ticks in latest_ticks.items()
    }
    source_mode = "mixed" if len(source_modes) > 1 else next(iter(source_modes))
    return detect_arbitrage(latest, source_mode)
