from functools import lru_cache

from crypto_arbitrage_aws.database import DatabaseSettings, connect_postgres


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
    id BIGSERIAL PRIMARY KEY,
    opportunity_key TEXT NOT NULL UNIQUE,
    detected_at TIMESTAMPTZ NOT NULL,
    coin TEXT NOT NULL,
    exchange_low TEXT NOT NULL,
    exchange_high TEXT NOT NULL,
    price_low NUMERIC NOT NULL,
    price_high NUMERIC NOT NULL,
    spread_pct NUMERIC NOT NULL,
    source_mode TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_arbitrage_detected_at
ON arbitrage_opportunities (detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_arbitrage_coin
ON arbitrage_opportunities (coin);

CREATE TABLE IF NOT EXISTS latest_prices (
    coin TEXT NOT NULL,
    exchange TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    price_usd NUMERIC NOT NULL,
    source_mode TEXT NOT NULL,
    PRIMARY KEY (coin, exchange)
);
"""


@lru_cache(maxsize=1)
def _settings() -> DatabaseSettings:
    return DatabaseSettings.from_env(require_postgres=True)


def lambda_handler(event, context):
    conn = connect_postgres(_settings())
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(SCHEMA_SQL)
        return {"status": "ok", "message": "Schema initialized"}
    finally:
        conn.close()
