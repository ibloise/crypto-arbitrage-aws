-- PostgreSQL schema for AWS RDS
-- Run once after creating the RDS instance:
--   psql -h <rds-endpoint> -U <user> -d <dbname> -f schema.sql

CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
    id            SERIAL          PRIMARY KEY,
    opportunity_key VARCHAR(64)   UNIQUE,
    detected_at   TIMESTAMPTZ     NOT NULL,
    coin          VARCHAR(20)     NOT NULL,
    exchange_low  VARCHAR(20)     NOT NULL,
    exchange_high VARCHAR(20)     NOT NULL,
    price_low     NUMERIC(20, 8)  NOT NULL,
    price_high    NUMERIC(20, 8)  NOT NULL,
    spread_pct    NUMERIC(8, 4)   NOT NULL,
    source_mode   VARCHAR(10)     NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_opportunities_detected_at ON arbitrage_opportunities (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_opportunities_coin        ON arbitrage_opportunities (coin);

CREATE TABLE IF NOT EXISTS latest_prices (
    coin        VARCHAR(20)    NOT NULL,
    exchange    VARCHAR(20)    NOT NULL,
    observed_at TIMESTAMPTZ    NOT NULL,
    price_usd   NUMERIC(20, 8) NOT NULL,
    source_mode VARCHAR(10)    NOT NULL,
    PRIMARY KEY (coin, exchange)
);
