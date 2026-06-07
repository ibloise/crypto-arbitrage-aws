import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from crypto_arbitrage_aws import processor as processor_module
from crypto_arbitrage_aws.contracts import make_tick, tick_to_kinesis_record
from crypto_arbitrage_aws.database import DatabaseSettings
from crypto_arbitrage_aws.lambdas import processor as lambda_processor
from crypto_arbitrage_aws.processor import (
    detect_arbitrage,
    process_persistent_tick_batch,
    process_tick_batch,
    save_raw_ticks,
    upsert_latest_prices,
)


def test_processor_postgres_connection_uses_common_database_settings(monkeypatch) -> None:
    settings = DatabaseSettings(
        db_type="postgres",
        host="database.internal",
        port=5433,
        name="arbitrage",
        user="app",
        password="secret",
    )
    connection = object()
    calls = []
    monkeypatch.setattr(processor_module, "DB_TYPE", "postgres")
    monkeypatch.setattr(processor_module, "DB_SETTINGS", settings)
    monkeypatch.setattr(
        processor_module,
        "connect_postgres",
        lambda received: calls.append(received) or connection,
    )

    assert processor_module.get_connection() is connection
    assert calls == [settings]


def test_detect_arbitrage_returns_highest_spread_route() -> None:
    opportunities = detect_arbitrage(
        {
            "BTC": {
                "binance": 100.0,
                "kraken": 101.0,
                "coinbase": 103.0,
                "bybit": None,
            }
        },
        source_mode="rest",
        threshold_pct=2.0,
    )

    assert len(opportunities) == 1
    opportunity = opportunities[0]
    assert opportunity["coin"] == "BTC"
    assert opportunity["exchange_low"] == "binance"
    assert opportunity["exchange_high"] == "coinbase"
    assert opportunity["price_low"] == 100.0
    assert opportunity["price_high"] == 103.0
    assert opportunity["spread_pct"] == 3.0
    assert opportunity["source_mode"] == "rest"
    assert datetime.fromisoformat(opportunity["detected_at"]).tzinfo is not None


def test_detect_arbitrage_respects_threshold_and_requires_two_prices() -> None:
    opportunities = detect_arbitrage(
        {
            "BTC": {"binance": 100.0, "kraken": 100.2},
            "ETH": {"binance": 50.0, "kraken": None},
        },
        source_mode="rest",
        threshold_pct=0.3,
    )

    assert opportunities == []


def test_process_tick_batch_keeps_latest_price_and_marks_mixed_source() -> None:
    ticks = [
        {"coin": "BTC", "exchange": "binance", "price_usd": 90.0, "source_mode": "rest"},
        {"coin": "BTC", "exchange": "binance", "price_usd": 100.0, "source_mode": "websocket"},
        {"coin": "BTC", "exchange": "kraken", "price_usd": 101.0, "source_mode": "websocket"},
    ]

    opportunities = process_tick_batch(ticks)

    assert len(opportunities) == 1
    assert opportunities[0]["price_low"] == 100.0
    assert opportunities[0]["price_high"] == 101.0
    assert opportunities[0]["spread_pct"] == pytest.approx(1.0)
    assert opportunities[0]["source_mode"] == "mixed"


def test_process_tick_batch_uses_timestamp_instead_of_list_order() -> None:
    now = datetime.now(timezone.utc)
    ticks = [
        {
            "timestamp": now.isoformat(),
            "coin": "BTC",
            "exchange": "binance",
            "price_usd": 100.0,
            "source_mode": "rest",
        },
        {
            "timestamp": (now - timedelta(seconds=10)).isoformat(),
            "coin": "BTC",
            "exchange": "binance",
            "price_usd": 90.0,
            "source_mode": "rest",
        },
        {
            "timestamp": now.isoformat(),
            "coin": "BTC",
            "exchange": "kraken",
            "price_usd": 101.0,
            "source_mode": "rest",
        },
    ]

    opportunities = process_tick_batch(ticks)

    assert opportunities[0]["price_low"] == 100.0


def test_process_tick_batch_accepts_empty_batch() -> None:
    assert process_tick_batch([]) == []


def test_lambda_processor_consumes_kinesis_json_contract(monkeypatch) -> None:
    import base64

    ticks = [
        make_tick("binance", "BTC", 100.0, source_mode="websocket"),
        make_tick("kraken", "BTC", 101.0, source_mode="websocket"),
    ]
    event = {
        "Records": [
            {
                "kinesis": {
                    "data": base64.b64encode(tick_to_kinesis_record(tick)["Data"]).decode()
                }
            }
            for tick in ticks
        ]
    }
    saved_ticks = []
    monkeypatch.setattr(
        lambda_processor,
        "save_raw_ticks_to_s3",
        lambda received, bucket, client: saved_ticks.extend(received),
    )
    class FakeConnection:
        def close(self):
            return None

    monkeypatch.setattr(
        lambda_processor,
        "connect_postgres",
        lambda settings: FakeConnection(),
    )
    monkeypatch.setattr(
        lambda_processor,
        "process_persistent_tick_batch",
        lambda received, conn, **kwargs: detect_arbitrage(
            {
                "BTC": {
                    tick["exchange"]: tick["price_usd"]
                    for tick in received
                }
            },
            source_mode="websocket",
        ),
    )

    monkeypatch.setattr(lambda_processor, "_s3_client", lambda: object())
    monkeypatch.setenv("DB_TYPE", "postgres")
    monkeypatch.setenv("DB_HOST", "database.internal")
    monkeypatch.setenv("DB_USER", "app")
    monkeypatch.setenv("DB_PASSWORD", "secret")
    monkeypatch.setenv("S3_BUCKET", "raw-ticks")
    lambda_processor._settings.cache_clear()

    result = lambda_processor.lambda_handler(event, None)

    assert result == {"statusCode": 200, "opportunities": 1}
    assert saved_ticks == ticks


def test_persistent_processing_detects_across_separate_batches() -> None:
    conn = sqlite3.connect(":memory:")
    first_tick = make_tick("binance", "BTC", 100.0, source_mode="websocket")
    second_tick = make_tick("kraken", "BTC", 101.0, source_mode="websocket")

    assert process_persistent_tick_batch([first_tick], conn) == []
    opportunities = process_persistent_tick_batch([second_tick], conn)

    assert len(opportunities) == 1
    assert opportunities[0]["exchange_low"] == "binance"
    assert opportunities[0]["exchange_high"] == "kraken"
    assert conn.execute("SELECT COUNT(*) FROM latest_prices").fetchone()[0] == 2


def test_persistent_processing_is_idempotent_on_batch_retry() -> None:
    conn = sqlite3.connect(":memory:")
    ticks = [
        make_tick("binance", "BTC", 100.0, source_mode="websocket"),
        make_tick("kraken", "BTC", 101.0, source_mode="websocket"),
    ]

    first = process_persistent_tick_batch(ticks, conn)
    second = process_persistent_tick_batch(ticks, conn)

    assert first[0]["opportunity_key"] == second[0]["opportunity_key"]
    assert conn.execute(
        "SELECT COUNT(*) FROM arbitrage_opportunities"
    ).fetchone()[0] == 1


def test_latest_price_state_ignores_out_of_order_ticks() -> None:
    conn = sqlite3.connect(":memory:")
    now = datetime.now(timezone.utc)
    newer = make_tick("binance", "BTC", 100.0, source_mode="websocket")
    newer["timestamp"] = now.isoformat()
    older = make_tick("binance", "BTC", 90.0, source_mode="websocket")
    older["timestamp"] = (now - timedelta(seconds=10)).isoformat()

    process_persistent_tick_batch([newer], conn)
    upsert_latest_prices([older], conn)

    assert conn.execute(
        "SELECT price_usd FROM latest_prices WHERE coin = 'BTC'"
    ).fetchone()[0] == 100.0


def test_latest_price_state_does_not_replace_equal_timestamp() -> None:
    conn = sqlite3.connect(":memory:")
    timestamp = datetime.now(timezone.utc).isoformat()
    first = make_tick("binance", "BTC", 100.0, source_mode="websocket")
    first["timestamp"] = timestamp
    conflicting = make_tick("binance", "BTC", 90.0, source_mode="websocket")
    conflicting["timestamp"] = timestamp

    process_persistent_tick_batch([first], conn)
    upsert_latest_prices([conflicting], conn)

    assert conn.execute(
        "SELECT price_usd FROM latest_prices WHERE coin = 'BTC'"
    ).fetchone()[0] == 100.0


def test_persistent_processing_excludes_stale_prices() -> None:
    conn = sqlite3.connect(":memory:")
    stale = make_tick("binance", "BTC", 100.0, source_mode="websocket")
    stale["timestamp"] = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    fresh = make_tick("kraken", "BTC", 101.0, source_mode="websocket")

    opportunities = process_persistent_tick_batch(
        [stale, fresh],
        conn,
        max_age_seconds=120,
    )

    assert opportunities == []


def test_raw_tick_storage_is_idempotent_for_batch_retries(
    monkeypatch,
    tmp_path,
) -> None:
    ticks = [make_tick("binance", "BTC", 100.0, source_mode="websocket")]
    monkeypatch.setattr("crypto_arbitrage_aws.processor.S3_BUCKET", "")
    monkeypatch.setattr("crypto_arbitrage_aws.processor.RAW_DATA_DIR", str(tmp_path))

    save_raw_ticks(ticks)
    save_raw_ticks(ticks)

    assert len(list(tmp_path.glob("*.json"))) == 1
