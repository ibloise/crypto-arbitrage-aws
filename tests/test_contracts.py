import base64
import json

import pytest

from crypto_arbitrage_aws.contracts import (
    make_tick,
    tick_to_kinesis_record,
    ticks_from_kinesis_event,
    validate_tick,
)


def test_tick_survives_kinesis_json_boundary() -> None:
    tick = make_tick("binance", "BTC", 100.25, source_mode="websocket")
    record = tick_to_kinesis_record(tick)
    event = {
        "Records": [
            {
                "kinesis": {
                    "data": base64.b64encode(record["Data"]).decode("ascii"),
                }
            }
        ]
    }

    assert ticks_from_kinesis_event(event) == [tick]
    assert isinstance(record["Data"], bytes)
    assert record["PartitionKey"] == "BTC"


def test_tick_contract_rejects_invalid_payload() -> None:
    with pytest.raises(ValueError, match="Invalid tick fields"):
        validate_tick({"coin": "BTC"})


@pytest.mark.parametrize(
    "invalid_tick",
    [
        {
            "timestamp": "2026-06-07T10:00:00",
            "source_mode": "rest",
            "exchange": "binance",
            "coin": "BTC",
            "price_usd": 100.0,
        },
        {
            "timestamp": "2026-06-07T10:00:00+00:00",
            "source_mode": "rest",
            "exchange": "unknown",
            "coin": "BTC",
            "price_usd": 100.0,
        },
        {
            "timestamp": "2026-06-07T10:00:00+00:00",
            "source_mode": "rest",
            "exchange": "binance",
            "coin": "BTC",
            "price_usd": float("nan"),
        },
    ],
)
def test_tick_contract_rejects_non_interoperable_values(invalid_tick) -> None:
    with pytest.raises(ValueError):
        validate_tick(invalid_tick)


def test_kinesis_event_rejects_non_contract_json() -> None:
    payload = base64.b64encode(json.dumps({"coin": "BTC"}).encode()).decode()

    with pytest.raises(ValueError, match="Invalid tick fields"):
        ticks_from_kinesis_event({"Records": [{"kinesis": {"data": payload}}]})
