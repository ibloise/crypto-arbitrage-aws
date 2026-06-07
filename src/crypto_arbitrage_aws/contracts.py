import base64
import json
import math
from datetime import datetime, timezone
from typing import Any


TICK_FIELDS = {"timestamp", "source_mode", "exchange", "coin", "price_usd"}
SOURCE_MODES = {"rest", "websocket"}
EXCHANGES = {"binance", "kraken", "coinbase", "bybit"}


def make_tick(
    exchange: str,
    coin: str,
    price_usd: float,
    source_mode: str,
) -> dict[str, Any]:
    tick = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source_mode": source_mode,
        "exchange": exchange,
        "coin": coin,
        "price_usd": float(price_usd),
    }
    validate_tick(tick)
    return tick


def validate_tick(tick: dict[str, Any]) -> None:
    if set(tick) != TICK_FIELDS:
        raise ValueError(f"Invalid tick fields: {set(tick)}")
    if tick["source_mode"] not in SOURCE_MODES:
        raise ValueError(f"Invalid source mode: {tick['source_mode']}")
    if tick["exchange"] not in EXCHANGES:
        raise ValueError(f"Invalid exchange: {tick['exchange']}")
    if not isinstance(tick["coin"], str) or not tick["coin"].isupper():
        raise ValueError("Coin must be an uppercase symbol")
    if (
        isinstance(tick["price_usd"], bool)
        or not isinstance(tick["price_usd"], (int, float))
        or not math.isfinite(tick["price_usd"])
        or tick["price_usd"] <= 0
    ):
        raise ValueError("Price must be a positive number")
    timestamp = datetime.fromisoformat(tick["timestamp"].replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")


def tick_to_json(tick: dict[str, Any]) -> str:
    validate_tick(tick)
    return json.dumps(tick, separators=(",", ":"))


def tick_to_kinesis_record(tick: dict[str, Any]) -> dict[str, Any]:
    return {
        "Data": tick_to_json(tick).encode("utf-8"),
        "PartitionKey": tick["coin"],
    }


def ticks_from_kinesis_event(event: dict[str, Any]) -> list[dict[str, Any]]:
    ticks = []
    for record in event.get("Records", []):
        payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        tick = json.loads(payload)
        validate_tick(tick)
        ticks.append(tick)
    return ticks
