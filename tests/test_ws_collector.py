import asyncio
from datetime import datetime

import pytest

from crypto_arbitrage_aws.ws_collector import (
    BinanceTickerClient,
    BybitTickerClient,
    CoinbaseTickerClient,
    KrakenTickerClient,
    TickBatchProcessor,
    build_clients,
)


async def discard_tick(_tick: dict) -> None:
    pass


@pytest.mark.parametrize(
    ("client_type", "message", "exchange"),
    [
        (
            BinanceTickerClient,
            {"data": {"s": "BTCUSDT", "c": "100.25"}},
            "binance",
        ),
        (
            KrakenTickerClient,
            [42, {"c": ["100.25", "1.0"]}, "ticker", "XBT/USD"],
            "kraken",
        ),
        (
            CoinbaseTickerClient,
            {"type": "ticker", "product_id": "BTC-USD", "price": "100.25"},
            "coinbase",
        ),
        (
            BybitTickerClient,
            {
                "topic": "tickers.BTCUSDT",
                "data": {"symbol": "BTCUSDT", "lastPrice": "100.25"},
            },
            "bybit",
        ),
    ],
)
def test_socket_messages_share_normalized_tick_contract(
    client_type,
    message: object,
    exchange: str,
) -> None:
    client = client_type(["BTC"], discard_tick)
    tick = client.normalize_message(message)

    assert tick is not None
    assert set(tick) == {"timestamp", "source_mode", "exchange", "coin", "price_usd"}
    assert tick["source_mode"] == "websocket"
    assert tick["exchange"] == exchange
    assert tick["coin"] == "BTC"
    assert tick["price_usd"] == 100.25
    assert datetime.fromisoformat(tick["timestamp"]).tzinfo is not None


@pytest.mark.parametrize(
    ("client_type", "message"),
    [
        (BinanceTickerClient, {"result": None}),
        (KrakenTickerClient, {"event": "heartbeat"}),
        (CoinbaseTickerClient, {"type": "subscriptions"}),
        (BybitTickerClient, {"op": "pong"}),
    ],
)
def test_socket_clients_ignore_non_ticker_messages(client_type, message: object) -> None:
    client = client_type(["BTC"], discard_tick)
    assert client.normalize_message(message) is None


@pytest.mark.parametrize(
    ("client_type", "message"),
    [
        (BinanceTickerClient, {"data": {"s": "ETHUSDT", "c": "10"}}),
        (KrakenTickerClient, [42, {"c": ["10"]}, "ticker", "ETH/USD"]),
        (
            CoinbaseTickerClient,
            {"type": "ticker", "product_id": "ETH-USD", "price": "10"},
        ),
        (
            BybitTickerClient,
            {
                "topic": "tickers.ETHUSDT",
                "data": {"symbol": "ETHUSDT", "lastPrice": "10"},
            },
        ),
    ],
)
def test_socket_clients_ignore_untracked_coins(client_type, message: object) -> None:
    client = client_type(["BTC"], discard_tick)
    assert client.normalize_message(message) is None


def test_build_clients_returns_one_client_per_exchange() -> None:
    clients = build_clients(["BTC"], discard_tick)

    assert [client.name for client in clients] == [
        "binance",
        "kraken",
        "coinbase",
        "bybit",
    ]


@pytest.mark.parametrize(
    ("client_type", "env_name", "custom_url"),
    [
        (BinanceTickerClient, "BINANCE_WS_URL", "wss://binance.example/stream"),
        (KrakenTickerClient, "KRAKEN_WS_URL", "wss://kraken.example"),
        (CoinbaseTickerClient, "COINBASE_WS_URL", "wss://coinbase.example"),
        (BybitTickerClient, "BYBIT_WS_URL", "wss://bybit.example/spot"),
    ],
)
def test_socket_endpoint_can_be_overridden_by_environment(
    monkeypatch,
    client_type,
    env_name: str,
    custom_url: str,
) -> None:
    monkeypatch.setenv(env_name, custom_url)

    client = client_type(["BTC"], discard_tick)

    assert client.ws_url == custom_url


def test_binance_endpoint_keeps_default_and_appends_streams(monkeypatch) -> None:
    monkeypatch.delenv("BINANCE_WS_URL", raising=False)

    client = BinanceTickerClient(["BTC", "ETH"], discard_tick)

    assert client.connection_url == (
        "wss://stream.binance.com:9443/stream"
        "?streams=btcusdt@ticker/ethusdt@ticker"
    )


def test_binance_endpoint_supports_query_and_stream_template(monkeypatch) -> None:
    monkeypatch.setenv("BINANCE_WS_URL", "wss://proxy.example/ws?region=us")
    with_query = BinanceTickerClient(["BTC"], discard_tick)

    monkeypatch.setenv("BINANCE_WS_URL", "wss://proxy.example/{streams}")
    with_template = BinanceTickerClient(["BTC"], discard_tick)

    assert with_query.connection_url == (
        "wss://proxy.example/ws?region=us&streams=btcusdt@ticker"
    )
    assert with_template.connection_url == "wss://proxy.example/btcusdt@ticker"


def test_explicit_socket_endpoint_takes_precedence_over_environment(monkeypatch) -> None:
    monkeypatch.setenv("KRAKEN_WS_URL", "wss://environment.example")

    client = KrakenTickerClient(
        ["BTC"],
        discard_tick,
        ws_url="wss://explicit.example",
    )

    assert client.connection_url == "wss://explicit.example"


def test_batch_processor_drains_queue() -> None:
    queue = asyncio.Queue()
    queue.put_nowait({"coin": "BTC"})
    queue.put_nowait({"coin": "ETH"})

    processor = TickBatchProcessor(queue)

    assert processor._drain_queue() == [{"coin": "BTC"}, {"coin": "ETH"}]
    assert queue.empty()


def test_kinesis_batch_processor_does_not_initialize_database(monkeypatch) -> None:
    processor = TickBatchProcessor(asyncio.Queue(), kinesis_stream="ticks")

    def fail_if_called() -> None:
        raise AssertionError("Kinesis mode must not initialize a database")

    monkeypatch.setattr(processor, "_init_database", fail_if_called)

    async def stop_after_first_sleep(_interval: int) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", stop_after_first_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(processor.run_forever())


def test_publish_to_kinesis_uses_json_bytes_and_detects_partial_failure(
    monkeypatch,
) -> None:
    class FakeKinesis:
        def __init__(self) -> None:
            self.records = []

        def put_records(self, **kwargs):
            self.records.extend(kwargs["Records"])
            return {"FailedRecordCount": 1}

    fake_kinesis = FakeKinesis()
    processor = TickBatchProcessor(asyncio.Queue(), kinesis_stream="ticks")
    monkeypatch.setattr(processor, "_get_kinesis_client", lambda: fake_kinesis)
    tick = BinanceTickerClient(["BTC"], discard_tick).normalize_message(
        {"data": {"s": "BTCUSDT", "c": "100.25"}}
    )

    with pytest.raises(RuntimeError, match="rejected 1 records"):
        processor._publish_to_kinesis([tick])

    assert isinstance(fake_kinesis.records[0]["Data"], bytes)
