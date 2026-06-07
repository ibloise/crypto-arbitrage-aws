import sys
from types import SimpleNamespace

from crypto_arbitrage_aws import poller
from crypto_arbitrage_aws.lambdas import poller as lambda_poller
from crypto_arbitrage_aws.poller import (
    BinanceRestClient,
    CoinUniverseClient,
    ExchangeRestClient,
    build_ticks,
)


class FakeResponse:
    def __init__(self, payload) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self.payload


class FakeExchangeClient(ExchangeRestClient):
    def __init__(
        self,
        name: str,
        available: set[str] | Exception,
        prices: dict[str, float | Exception],
    ) -> None:
        self.name = name
        self._available = available
        self._prices = prices

    def available_coins(self) -> set[str]:
        if isinstance(self._available, Exception):
            raise self._available
        return self._available

    def price(self, coin: str) -> float:
        value = self._prices[coin]
        if isinstance(value, Exception):
            raise value
        return value


def test_build_ticks_generates_rest_contract_dicts() -> None:
    ticks = build_ticks(
        {
            "BTC": {"binance": 100.0, "kraken": None},
            "ETH": {"coinbase": 50.0},
        }
    )

    assert len(ticks) == 2
    assert {tick["coin"] for tick in ticks} == {"BTC", "ETH"}
    assert all(tick["source_mode"] == "rest" for tick in ticks)
    assert all(isinstance(tick, dict) for tick in ticks)


def test_lambda_poller_publishes_json_records(monkeypatch) -> None:
    class FakeKinesis:
        def __init__(self) -> None:
            self.calls = []

        def put_records(self, **kwargs):
            self.calls.append(kwargs)
            return {"FailedRecordCount": 0, "Records": [{}]}

    kinesis = FakeKinesis()
    monkeypatch.setitem(
        sys.modules,
        "boto3",
        SimpleNamespace(client=lambda service: kinesis),
    )
    monkeypatch.setenv("KINESIS_STREAM", "ticks")
    lambda_poller._settings.cache_clear()
    lambda_poller._kinesis_client.cache_clear()
    monkeypatch.setattr(lambda_poller, "get_top30_symbols", lambda: ["BTC"])
    client = SimpleNamespace(name="binance")
    monkeypatch.setattr(
        lambda_poller,
        "build_poller_plan",
        lambda top: SimpleNamespace(coins=top, clients=[client]),
    )
    monkeypatch.setattr(
        lambda_poller,
        "fetch_all_prices",
        lambda coins, clients: {"BTC": {"binance": 100.0}},
    )

    result = lambda_poller.lambda_handler({}, None)

    assert result == {
        "statusCode": 200,
        "coins": 1,
        "exchanges": ["binance"],
        "ticks": 1,
    }
    assert kinesis.calls[0]["StreamName"] == "ticks"
    assert isinstance(kinesis.calls[0]["Records"][0]["Data"], bytes)


def test_lambda_poller_requires_stream_configuration(monkeypatch) -> None:
    lambda_poller._settings.cache_clear()
    monkeypatch.delenv("KINESIS_STREAM", raising=False)

    try:
        lambda_poller.lambda_handler({}, None)
    except RuntimeError as exc:
        assert "KINESIS_STREAM" in str(exc)
    else:
        raise AssertionError("Missing KINESIS_STREAM must fail fast")


def test_rest_endpoint_can_be_overridden_by_environment(monkeypatch) -> None:
    calls = []
    monkeypatch.setenv("BINANCE_REST_URL", "https://proxy.example/binance")
    client = BinanceRestClient(
        request_get=lambda url, **kwargs: calls.append((url, kwargs))
        or FakeResponse({"price": "100.25"})
    )

    assert client.price("BTC") == 100.25
    assert calls[0][0] == "https://proxy.example/binance/api/v3/ticker/price"
    assert calls[0][1]["params"] == {"symbol": "BTCUSDT"}


def test_rest_endpoint_keeps_public_default(monkeypatch) -> None:
    monkeypatch.delenv("BINANCE_REST_URL", raising=False)

    assert BinanceRestClient().endpoint("api/v3/ticker/price") == (
        "https://api.binance.com/api/v3/ticker/price"
    )


def test_coingecko_failure_uses_fallback_universe(monkeypatch) -> None:
    client = CoinUniverseClient(
        request_get=lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("offline")
        )
    )

    assert poller.get_top30_symbols(client) == poller.DEFAULT_COIN_UNIVERSE


def test_unavailable_exchange_does_not_block_tradeable_coin_discovery(
) -> None:
    clients = [
        FakeExchangeClient("binance", {"BTC", "ETH"}, {}),
        FakeExchangeClient("kraken", {"BTC", "ETH"}, {}),
        FakeExchangeClient("coinbase", {"BTC"}, {}),
        FakeExchangeClient("bybit", RuntimeError("offline"), {}),
    ]

    assert poller.get_tradeable_coins(["BTC", "ETH", "SOL"], clients) == ["BTC"]


def test_unavailable_price_endpoint_only_removes_its_ticks() -> None:
    clients = [
        FakeExchangeClient("binance", set(), {"BTC": 100.0}),
        FakeExchangeClient("kraken", set(), {"BTC": RuntimeError("offline")}),
    ]

    prices = poller.fetch_all_prices(["BTC"], clients)
    ticks = poller.build_ticks(prices)

    assert prices == {"BTC": {"binance": 100.0, "kraken": None}}
    assert len(ticks) == 1
    assert ticks[0]["exchange"] == "binance"


def test_build_exchange_clients_returns_one_client_per_exchange() -> None:
    clients = poller.build_exchange_clients()

    assert [client.name for client in clients] == [
        "binance",
        "kraken",
        "coinbase",
        "bybit",
    ]


def test_orchestrators_accept_an_empty_client_set() -> None:
    assert poller.get_tradeable_coins(["BTC"], []) == ["BTC"]
    assert poller.fetch_all_prices(["BTC"], []) == {"BTC": {}}
