import sys
from types import SimpleNamespace

from crypto_arbitrage_aws.lambdas import poller as lambda_poller
from crypto_arbitrage_aws.poller import build_ticks


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
    monkeypatch.setattr(lambda_poller, "get_tradeable_coins", lambda top: top)
    monkeypatch.setattr(
        lambda_poller,
        "fetch_all_prices",
        lambda coins: {"BTC": {"binance": 100.0}},
    )

    result = lambda_poller.lambda_handler({}, None)

    assert result == {"statusCode": 200, "ticks": 1}
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
