from crypto_arbitrage_aws.contracts import make_tick
from crypto_arbitrage_aws.kinesis import publish_ticks


def test_publish_ticks_retries_only_failed_records() -> None:
    class FakeKinesis:
        def __init__(self) -> None:
            self.calls = []

        def put_records(self, **kwargs):
            self.calls.append(kwargs["Records"])
            if len(self.calls) == 1:
                return {
                    "FailedRecordCount": 1,
                    "Records": [{}, {"ErrorCode": "ProvisionedThroughputExceededException"}],
                }
            return {"FailedRecordCount": 0, "Records": [{}]}

    client = FakeKinesis()
    ticks = [
        make_tick("binance", "BTC", 100.0, source_mode="rest"),
        make_tick("kraken", "BTC", 101.0, source_mode="rest"),
    ]

    assert publish_ticks(client, "ticks", ticks) == 2
    assert len(client.calls) == 2
    assert len(client.calls[0]) == 2
    assert len(client.calls[1]) == 1
    assert all(isinstance(record["Data"], bytes) for record in client.calls[0])
