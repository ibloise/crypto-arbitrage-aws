from functools import lru_cache

from crypto_arbitrage_aws.kinesis import publish_ticks
from crypto_arbitrage_aws.lambdas.config import PollerSettings
from crypto_arbitrage_aws.poller import (
    build_poller_plan,
    build_ticks,
    fetch_all_prices,
    get_top30_symbols,
)


@lru_cache(maxsize=1)
def _settings() -> PollerSettings:
    return PollerSettings.from_env()


@lru_cache(maxsize=1)
def _kinesis_client():
    import boto3

    return boto3.client("kinesis")


def lambda_handler(event, context):
    settings = _settings()

    plan = build_poller_plan(get_top30_symbols())
    prices = fetch_all_prices(plan.coins, clients=plan.clients)
    ticks = build_ticks(prices)

    publish_ticks(_kinesis_client(), settings.kinesis_stream, ticks)

    return {
        "statusCode": 200,
        "coins": len(plan.coins),
        "exchanges": [client.name for client in plan.clients],
        "ticks": len(ticks),
    }
