from functools import lru_cache

from crypto_arbitrage_aws.kinesis import publish_ticks
from crypto_arbitrage_aws.lambdas.config import PollerSettings
from crypto_arbitrage_aws.poller import (
    build_ticks,
    fetch_all_prices,
    get_top30_symbols,
    get_tradeable_coins,
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
    coins = get_tradeable_coins(get_top30_symbols())
    ticks = build_ticks(fetch_all_prices(coins))
    publish_ticks(_kinesis_client(), settings.kinesis_stream, ticks)
    return {"statusCode": 200, "ticks": len(ticks)}
