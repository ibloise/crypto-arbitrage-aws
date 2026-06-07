import logging
from functools import lru_cache

from crypto_arbitrage_aws.kinesis import publish_ticks
from crypto_arbitrage_aws.lambdas.config import PollerSettings
from crypto_arbitrage_aws.observability import configure_logging
from crypto_arbitrage_aws.poller import (
    build_poller_plan,
    build_ticks,
    fetch_all_prices,
    get_top30_symbols,
)

LOGGER = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _settings() -> PollerSettings:
    return PollerSettings.from_env()


@lru_cache(maxsize=1)
def _kinesis_client():
    import boto3

    return boto3.client("kinesis")


def lambda_handler(event, context):
    configure_logging()
    settings = _settings()
    request_id = getattr(context, "aws_request_id", "local")
    LOGGER.info(
        "Poller Lambda start request_id=%s stream=%s",
        request_id,
        settings.kinesis_stream,
    )

    plan = build_poller_plan(get_top30_symbols())
    prices = fetch_all_prices(plan.coins, clients=plan.clients)
    ticks = build_ticks(prices)

    publish_ticks(_kinesis_client(), settings.kinesis_stream, ticks)
    LOGGER.info(
        "Poller Lambda complete request_id=%s stream=%s exchanges=%s coins=%d ticks=%d",
        request_id,
        settings.kinesis_stream,
        [client.name for client in plan.clients],
        len(plan.coins),
        len(ticks),
    )

    return {
        "statusCode": 200,
        "coins": len(plan.coins),
        "exchanges": [client.name for client in plan.clients],
        "ticks": len(ticks),
    }
