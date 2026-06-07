from functools import lru_cache

from crypto_arbitrage_aws.contracts import ticks_from_kinesis_event
from crypto_arbitrage_aws.database import connect_postgres
from crypto_arbitrage_aws.lambdas.config import ProcessorSettings
from crypto_arbitrage_aws.processor import (
    process_persistent_tick_batch,
    save_raw_ticks_to_s3,
)


@lru_cache(maxsize=1)
def _settings() -> ProcessorSettings:
    return ProcessorSettings.from_env()


@lru_cache(maxsize=1)
def _s3_client():
    import boto3

    return boto3.client("s3")


def lambda_handler(event, context):
    settings = _settings()
    ticks = ticks_from_kinesis_event(event)
    save_raw_ticks_to_s3(ticks, settings.s3_bucket, _s3_client())

    conn = connect_postgres(settings.database)
    try:
        opportunities = process_persistent_tick_batch(
            ticks,
            conn,
            max_age_seconds=settings.max_price_age_seconds,
        )
    finally:
        conn.close()

    return {"statusCode": 200, "opportunities": len(opportunities)}
