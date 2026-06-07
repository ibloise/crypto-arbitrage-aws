import os
from dataclasses import dataclass


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class PollerSettings:
    kinesis_stream: str

    @classmethod
    def from_env(cls) -> "PollerSettings":
        return cls(kinesis_stream=required_env("KINESIS_STREAM"))


@dataclass(frozen=True)
class ProcessorSettings:
    db_dsn: str
    s3_bucket: str
    max_price_age_seconds: int

    @classmethod
    def from_env(cls) -> "ProcessorSettings":
        return cls(
            db_dsn=required_env("DB_DSN"),
            s3_bucket=required_env("S3_BUCKET"),
            max_price_age_seconds=int(os.environ.get("MAX_PRICE_AGE_SECONDS", "120")),
        )
