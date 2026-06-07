import pytest

from crypto_arbitrage_aws.lambdas.config import PollerSettings, ProcessorSettings


def test_processor_lambda_requires_aws_storage_configuration(monkeypatch) -> None:
    monkeypatch.delenv("DB_DSN", raising=False)
    monkeypatch.delenv("S3_BUCKET", raising=False)

    with pytest.raises(RuntimeError, match="DB_DSN"):
        ProcessorSettings.from_env()


def test_processor_lambda_loads_explicit_configuration(monkeypatch) -> None:
    monkeypatch.setenv("DB_DSN", "postgresql://proxy/database")
    monkeypatch.setenv("S3_BUCKET", "raw-ticks")
    monkeypatch.setenv("MAX_PRICE_AGE_SECONDS", "90")

    settings = ProcessorSettings.from_env()

    assert settings.db_dsn == "postgresql://proxy/database"
    assert settings.s3_bucket == "raw-ticks"
    assert settings.max_price_age_seconds == 90


def test_poller_lambda_requires_kinesis_stream(monkeypatch) -> None:
    monkeypatch.delenv("KINESIS_STREAM", raising=False)

    with pytest.raises(RuntimeError, match="KINESIS_STREAM"):
        PollerSettings.from_env()
