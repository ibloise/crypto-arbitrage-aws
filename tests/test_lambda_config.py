import pytest

from crypto_arbitrage_aws.database import DatabaseSettings
from crypto_arbitrage_aws.lambdas.config import (
    PollerSettings,
    ProcessorSettings,
)


def test_processor_lambda_requires_aws_storage_configuration(monkeypatch) -> None:
    monkeypatch.setenv("DB_TYPE", "postgres")
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    monkeypatch.delenv("S3_BUCKET", raising=False)

    with pytest.raises(RuntimeError, match="DB_HOST"):
        ProcessorSettings.from_env()


def test_processor_lambda_requires_explicit_postgres_type(monkeypatch) -> None:
    monkeypatch.delenv("DB_TYPE", raising=False)
    monkeypatch.setenv("DB_HOST", "database.internal")
    monkeypatch.setenv("DB_USER", "app")
    monkeypatch.setenv("DB_PASSWORD", "secret")
    monkeypatch.setenv("S3_BUCKET", "raw-ticks")

    with pytest.raises(RuntimeError, match="DB_TYPE"):
        ProcessorSettings.from_env()


def test_processor_lambda_loads_explicit_configuration(monkeypatch) -> None:
    monkeypatch.setenv("DB_TYPE", "postgres")
    monkeypatch.setenv("DB_HOST", "database.internal")
    monkeypatch.setenv("DB_PORT", "5433")
    monkeypatch.setenv("DB_NAME", "arbitrage")
    monkeypatch.setenv("DB_USER", "app")
    monkeypatch.setenv("DB_PASSWORD", "secret")
    monkeypatch.setenv("S3_BUCKET", "raw-ticks")
    monkeypatch.setenv("MAX_PRICE_AGE_SECONDS", "90")

    settings = ProcessorSettings.from_env()

    assert settings.database == DatabaseSettings(
        db_type="postgres",
        host="database.internal",
        port=5433,
        name="arbitrage",
        user="app",
        password="secret",
    )
    assert settings.s3_bucket == "raw-ticks"
    assert settings.max_price_age_seconds == 90


def test_poller_lambda_requires_kinesis_stream(monkeypatch) -> None:
    monkeypatch.delenv("KINESIS_STREAM", raising=False)

    with pytest.raises(RuntimeError, match="KINESIS_STREAM"):
        PollerSettings.from_env()


def test_init_db_lambda_loads_connection_configuration(monkeypatch) -> None:
    monkeypatch.setenv("DB_TYPE", "postgres")
    monkeypatch.setenv("DB_HOST", "database.internal")
    monkeypatch.setenv("DB_PORT", "5433")
    monkeypatch.setenv("DB_NAME", "arbitrage")
    monkeypatch.setenv("DB_USER", "app")
    monkeypatch.setenv("DB_PASSWORD", "secret")

    settings = DatabaseSettings.from_env(require_postgres=True)

    assert settings.host == "database.internal"
    assert settings.port == 5433
    assert settings.name == "arbitrage"
    assert settings.user == "app"
    assert settings.password == "secret"


def test_init_db_lambda_requires_connection_credentials(monkeypatch) -> None:
    monkeypatch.setenv("DB_TYPE", "postgres")
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)

    with pytest.raises(RuntimeError, match="DB_HOST"):
        DatabaseSettings.from_env(require_postgres=True)
