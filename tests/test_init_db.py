import sys
from types import SimpleNamespace

from crypto_arbitrage_aws.database import DatabaseSettings, connect_postgres
from crypto_arbitrage_aws.lambdas import init_db as lambda_init_db


class FakeCursor:
    def __init__(self) -> None:
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return None

    def execute(self, sql: str) -> None:
        self.executed.append(sql)


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return None

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def close(self) -> None:
        self.closed = True


def test_init_db_connects_with_explicit_postgres_parameters(monkeypatch) -> None:
    calls = []
    monkeypatch.setitem(
        sys.modules,
        "psycopg2",
        SimpleNamespace(connect=lambda **kwargs: calls.append(kwargs) or FakeConnection()),
    )
    settings = DatabaseSettings(
        db_type="postgres",
        host="database.internal",
        port=5433,
        name="arbitrage",
        user="app",
        password="secret",
    )

    connect_postgres(settings)

    assert calls == [
        {
            "host": "database.internal",
            "port": 5433,
            "dbname": "arbitrage",
            "user": "app",
            "password": "secret",
            "connect_timeout": 10,
        }
    ]


def test_init_db_lambda_executes_idempotent_schema_and_closes_connection(
    monkeypatch,
) -> None:
    connection = FakeConnection()
    monkeypatch.setenv("DB_TYPE", "postgres")
    monkeypatch.setenv("DB_HOST", "database.internal")
    monkeypatch.setenv("DB_USER", "app")
    monkeypatch.setenv("DB_PASSWORD", "secret")
    lambda_init_db._settings.cache_clear()
    monkeypatch.setattr(lambda_init_db, "connect_postgres", lambda settings: connection)

    result = lambda_init_db.lambda_handler({}, None)

    assert result == {"status": "ok", "message": "Schema initialized"}
    assert connection.closed
    assert connection.cursor_instance.executed == [lambda_init_db.SCHEMA_SQL]
    assert "CREATE TABLE IF NOT EXISTS arbitrage_opportunities" in lambda_init_db.SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS latest_prices" in lambda_init_db.SCHEMA_SQL


def test_init_db_lambda_closes_connection_when_schema_execution_fails(
    monkeypatch,
) -> None:
    connection = FakeConnection()
    monkeypatch.setenv("DB_TYPE", "postgres")
    monkeypatch.setenv("DB_HOST", "database.internal")
    monkeypatch.setenv("DB_USER", "app")
    monkeypatch.setenv("DB_PASSWORD", "secret")
    lambda_init_db._settings.cache_clear()
    monkeypatch.setattr(lambda_init_db, "connect_postgres", lambda settings: connection)

    def fail(sql: str) -> None:
        raise RuntimeError("database error")

    monkeypatch.setattr(connection.cursor_instance, "execute", fail)

    try:
        lambda_init_db.lambda_handler({}, None)
    except RuntimeError as exc:
        assert str(exc) == "database error"
    else:
        raise AssertionError("Database errors must propagate")

    assert connection.closed
