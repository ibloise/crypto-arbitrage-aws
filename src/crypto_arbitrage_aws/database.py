import os
from dataclasses import dataclass


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class DatabaseSettings:
    db_type: str
    host: str
    port: int
    name: str
    user: str
    password: str

    @classmethod
    def from_env(cls, *, require_postgres: bool = False) -> "DatabaseSettings":
        db_type = _required_env("DB_TYPE") if require_postgres else os.environ.get("DB_TYPE", "sqlite")
        if db_type not in {"postgres", "sqlite"}:
            raise RuntimeError(f"Unsupported DB_TYPE: {db_type}")
        if require_postgres and db_type != "postgres":
            raise RuntimeError("DB_TYPE must be postgres")

        is_postgres = db_type == "postgres"
        return cls(
            db_type=db_type,
            host=_required_env("DB_HOST") if is_postgres else "",
            port=int(os.environ.get("DB_PORT", "5432")),
            name=os.environ.get("DB_NAME", "postgres"),
            user=_required_env("DB_USER") if is_postgres else "",
            password=_required_env("DB_PASSWORD") if is_postgres else "",
        )

    def postgres_kwargs(self) -> dict:
        if self.db_type != "postgres":
            raise RuntimeError("PostgreSQL settings requested for a non-postgres database")
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.name,
            "user": self.user,
            "password": self.password,
            "connect_timeout": 10,
        }


def connect_postgres(settings: DatabaseSettings):
    import psycopg2

    return psycopg2.connect(**settings.postgres_kwargs())
