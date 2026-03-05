from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class OdooConfig:
    url: str
    db: str
    username: str
    api_key: str


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str = "prefer"


@dataclass(frozen=True)
class AppConfig:
    odoo: OdooConfig
    postgres: PostgresConfig
    sync_batch_size: int = 1000
    sync_log_level: str = "INFO"
    sync_timezone: str = "Asia/Singapore"


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def load_config() -> AppConfig:
    load_dotenv(encoding="utf-8-sig")

    odoo_cfg = OdooConfig(
        url=_require_env("ODOO_URL").rstrip("/"),
        db=_require_env("ODOO_DB"),
        username=_require_env("ODOO_USERNAME"),
        api_key=_require_env("ODOO_API_KEY"),
    )
    pg_cfg = PostgresConfig(
        host=_require_env("PGHOST"),
        port=int(os.getenv("PGPORT", "5432")),
        database=_require_env("PGDATABASE"),
        user=_require_env("PGUSER"),
        password=_require_env("PGPASSWORD"),
        sslmode=os.getenv("PGSSLMODE", "prefer"),
    )

    return AppConfig(
        odoo=odoo_cfg,
        postgres=pg_cfg,
        sync_batch_size=int(os.getenv("SYNC_BATCH_SIZE", "1000")),
        sync_log_level=os.getenv("SYNC_LOG_LEVEL", "INFO"),
        sync_timezone=os.getenv("SYNC_TIMEZONE", "Asia/Singapore"),
    )
