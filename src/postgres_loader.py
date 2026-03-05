from __future__ import annotations

import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor, execute_values

from src.config import PostgresConfig


@dataclass(frozen=True)
class SyncState:
    job_name: str
    last_write_date: datetime | None
    last_odoo_id: int | None


class PostgresLoader:
    def __init__(self, cfg: PostgresConfig) -> None:
        self._cfg = cfg

    @contextmanager
    def connect(self) -> Iterator[Any]:
        conn = psycopg2.connect(
            host=self._cfg.host,
            port=self._cfg.port,
            dbname=self._cfg.database,
            user=self._cfg.user,
            password=self._cfg.password,
            sslmode=self._cfg.sslmode,
        )
        try:
            yield conn
        finally:
            conn.close()

    def ensure_metadata_tables(self, conn: Any) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_state (
                    job_name text PRIMARY KEY,
                    last_write_date timestamp NULL,
                    last_odoo_id bigint NULL,
                    updated_at timestamp NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_runs (
                    run_id text PRIMARY KEY,
                    job_name text NOT NULL,
                    mode text NOT NULL,
                    started_at timestamp NOT NULL,
                    ended_at timestamp NULL,
                    rows_extracted bigint NOT NULL DEFAULT 0,
                    rows_upserted bigint NOT NULL DEFAULT 0,
                    rows_failed bigint NOT NULL DEFAULT 0,
                    status text NOT NULL,
                    error_message text NULL
                )
                """
            )
        conn.commit()

    def start_run(self, conn: Any, job_name: str, mode: str) -> str:
        run_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sync_runs (run_id, job_name, mode, started_at, status)
                VALUES (%s, %s, %s, now(), %s)
                """,
                (run_id, job_name, mode, "running"),
            )
        conn.commit()
        return run_id

    def finish_run(
        self,
        conn: Any,
        run_id: str,
        status: str,
        rows_extracted: int,
        rows_upserted: int,
        rows_failed: int = 0,
        error_message: str | None = None,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sync_runs
                SET ended_at = now(),
                    rows_extracted = %s,
                    rows_upserted = %s,
                    rows_failed = %s,
                    status = %s,
                    error_message = %s
                WHERE run_id = %s
                """,
                (rows_extracted, rows_upserted, rows_failed, status, error_message, run_id),
            )
        conn.commit()

    def get_state(self, conn: Any, job_name: str) -> SyncState:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                "SELECT job_name, last_write_date, last_odoo_id FROM sync_state WHERE job_name = %s",
                (job_name,),
            )
            row = cur.fetchone()
        if not row:
            return SyncState(job_name=job_name, last_write_date=None, last_odoo_id=None)
        return SyncState(
            job_name=row["job_name"],
            last_write_date=row["last_write_date"],
            last_odoo_id=row["last_odoo_id"],
        )

    def set_state(self, conn: Any, state: SyncState) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sync_state (job_name, last_write_date, last_odoo_id, updated_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (job_name)
                DO UPDATE SET
                    last_write_date = EXCLUDED.last_write_date,
                    last_odoo_id = EXCLUDED.last_odoo_id,
                    updated_at = now()
                """,
                (state.job_name, state.last_write_date, state.last_odoo_id),
            )
        conn.commit()

    def upsert_rows(
        self,
        conn: Any,
        table: str,
        rows: list[dict[str, Any]],
        conflict_keys: list[str],
    ) -> int:
        if not rows:
            return 0

        if conflict_keys:
            # Keep last row per conflict key tuple to avoid duplicate-key collisions
            # inside the same INSERT ... ON CONFLICT statement.
            deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
            for row in rows:
                key = tuple(row.get(k) for k in conflict_keys)
                deduped[key] = row
            rows = list(deduped.values())

        columns = list(rows[0].keys())
        values = [tuple(row.get(col) for col in columns) for row in rows]

        with conn.cursor() as cur:
            insert_cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
            base_query = sql.SQL("INSERT INTO {table} ({cols}) VALUES %s").format(
                table=sql.Identifier(table),
                cols=insert_cols,
            )

            if conflict_keys:
                update_cols = [c for c in columns if c not in conflict_keys]
                conflict_cols = sql.SQL(", ").join(sql.Identifier(c) for c in conflict_keys)

                if update_cols:
                    set_clause = sql.SQL(", ").join(
                        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c)) for c in update_cols
                    )
                    query = base_query + sql.SQL(" ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}").format(
                        conflict_cols=conflict_cols,
                        set_clause=set_clause,
                    )
                else:
                    query = base_query + sql.SQL(" ON CONFLICT ({conflict_cols}) DO NOTHING").format(
                        conflict_cols=conflict_cols
                    )
            else:
                query = base_query

            execute_values(cur, query, values, page_size=1000)
        conn.commit()
        return len(rows)
