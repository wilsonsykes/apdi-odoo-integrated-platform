from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config import AppConfig
from src.mapping_loader import MappingConfig, load_mapping
from src.odoo_client import OdooXmlRpcClient
from src.postgres_loader import PostgresLoader, SyncState
from src.transforms import apply_transform


@dataclass(frozen=True)
class SyncResult:
    rows_extracted: int
    rows_upserted: int


def _build_domain(mode: str, mapping: MappingConfig, state: SyncState) -> list[Any]:
    domain: list[Any] = list(mapping.domain or [])
    if mode == "incremental" and state.last_write_date is not None:
        domain.append([mapping.cursor.field, ">", state.last_write_date.strftime("%Y-%m-%d %H:%M:%S")])
    return domain


def _map_record(record: dict[str, Any], mapping: MappingConfig) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field in mapping.fields:
        raw = record.get(field.source)
        out[field.target] = apply_transform(raw, field.transform, field.null_if)
    return out


def _extract_cursor(record: dict[str, Any], mapping: MappingConfig) -> tuple[datetime | None, int | None]:
    write_value = record.get(mapping.cursor.field)
    row_id = record.get(mapping.cursor.tie_breaker)

    write_date: datetime | None = None
    if isinstance(write_value, str) and write_value:
        write_date = datetime.fromisoformat(write_value.replace(" ", "T"))
    elif isinstance(write_value, datetime):
        write_date = write_value

    odoo_id: int | None = int(row_id) if row_id is not None else None
    return write_date, odoo_id


def run_sync(
    cfg: AppConfig,
    mapping_path: Path,
    mode: str = "incremental",
    batch_size_override: int | None = None,
) -> SyncResult:
    mapping = load_mapping(mapping_path)
    batch_size = batch_size_override or mapping.batch_size or cfg.sync_batch_size

    odoo = OdooXmlRpcClient(cfg.odoo)
    pg = PostgresLoader(cfg.postgres)
    odoo.authenticate()

    rows_extracted = 0
    rows_upserted = 0
    offset = 0
    latest_write_date: datetime | None = None
    latest_odoo_id: int | None = None

    with pg.connect() as conn:
        pg.ensure_metadata_tables(conn)
        run_id = pg.start_run(conn, mapping.name, mode)
        try:
            if mode == "full" and mapping.full_sync_strategy == "truncate":
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {mapping.target_table} RESTART IDENTITY")
                conn.commit()

            state = pg.get_state(conn, mapping.name)
            domain = _build_domain(mode=mode, mapping=mapping, state=state)

            source_fields = sorted(
                set(
                    [f.source for f in mapping.fields]
                    + [mapping.cursor.field, mapping.cursor.tie_breaker]
                )
            )

            while True:
                records = odoo.search_read(
                    model=mapping.odoo_model,
                    domain=domain,
                    fields=source_fields,
                    offset=offset,
                    limit=batch_size,
                    order=mapping.order,
                )
                if not records:
                    break

                mapped_rows = [_map_record(rec, mapping) for rec in records]
                loaded_count = pg.upsert_rows(
                    conn=conn,
                    table=mapping.target_table,
                    rows=mapped_rows,
                    conflict_keys=mapping.conflict_keys,
                )

                rows_extracted += len(records)
                rows_upserted += loaded_count

                for record in records:
                    batch_write_date, batch_odoo_id = _extract_cursor(record, mapping)
                    if batch_write_date is not None:
                        latest_write_date = batch_write_date
                    if batch_odoo_id is not None:
                        latest_odoo_id = batch_odoo_id

                if len(records) < batch_size:
                    break
                offset += len(records)

            if latest_write_date is not None or latest_odoo_id is not None:
                pg.set_state(
                    conn,
                    SyncState(
                        job_name=mapping.name,
                        last_write_date=latest_write_date,
                        last_odoo_id=latest_odoo_id,
                    ),
                )

            pg.finish_run(
                conn=conn,
                run_id=run_id,
                status="success",
                rows_extracted=rows_extracted,
                rows_upserted=rows_upserted,
            )
        except Exception as exc:
            conn.rollback()
            pg.finish_run(
                conn=conn,
                run_id=run_id,
                status="failed",
                rows_extracted=rows_extracted,
                rows_upserted=rows_upserted,
                rows_failed=max(0, rows_extracted - rows_upserted),
                error_message=str(exc),
            )
            raise

    return SyncResult(rows_extracted=rows_extracted, rows_upserted=rows_upserted)
