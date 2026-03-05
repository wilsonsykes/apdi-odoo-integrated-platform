from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.config import AppConfig
from src.odoo_client import OdooXmlRpcClient
from src.postgres_loader import PostgresLoader
from src.transforms import to_numeric


@dataclass(frozen=True)
class SyncResult:
    rows_extracted: int
    rows_upserted: int


def _fetch_pricelist_rows(
    odoo: OdooXmlRpcClient,
    db: str,
    api_key: str,
    pricelist_id: int,
    batch_size: int = 1000,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    while True:
        rows = odoo._models.execute_kw(
            db,
            odoo.uid,
            api_key,
            "product.pricelist.item",
            "search_read",
            [[["pricelist_id", "=", pricelist_id]]],
            {
                "fields": [
                    "id",
                    "product_tmpl_id",
                    "x_studio_product_pricelist_sku",
                    "x_studio_product_pricelist_item_srp",
                ],
                "offset": offset,
                "limit": batch_size,
                "order": "id asc",
            },
        )
        if not rows:
            break
        out.extend(rows)
        if len(rows) < batch_size:
            break
        offset += len(rows)
    return out


def run_sync(cfg: AppConfig, mode: str = "full", batch_size_override: int | None = None) -> SyncResult:
    batch_size = batch_size_override or cfg.sync_batch_size

    odoo = OdooXmlRpcClient(cfg.odoo)
    odoo.authenticate()
    pg = PostgresLoader(cfg.postgres)

    OH_PRICE_ID = 21
    SMH_PRICE_ID = 18

    oh_rows = _fetch_pricelist_rows(odoo, cfg.odoo.db, cfg.odoo.api_key, OH_PRICE_ID, batch_size=batch_size)
    smh_rows = _fetch_pricelist_rows(odoo, cfg.odoo.db, cfg.odoo.api_key, SMH_PRICE_ID, batch_size=batch_size)
    rows_extracted = len(oh_rows) + len(smh_rows)

    merged: dict[int, dict[str, Any]] = {}

    for row in oh_rows:
        tmpl = row.get("product_tmpl_id")
        if not tmpl:
            continue
        tmpl_id, tmpl_name = tmpl[0], tmpl[1]
        rec = merged.setdefault(
            int(tmpl_id),
            {
                "source_odoo_id": int(tmpl_id),
                "product": tmpl_name,
                "oh_sku": None,
                "oh_srp": None,
                "smh_sku": None,
                "smh_srp": None,
            },
        )
        rec["oh_sku"] = row.get("x_studio_product_pricelist_sku")
        rec["oh_srp"] = to_numeric(row.get("x_studio_product_pricelist_item_srp"))

    for row in smh_rows:
        tmpl = row.get("product_tmpl_id")
        if not tmpl:
            continue
        tmpl_id, tmpl_name = tmpl[0], tmpl[1]
        rec = merged.setdefault(
            int(tmpl_id),
            {
                "source_odoo_id": int(tmpl_id),
                "product": tmpl_name,
                "oh_sku": None,
                "oh_srp": None,
                "smh_sku": None,
                "smh_srp": None,
            },
        )
        rec["smh_sku"] = row.get("x_studio_product_pricelist_sku")
        rec["smh_srp"] = to_numeric(row.get("x_studio_product_pricelist_item_srp"))

    rows_to_upsert = list(merged.values())

    with pg.connect() as conn:
        pg.ensure_metadata_tables(conn)
        run_id = pg.start_run(conn, "sku_list", "full")
        try:
            if mode == "full":
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE sku_list")
                conn.commit()

            upserted = pg.upsert_rows(
                conn=conn,
                table="sku_list",
                rows=rows_to_upsert,
                conflict_keys=["product"],
            )
            pg.finish_run(
                conn=conn,
                run_id=run_id,
                status="success",
                rows_extracted=rows_extracted,
                rows_upserted=upserted,
            )
        except Exception as exc:
            conn.rollback()
            pg.finish_run(
                conn=conn,
                run_id=run_id,
                status="failed",
                rows_extracted=rows_extracted,
                rows_upserted=0,
                rows_failed=rows_extracted,
                error_message=str(exc),
            )
            raise

    return SyncResult(rows_extracted=rows_extracted, rows_upserted=len(rows_to_upsert))
