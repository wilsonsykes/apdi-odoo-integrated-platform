from __future__ import annotations

import argparse
from pathlib import Path

from src.config import load_config
from src.jobs.sync_sm_sales_raw_data import run_sync
from src.jobs.sync_sku_list import run_sync as run_sku_sync


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Odoo XML-RPC to Postgres sync runner")
    parser.add_argument("--job", required=True, help="Job name. Currently supported: sm_sales_raw_data")
    parser.add_argument("--mode", default="incremental", choices=["incremental", "full"], help="Sync mode")
    parser.add_argument("--batch-size", type=int, default=None, help="Optional batch size override")
    parser.add_argument(
        "--mapping",
        default=None,
        help="Optional mapping file path override (defaults to mappings/<job>.yaml)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config()

    supported_jobs = {
        "sm_sales_raw_data",
        "nonsm_sales_raw_data",
        "store_inv_on_hand",
        "whse_inv_on_hand",
        "nonsm_inv_on_hand",
        "whse_rr_summary",
        "products_list_raw",
        "sku_list",
    }
    if args.job not in supported_jobs:
        raise ValueError(f"Unsupported job. Implemented job(s): {', '.join(sorted(supported_jobs))}")

    if args.job == "sku_list":
        result = run_sku_sync(
            cfg=cfg,
            mode=args.mode,
            batch_size_override=args.batch_size,
        )
        print(
            f"Sync completed for {args.job}: "
            f"rows_extracted={result.rows_extracted}, rows_upserted={result.rows_upserted}"
        )
        return

    mapping_path = Path(args.mapping) if args.mapping else Path("mappings") / f"{args.job}.yaml"
    if not mapping_path.exists():
        raise FileNotFoundError(f"Mapping file not found: {mapping_path}")

    result = run_sync(
        cfg=cfg,
        mapping_path=mapping_path,
        mode=args.mode,
        batch_size_override=args.batch_size,
    )
    print(
        f"Sync completed for {args.job}: "
        f"rows_extracted={result.rows_extracted}, rows_upserted={result.rows_upserted}"
    )


if __name__ == "__main__":
    main()
