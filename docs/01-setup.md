# 01 - Setup Guide

## Objective
Set up the APDI Odoo SaaS to PostgreSQL sync service and dashboard.

## Current Supported Jobs
- `sm_sales_raw_data`
- `nonsm_sales_raw_data`
- `store_inv_on_hand`
- `whse_inv_on_hand`
- `nonsm_inv_on_hand`
- `whse_rr_summary`
- `products_list_raw`
- `sku_list` (custom job, no YAML mapping file)

## Prerequisites
- Windows Server/Windows 10+
- Python 3.11+
- PostgreSQL reachable from server
- Odoo SaaS XML-RPC access (`URL`, `DB`, `username`, `API key`)

## Environment Setup
From repo root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and set real values.

Required variables:

```env
ODOO_URL=https://<company>.odoo.com
ODOO_DB=<odoo_db_name>
ODOO_USERNAME=<odoo_user_email>
ODOO_API_KEY=<odoo_api_key>

PGHOST=<postgres_host_or_ip>
PGPORT=5432
PGDATABASE=apdireports
PGUSER=<db_user>
PGPASSWORD=<db_password>
PGSSLMODE=prefer

SYNC_BATCH_SIZE=1000
SYNC_LOG_LEVEL=INFO
SYNC_TIMEZONE=Asia/Singapore
```

## First Sync Commands

```powershell
python -m src.main --job sm_sales_raw_data --mode full
python -m src.main --job sm_sales_raw_data --mode incremental
python -m src.main --job nonsm_sales_raw_data --mode incremental
python -m src.main --job store_inv_on_hand --mode incremental
python -m src.main --job whse_inv_on_hand --mode incremental
python -m src.main --job nonsm_inv_on_hand --mode incremental
python -m src.main --job whse_rr_summary --mode incremental
python -m src.main --job products_list_raw --mode incremental
python -m src.main --job sku_list --mode incremental
```

## Dashboard Setup

```powershell
streamlit run src/reconcile_dashboard.py
```

Dashboard has 3 tabs:
- `Dashboard Overview`
- `Manual Sync`
- `Image Import`

## Metadata Tables
Auto-managed by app:
- `sync_state`
- `sync_runs`

Image workflow table:
- `product_images` (auto-created/auto-upgraded when image import runs)

## Notes
- `celes_masterlist` is no longer in active job scope.
- `--job all` is not supported in current CLI.
