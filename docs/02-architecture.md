# 02 - Architecture

## Objective
Describe the current production architecture for APDI Odoo -> PostgreSQL sync and dashboard operations.

## Core Flow
1. CLI job starts (`src.main`).
2. Config loads from `.env` (`src.config`).
3. Odoo XML-RPC auth + extraction (`src.odoo_client`).
4. Transform/mapping apply (`mappings/*.yaml` + `src.transforms`).
5. PostgreSQL upsert/load (`src.postgres_loader`).
6. Checkpoint/run metrics update (`sync_state`, `sync_runs`).

## Components
- `src/main.py`: job dispatcher and mode handling (`incremental` or `full`)
- `src/odoo_client.py`: XML-RPC authenticate/search_read/execute_kw/write with retry
- `src/postgres_loader.py`: upsert helpers + metadata tables + run tracking
- `src/jobs/sync_sm_sales_raw_data.py`: generic mapping-driven job runner for mapped jobs
- `src/jobs/sync_sku_list.py`: custom SKU merge job
- `src/reconcile_dashboard.py`: Streamlit operational UI
- `src/jobs/batch_import_images.py`: image index + upload engine

## Dashboard Architecture
- `Dashboard Overview`: reconcile Odoo counts/cursors vs local DB
- `Manual Sync`: on-demand job execution
- `Image Import`: index image paths and upload to Odoo product images

## Image Import Architecture
- Local table: `product_images` (`name`, `path`, `created_at`, `synced_at`, `last_error`, plus file metadata columns)
- Index step scans local/UNC path and inserts candidate image records
- Upload step reads pending rows and writes to `product.template.image_1920`
- Matching key (current): `product.template.name`
- Incremental rule: default upload scope is rows where `synced_at IS NULL`
- Parallel upload workers supported

## Key Operational Rules
- Mapped jobs support `full_sync_strategy: truncate` for clean reload
- Incremental sync uses cursor/tie-breaker ordering
- Dashboard and image flow are designed for operator-led execution on local network
