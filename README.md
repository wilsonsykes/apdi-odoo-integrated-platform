# Odoo XML-RPC Sync (Scaffold)

This project bootstraps an automated sync service from Odoo SaaS to local PostgreSQL.

## 1. Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill real values.

## 2. Run first job

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

## 3. Notes

- Metadata tables are auto-created: `sync_state`, `sync_runs`.
- Current mapping files are in `mappings/*.yaml` for all non-custom jobs.
- `source_odoo_id` is used as the upsert key for most tables.
- `sku_list` uses a custom merge job over OH and SMH pricelist items.
- For mapped jobs, `--mode full` uses `full_sync_strategy: truncate` (clean reload) to remove stale rows.
- Adjust Odoo model and source fields in mapping as needed for your tenant.

## 4. Interactive Reconciliation Dashboard

Install dependencies (if not yet installed):

```powershell
pip install -r requirements.txt
```

Run dashboard:

```powershell
streamlit run src/reconcile_dashboard.py
```

What it shows:
- Odoo count vs PostgreSQL count per job/table
- Count difference side-by-side
- Cursor lag (minutes) using Odoo latest cursor vs local `sync_state`
- Last successful run timestamp and row metrics
- Includes `Image Import` page with 2-step workflow:
  1. index new image paths to local `product_images`
  2. upload from `product_images.path` to Odoo `product.template.image_1920`

## 5. Batch Image Import to Odoo

Use this when you need to upload many product images from a local folder into Odoo SaaS.

Default behavior:
- Target model: `product.template`
- Match field: `default_code`
- Image field: `image_1920`
- Key is filename stem (example: `ABC123.jpg` -> key `ABC123`)

Run a dry-run first (no upload, validation only):

```powershell
python -m src.jobs.batch_import_images --input-dir "C:\images\products" --dry-run
```

If validation looks correct, run actual upload:

```powershell
python -m src.jobs.batch_import_images --input-dir "C:\images\products"
```

Optional CSV mapping (for custom filename-to-key mapping):
- CSV columns required: `filename,key`

```powershell
python -m src.jobs.batch_import_images --input-dir "C:\images\products" --mapping-csv "C:\images\map.csv"
```

Optional targeting overrides:

```powershell
python -m src.jobs.batch_import_images `
  --input-dir "C:\images\products" `
  --model product.product `
  --key-field default_code `
  --image-field image_1920
```

Legacy-compatible flow (same idea as old `import_image_path.py`):
1. Index network folder into `product_images(name,path)`.
2. Upload using rows from `product_images`.
3. Incremental behavior is default: only rows with `synced_at IS NULL` are uploaded.

```powershell
python -m src.jobs.batch_import_images `
  --input-dir "\\mpc2\Users\Public\Merchandise Pictures" `
  --index-to-db `
  --dry-run

python -m src.jobs.batch_import_images `
  --input-dir "\\mpc2\Users\Public\Merchandise Pictures" `
  --from-db-table `
  --db-table product_images
```

Force re-upload of all indexed rows (ignore incremental filter):

```powershell
python -m src.jobs.batch_import_images `
  --input-dir "\\mpc2\Users\Public\Merchandise Pictures" `
  --from-db-table `
  --db-table product_images `
  --all-rows
```
