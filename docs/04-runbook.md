# 04 - Operations Runbook

## Standard Sync Commands

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

## Dashboard Operations
Run dashboard:

```powershell
streamlit run src/reconcile_dashboard.py
```

Tabs:
- `Dashboard Overview`: reconcile status, counts, lag
- `Manual Sync`: run one job on demand
- `Image Import`: image index/upload operations

## Schedule Baseline
- Sales jobs: every 15 minutes
- Inventory jobs: every 15 to 30 minutes
- Product/SKU jobs: every 1 to 6 hours
- Image import: operator-triggered (not automatic by default)

## Image Import Runbook
1. Set image folder UNC path (recommended current path: `\\192.168.2.177\Users\Public\Merchandise Pictures`).
2. Keep `Upload only new/changed images` checked.
3. Click `Step 1: Index New Images`.
4. Run `Step 2/3` with dry-run first.
5. Enable live upload and re-run `Step 2/3`.

## Common Issues
### Dashboard stuck / cannot click
- Cause: long-running Streamlit request
- Action: restart Streamlit process and hard refresh browser

### `No target record found` on image upload
- Cause: key mismatch
- Current key logic is `product.template.name`
- Confirm filename stem matches Odoo product name exactly

### UNC path not reachable
- Use IP UNC path if hostname resolution fails
- Verify SMB reachability and local network policy

## Recovery
- For sync jobs: rerun incremental mode after resolving root cause
- For image uploads: keep incremental mode and rerun pending rows (`synced_at IS NULL`)
