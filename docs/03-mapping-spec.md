# 03 - Mapping Specification

## Objective
Define mapping behavior for YAML-driven sync jobs and document non-YAML custom jobs.

## Active Mapping Files
- `mappings/sm_sales_raw_data.yaml`
- `mappings/nonsm_sales_raw_data.yaml`
- `mappings/store_inv_on_hand.yaml`
- `mappings/whse_inv_on_hand.yaml`
- `mappings/nonsm_inv_on_hand.yaml`
- `mappings/whse_rr_summary.yaml`
- `mappings/products_list_raw.yaml`

## Custom Job (No YAML)
- `sku_list` is implemented in `src/jobs/sync_sku_list.py`

## YAML Contract
Each mapping contains:
- `name`
- `odoo_model`
- `target_table`
- `full_sync_strategy`
- `batch_size`
- `order`
- `domain`
- `cursor.field`
- `cursor.tie_breaker`
- `conflict_keys`
- `fields[]` (`source`, `target`, `type`, optional `transform`)

## Current Important Mapping Notes
- Sales sync jobs are aligned to `sale.order.line`-based source extraction logic.
- Domain filters include business segmentation rules (SM vs Non-SM) and reference prefix logic where configured.
- `products_list_raw` maps product display fields into reporting table and is used by image-import filtering.

## Key Policy
- Prefer stable technical keys (`source_odoo_id`) for idempotent upserts where applicable.
- Ensure conflict keys match actual uniqueness in target table.

## Validation Checklist (Per Mapping Change)
1. Load 100+ sample rows from Odoo.
2. Verify transform conversion success (no silent bad casts).
3. Run incremental sync.
4. Reconcile source/target counts.
5. Confirm `sync_state` cursor movement.
