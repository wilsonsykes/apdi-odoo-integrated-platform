# 06 - Testing Strategy

## Scope
Covers sync jobs, dashboard operations, and image import pipeline.

## A. Sync Job Tests
1. Odoo authentication success/failure.
2. Incremental cursor progression (`sync_state`).
3. Full mode clean reload behavior.
4. Upsert idempotency checks.
5. Reconciliation checks (`odoo_count` vs `target_count`).

## B. Dashboard Tests
1. Page load for all tabs.
2. Refresh/filter/search behavior.
3. Manual sync command execution and output capture.
4. Streamlit restart recovery after long-running tasks.

## C. Image Import Tests
1. UNC path reachability test.
2. Index step inserts only intended records.
3. Upload dry-run output correctness.
4. Live upload marks `synced_at` and clears `last_error` on success.
5. Failed uploads set `last_error`.
6. Incremental rerun uploads only pending rows.
7. Filter narrowing works:
   - by local `products_list_raw.product`
   - by Odoo missing `image_1920`
   - by intersection of both
8. Parallel workers run safely (no duplicate updates).

## D. Quick SQL Validation
```sql
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (WHERE synced_at IS NOT NULL) AS synced,
       COUNT(*) FILTER (WHERE synced_at IS NULL) AS pending,
       COUNT(*) FILTER (WHERE last_error IS NOT NULL AND last_error <> '') AS errored
FROM product_images;
```

## E. Release Gate
- Sync and dashboard smoke tests pass.
- Image import dry-run pass.
- At least one live image upload batch verified.
- No critical errors in `sync_runs` after validation window.
