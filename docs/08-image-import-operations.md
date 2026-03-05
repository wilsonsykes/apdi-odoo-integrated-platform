# 08 - Image Import Operations

## Workflow Summary
- Source: network/local image folder
- Index table: `product_images`
- Target: `product.template.image_1920`
- Match key: `product.template.name`

## Step 1 - Index
Purpose:
- register candidate image files in local DB

Behavior:
- adds new `path` entries
- optional deep-change detection for modified files
- supports progress indicator

## Step 2/3 - Upload
Purpose:
- upload indexed image files to Odoo

Behavior:
- default incremental mode: only rows with `synced_at IS NULL`
- supports dry-run and live mode
- supports parallel workers
- writes `last_error` on failures

## Filtering Options
- Filter by local table (`products_list_raw.product`)
- Filter by Odoo missing image (`image_1920 = False`)
- Intersection mode (both filters enabled)

## Key SQL Checks
```sql
SELECT name, path, synced_at, last_error
FROM product_images
ORDER BY COALESCE(synced_at, created_at) DESC;
```

```sql
SELECT COUNT(*) FILTER (WHERE synced_at IS NULL) AS pending
FROM product_images;
```

## Common Failures
- `No target record found`: filename stem does not match `product.template.name`
- UNC path failure: SMB reachability/policy issue
- Slow indexing: run without deep detect for routine operations
