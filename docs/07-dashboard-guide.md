# 07 - Dashboard User Guide

## Title
APDI Odoo to Postgres Database Dashboard for APDI Reporting System

## Pages
- `Dashboard Overview`
- `Manual Sync`
- `Image Import`

## Dashboard Overview
Shows:
- row count comparison (Odoo vs PostgreSQL)
- cursor lag minutes
- latest successful run stats
- status per job (`OK`, `WARN`, `DRIFT`, `INFO`)

## Manual Sync
Use to run a job immediately:
- choose job
- choose mode (`incremental` or `full`)
- set batch size
- run and check output/error panel

## Image Import
Use for product image updates:
1. index image files to local `product_images`
2. upload pending records to Odoo `product.template.image_1920`

Recommended day-to-day:
- keep incremental checkbox enabled
- run dry-run first
- then run live upload

## Troubleshooting
- If UI freezes during long operation, restart Streamlit and refresh browser.
- If no upload happens, verify key match (`product.template.name` vs filename stem).
