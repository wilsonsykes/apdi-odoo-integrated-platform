from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from contextlib import redirect_stdout
import io
import re
import sys
import subprocess
import time

import pandas as pd
import streamlit as st

# Ensure project root is importable when launched via `streamlit run src/...`.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.mapping_loader import MappingConfig, load_mapping
from src.odoo_client import OdooXmlRpcClient
from src.postgres_loader import PostgresLoader
from src.jobs.batch_import_images import ensure_image_index_table, index_images_to_db, normalize_input_dir, run_import


PALETTE = {
    "bg_soft": "#F6F1FA",
    "bg_panel": "#EEE5F4",
    "primary": "#8A2BC2",
    "primary_dark": "#6E1FA3",
    "accent": "#E86EDB",
    "ink": "#171421",
    "muted": "#5C5570",
    "ok": "#1E8E5A",
    "warn": "#B06A00",
    "drift": "#B3261E",
}

JOB_LABELS = {
    "products_list_raw": "Products",
    "sku_list": "SKU List",
    "nonsm_inv_on_hand": "Non-SM Stores Inventory",
    "nonsm_sales_raw_data": "Non-SM Stores Sales per Product",
    "sm_sales_raw_data": "SM Sales per Product",
    "store_inv_on_hand": "SM Stores Inventory",
    "whse_inv_on_hand": "Warehouse Inventory",
    "whse_rr_summary": "RR Summary per Product",
}


def _apply_theme() -> None:
    st.markdown(
        f"""
        <style>
          :root {{
            --bg-soft: {PALETTE["bg_soft"]};
            --bg-panel: {PALETTE["bg_panel"]};
            --primary: {PALETTE["primary"]};
            --primary-dark: {PALETTE["primary_dark"]};
            --accent: {PALETTE["accent"]};
            --ink: {PALETTE["ink"]};
            --muted: {PALETTE["muted"]};
          }}
          .stApp {{
            background:
              radial-gradient(circle at 10% 20%, rgba(138,43,194,0.10) 0%, transparent 30%),
              radial-gradient(circle at 90% 10%, rgba(232,110,219,0.14) 0%, transparent 28%),
              linear-gradient(135deg, var(--bg-soft) 0%, #ffffff 48%, var(--bg-panel) 100%);
          }}
          [data-testid="stAppViewContainer"] .main .block-container {{
            max-width: 1220px;
            padding-top: 2rem;
            padding-bottom: 2rem;
          }}
          .hero-title {{
            background: linear-gradient(90deg, var(--primary-dark), var(--accent));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 800;
            letter-spacing: 0.2px;
            margin-bottom: 0.2rem;
          }}
          .hero-sub {{
            color: var(--muted);
            margin-bottom: 1rem;
          }}
          .section-card {{
            background: rgba(255,255,255,0.75);
            border: 1px solid #ddd3ea;
            border-radius: 14px;
            padding: 0.8rem 1rem 0.2rem 1rem;
            box-shadow: 0 6px 22px rgba(71, 38, 107, 0.06);
            margin-bottom: 0.9rem;
          }}
          h1, h2, h3 {{
            color: var(--ink);
            letter-spacing: 0.2px;
          }}
          p, label, .stCaption {{
            color: var(--muted);
          }}
          div[data-testid="stMetric"] {{
            background: rgba(255,255,255,0.80);
            border: 1px solid #ddd3ea;
            border-radius: 12px;
            padding: 0.5rem 0.75rem;
          }}
          [data-testid="stMetricLabel"] {{
            color: var(--muted);
          }}
          [data-testid="stMetricValue"] {{
            color: var(--primary-dark);
          }}
          .stButton > button, .stDownloadButton > button {{
            background: linear-gradient(90deg, var(--primary), var(--accent));
            color: #ffffff !important;
            border: none;
            border-radius: 10px;
            font-weight: 600;
            -webkit-text-fill-color: #ffffff !important;
            text-shadow: 0 1px 1px rgba(0,0,0,0.22);
            box-shadow: 0 8px 20px rgba(138,43,194,0.25);
          }}
          .stButton > button *, .stDownloadButton > button *,
          .stButton > button p, .stButton > button span,
          .stDownloadButton > button p, .stDownloadButton > button span {{
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
            fill: #ffffff !important;
          }}
          .stButton > button:hover, .stDownloadButton > button:hover {{
            filter: brightness(0.97);
            box-shadow: 0 10px 24px rgba(138,43,194,0.30);
          }}
          .stButton > button:focus, .stButton > button:active,
          .stDownloadButton > button:focus, .stDownloadButton > button:active {{
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
          }}
          div[data-baseweb="select"] > div {{
            border-radius: 10px;
            border-color: #c8b5dd !important;
            background: #fff;
          }}
          div[data-baseweb="tag"] {{
            background: rgba(138,43,194,0.12) !important;
            border: 1px solid rgba(138,43,194,0.30) !important;
          }}
          div[data-baseweb="tag"] span {{
            color: var(--primary-dark) !important;
            font-weight: 600;
          }}
          .stAlert {{
            border-radius: 12px;
          }}
          [data-testid="stDataFrame"] {{
            border: 1px solid #d8cce8;
            border-radius: 12px;
            overflow: hidden;
          }}
          [data-testid="stDataFrame"] * {{
            color: #171421 !important;
            -webkit-text-fill-color: #171421 !important;
          }}
          [data-testid="stDataFrame"] [role="columnheader"] {{
            color: #171421 !important;
            font-weight: 700 !important;
          }}
          [data-testid="stDataFrame"] [role="gridcell"] {{
            color: #171421 !important;
          }}
          div[data-testid="stTabs"] [role="tablist"] {{
            gap: 10px;
            background: rgba(255,255,255,0.68);
            border: 1px solid #ddd3ea;
            border-radius: 14px;
            padding: 8px;
            margin-bottom: 10px;
          }}
          div[data-testid="stTabs"] button[role="tab"] {{
            height: 42px;
            border-radius: 11px;
            border: 1px solid #d8cce8;
            background: rgba(255,255,255,0.92);
            color: var(--primary-dark);
            font-weight: 700;
            padding: 0 18px;
          }}
          div[data-testid="stTabs"] button[role="tab"] *,
          div[data-testid="stTabs"] button[role="tab"] p,
          div[data-testid="stTabs"] button[role="tab"] span {{
            color: var(--primary-dark) !important;
            -webkit-text-fill-color: var(--primary-dark) !important;
            fill: var(--primary-dark) !important;
          }}
          div[data-testid="stTabs"] button[role="tab"]:hover {{
            border-color: var(--primary);
            color: var(--primary);
          }}
          div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {{
            background: linear-gradient(90deg, var(--primary), var(--accent));
            color: #fff !important;
            border-color: transparent;
            box-shadow: 0 8px 20px rgba(138,43,194,0.25);
          }}
          div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] *,
          div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] p,
          div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] span {{
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
            fill: #ffffff !important;
            text-shadow: 0 1px 1px rgba(0,0,0,0.18);
          }}
          details {{
            background: rgba(255,255,255,0.75);
            border: 1px solid #ddd3ea;
            border-radius: 12px;
            padding: 0.4rem 0.7rem;
          }}
          .palette-chip {{
            display:inline-block;
            width:18px;
            height:18px;
            border-radius:4px;
            margin-right:8px;
            border:1px solid #d7d1e2;
            vertical-align:middle;
          }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_palette_group() -> None:
    st.markdown("**Palette Group (Reference Theme)**")
    palette_rows = [
        ("Soft Background", PALETTE["bg_soft"]),
        ("Panel Lilac", PALETTE["bg_panel"]),
        ("Primary Purple", PALETTE["primary"]),
        ("Deep Purple", PALETTE["primary_dark"]),
        ("Accent Magenta", PALETTE["accent"]),
        ("Ink", PALETTE["ink"]),
        ("Muted Text", PALETTE["muted"]),
    ]
    for label, hex_code in palette_rows:
        st.markdown(
            f"<span class='palette-chip' style='background:{hex_code}'></span> {label}: `{hex_code}`",
            unsafe_allow_html=True,
        )


def _parse_odoo_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace(" ", "T"))
    return None


def _safe_minutes_diff(a: datetime | None, b: datetime | None) -> float | None:
    if not a or not b:
        return None
    return round((a - b).total_seconds() / 60.0, 2)


def _status(odoo_count: int | None, target_count: int | None, lag_minutes: float | None) -> str:
    if odoo_count is None or target_count is None:
        return "WARN"
    count_diff = abs((target_count or 0) - (odoo_count or 0))
    if count_diff == 0 and (lag_minutes is None or lag_minutes <= 5):
        return "OK"
    if count_diff <= 10 and (lag_minutes is None or lag_minutes <= 30):
        return "WARN"
    return "DRIFT"


def _load_mappings() -> list[tuple[str, Path, MappingConfig]]:
    mapping_dir = Path("mappings")
    mappings: list[tuple[str, Path, MappingConfig]] = []
    for p in sorted(mapping_dir.glob("*.yaml")):
        cfg = load_mapping(p)
        mappings.append((cfg.name, p, cfg))
    return mappings


def _fetch_latest_cursor_from_odoo(odoo: OdooXmlRpcClient, mapping: MappingConfig, cfg: Any) -> datetime | None:
    rows = odoo.search_read(
        model=mapping.odoo_model,
        domain=mapping.domain or [],
        fields=[mapping.cursor.field, mapping.cursor.tie_breaker],
        offset=0,
        limit=1,
        order=f"{mapping.cursor.field} desc, {mapping.cursor.tie_breaker} desc",
    )
    if not rows:
        return None
    return _parse_odoo_dt(rows[0].get(mapping.cursor.field))


def _fetch_odoo_count(odoo: OdooXmlRpcClient, mapping: MappingConfig, cfg: Any) -> int:
    return odoo._models.execute_kw(
        cfg.odoo.db,
        odoo.uid,
        cfg.odoo.api_key,
        mapping.odoo_model,
        "search_count",
        [mapping.domain or []],
    )


def _fetch_target_count(pg: PostgresLoader, table: str) -> int:
    with pg.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            return int(cur.fetchone()[0])


def _fetch_sync_state(pg: PostgresLoader, job_name: str) -> datetime | None:
    with pg.connect() as conn:
        state = pg.get_state(conn, job_name)
        return state.last_write_date


def _fetch_last_success(pg: PostgresLoader, job_name: str) -> tuple[datetime | None, int | None, int | None]:
    with pg.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ended_at, rows_extracted, rows_upserted
                FROM sync_runs
                WHERE job_name = %s AND status = 'success'
                ORDER BY ended_at DESC NULLS LAST
                LIMIT 1
                """,
                (job_name,),
            )
            row = cur.fetchone()
            if not row:
                return None, None, None
            return row[0], row[1], row[2]


def build_dataframe() -> pd.DataFrame:
    cfg = load_config()
    pg = PostgresLoader(cfg.postgres)
    with pg.connect() as conn:
        pg.ensure_metadata_tables(conn)
    odoo = OdooXmlRpcClient(cfg.odoo)
    odoo.authenticate()

    rows: list[dict[str, Any]] = []
    mappings = _load_mappings()

    for job_name, _, mapping in mappings:
        try:
            odoo_count = _fetch_odoo_count(odoo, mapping, cfg)
        except Exception:
            odoo_count = None

        try:
            odoo_latest = _fetch_latest_cursor_from_odoo(odoo, mapping, cfg)
        except Exception:
            odoo_latest = None

        try:
            target_count = _fetch_target_count(pg, mapping.target_table)
        except Exception:
            target_count = None

        state_cursor = _fetch_sync_state(pg, job_name)
        last_success_at, last_extracted, last_upserted = _fetch_last_success(pg, job_name)

        lag_minutes = _safe_minutes_diff(odoo_latest, state_cursor)
        count_diff = None if (odoo_count is None or target_count is None) else target_count - odoo_count

        rows.append(
            {
                "display_name": JOB_LABELS.get(mapping.target_table, mapping.name),
                "job": job_name,
                "target_table": mapping.target_table,
                "odoo_model": mapping.odoo_model,
                "odoo_count": odoo_count,
                "target_count": target_count,
                "count_diff_target_minus_odoo": count_diff,
                "odoo_latest_cursor": odoo_latest,
                "sync_state_cursor": state_cursor,
                "lag_minutes": lag_minutes,
                "last_success_at": last_success_at,
                "last_success_rows_extracted": last_extracted,
                "last_success_rows_upserted": last_upserted,
                "status": _status(odoo_count, target_count, lag_minutes),
            }
        )

    # sku_list custom check (not mapping-driven)
    try:
        oh_count = odoo._models.execute_kw(
            cfg.odoo.db, odoo.uid, cfg.odoo.api_key, "product.pricelist.item", "search_count", [[["pricelist_id", "=", 21]]]
        )
        smh_count = odoo._models.execute_kw(
            cfg.odoo.db, odoo.uid, cfg.odoo.api_key, "product.pricelist.item", "search_count", [[["pricelist_id", "=", 18]]]
        )
        odoo_count = oh_count + smh_count
    except Exception:
        odoo_count = None

    try:
        target_count = _fetch_target_count(pg, "sku_list")
    except Exception:
        target_count = None

    last_success_at, last_extracted, last_upserted = _fetch_last_success(pg, "sku_list")
    count_diff = None if (odoo_count is None or target_count is None) else target_count - odoo_count
    rows.append(
        {
            "display_name": JOB_LABELS.get("sku_list", "sku_list"),
            "job": "sku_list",
            "target_table": "sku_list",
            "odoo_model": "product.pricelist.item (OH+SMH merged)",
            "odoo_count": odoo_count,
            "target_count": target_count,
            "count_diff_target_minus_odoo": count_diff,
            "odoo_latest_cursor": None,
            "sync_state_cursor": None,
            "lag_minutes": None,
            "last_success_at": last_success_at,
            "last_success_rows_extracted": last_extracted,
            "last_success_rows_upserted": last_upserted,
            "status": "INFO",
        }
    )

    df = pd.DataFrame(rows).sort_values(["status", "job"], ascending=[True, True]).reset_index(drop=True)
    return df


def _run_sync_job(job_name: str, mode: str, batch_size: int) -> tuple[int, str, str, float]:
    cmd = [
        sys.executable,
        "-m",
        "src.main",
        "--job",
        job_name,
        "--mode",
        mode,
        "--batch-size",
        str(batch_size),
    ]
    start = time.time()
    proc = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=60 * 60,
    )
    duration = round(time.time() - start, 2)
    return proc.returncode, proc.stdout, proc.stderr, duration


def _validate_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid table name: {name!r}")
    return name


def _fetch_image_table_stats(table_name: str) -> tuple[int, datetime | None]:
    table = _validate_identifier(table_name)
    cfg = load_config()
    pg = PostgresLoader(cfg.postgres)
    ensure_image_index_table(pg, table)
    with pg.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*), MAX(created_at) FROM {table}")
            row = cur.fetchone()
    return int(row[0] or 0), row[1]


def _fetch_image_table_preview(table_name: str, limit: int = 20) -> pd.DataFrame:
    table = _validate_identifier(table_name)
    cfg = load_config()
    pg = PostgresLoader(cfg.postgres)
    ensure_image_index_table(pg, table)
    with pg.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT name, path, created_at, synced_at, last_error
                FROM {table}
                ORDER BY COALESCE(synced_at, created_at) DESC NULLS LAST, created_at DESC NULLS LAST
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["name", "path", "created_at", "synced_at", "last_error"])


def _fetch_local_product_name_set(table_name: str, column_name: str) -> set[str]:
    table = _validate_identifier(table_name)
    column = _validate_identifier(column_name)
    cfg = load_config()
    pg = PostgresLoader(cfg.postgres)
    values: set[str] = set()
    with pg.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {column} FROM {table} WHERE {column} IS NOT NULL")
            for (value,) in cur.fetchall():
                text = str(value or "").strip()
                if text:
                    values.add(text)
    return values


def _fetch_odoo_missing_image_keys(
    model: str = "product.template",
    key_field: str = "name",
    image_field: str = "image_1920",
) -> set[str]:
    cfg = load_config()
    odoo = OdooXmlRpcClient(cfg.odoo)
    odoo.authenticate()
    keys: set[str] = set()
    offset = 0
    page_size = 1000
    while True:
        rows = odoo.search_read(
            model=model,
            domain=[[image_field, "=", False]],
            fields=[key_field],
            offset=offset,
            limit=page_size,
            order="id asc",
        )
        if not rows:
            break
        for row in rows:
            text = str(row.get(key_field) or "").strip()
            if text:
                keys.add(text)
        offset += len(rows)
    return keys


def _render_dashboard_page(df: pd.DataFrame) -> None:
    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        if st.button("Refresh Now"):
            st.cache_data.clear()
    with col2:
        status_filter = st.multiselect("Filter Status", ["OK", "WARN", "DRIFT", "INFO"], default=["OK", "WARN", "DRIFT", "INFO"])
    with col3:
        search = st.text_input("Search Job/Table", value="").strip().lower()
    st.markdown("</div>", unsafe_allow_html=True)

    if status_filter:
        df = df[df["status"].isin(status_filter)]
    if search:
        df = df[
            df["job"].str.lower().str.contains(search, na=False)
            | df["display_name"].str.lower().str.contains(search, na=False)
            | df["target_table"].str.lower().str.contains(search, na=False)
            | df["odoo_model"].str.lower().str.contains(search, na=False)
        ]

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.caption(f"Last refreshed: {now}")

    drift_jobs = df[df["status"] == "DRIFT"]["display_name"].tolist()
    if drift_jobs:
        st.warning("Drift detected on: " + ", ".join(drift_jobs))

    view_df = df.copy()
    # Keep a unique technical key column and a user-friendly job label column.
    view_df = view_df.rename(columns={"job": "job_key", "display_name": "job"})
    ordered = ["job", "job_key"] + [c for c in view_df.columns if c not in {"job", "job_key"}]
    view_df = view_df[ordered]
    view_df = view_df.reset_index(drop=True)
    view_df["status"] = view_df["status"].apply(_status_badge)

    csv_bytes = view_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download Current View (CSV)",
        data=csv_bytes,
        file_name=f"reconcile_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )

    styled_df = (
        view_df.style.apply(_style_status, axis=1)
        .set_properties(**{"color": "#171421"})
        .set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#efe4f8"),
                        ("color", "#171421"),
                        ("font-weight", "700"),
                    ],
                }
            ]
        )
    )

    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    st.dataframe(
        styled_df,
        width="stretch",
        height=520,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    st.subheader("Summary")
    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("Total Jobs", len(df))
    s2.metric("OK", int((df["status"] == "OK").sum()))
    s3.metric("WARN", int((df["status"] == "WARN").sum()))
    s4.metric("DRIFT", int((df["status"] == "DRIFT").sum()))
    max_lag = df["lag_minutes"].dropna().max() if "lag_minutes" in df else None
    s5.metric("Max Lag (min)", f"{max_lag:.2f}" if pd.notna(max_lag) else "N/A")

    st.markdown("### Dashboard Guide")
    st.info(
        "This table explains what each field means in simple terms and when you should check it. "
        "Use it to quickly see if sync is healthy or needs action."
    )
    guide_rows = [
        ("Job", "The report name you recognize on screen.", "No action needed."),
        ("job_key", "Internal job code used by the system.", "Only useful for troubleshooting."),
        ("target_table", "The PostgreSQL table where synced data is saved.", "Check this when validating DB output."),
        ("odoo_model", "The Odoo source object where data is pulled from.", "Check this if source mapping is wrong."),
        ("odoo_count", "How many rows currently exist in Odoo for this report.", "Watch if count suddenly drops to zero."),
        ("target_count", "How many rows currently exist in your local PostgreSQL table.", "Compare with Odoo count."),
        ("count_diff_target_minus_odoo", "Difference between PostgreSQL and Odoo row counts.", "Near zero is ideal."),
        ("odoo_latest_cursor", "Latest updated record timestamp seen from Odoo.", "Confirms newest source activity."),
        ("sync_state_cursor", "Latest timestamp that your sync process already saved.", "If older than Odoo, sync is behind."),
        ("lag_minutes", "Delay in minutes between Odoo latest data and local synced data.", "If high, run manual sync."),
        ("last_success_at", "Last time this job finished successfully.", "If stale, investigate scheduler/service."),
        ("last_success_rows_extracted", "Rows pulled from Odoo in the last successful run.", "If always zero, check filters/domain."),
        ("last_success_rows_upserted", "Rows inserted/updated in PostgreSQL in the last successful run.", "If very low, validate mapping."),
        ("status", "Overall health marker: OK, WARN, DRIFT, or INFO.", "Focus on WARN and DRIFT first."),
        ("Mode: incremental", "Fast update mode: only new/changed rows are synced.", "Use for regular scheduled runs."),
        ("Mode: full", "Clean reload mode: target table is cleared then reloaded.", "Use only when resetting/correcting data."),
        ("Batch Size", "How many rows are requested per API call from Odoo.", "Increase carefully for faster sync."),
    ]
    guide_df = pd.DataFrame(guide_rows, columns=["Field", "Simple Meaning", "When To Act"])
    st.dataframe(guide_df, width="stretch", hide_index=True)


def _render_manual_page(df: pd.DataFrame) -> None:
    st.subheader("Manual Sync")
    st.info(
        "Use this page for on-demand syncs. Recommended schedule baseline remains every 15 minutes "
        "incremental for all jobs."
    )

    st.markdown("### Sync Controls")
    label_to_job = {row["display_name"]: row["job"] for _, row in df[["display_name", "job"]].drop_duplicates().iterrows()}
    job_options = sorted(label_to_job.keys())
    m1, m2, m3, m4 = st.columns([2, 1, 1, 2])
    with m1:
        selected_job_label = st.selectbox("Job", job_options, index=0, key="manual_job")
    with m2:
        selected_mode = st.selectbox("Mode", ["incremental", "full"], index=0, key="manual_mode")
    with m3:
        batch_size = st.number_input("Batch Size", min_value=100, max_value=10000, value=1000, step=100, key="manual_batch")
    with m4:
        st.write("")
        st.write("")
        run_clicked = st.button("Run Manual Sync", type="primary")

    if selected_mode == "full":
        st.error(
            "Full mode performs clean sync (truncate + reload) for mapped jobs. "
            "Use only when you intentionally want to replace target table contents."
        )

    if run_clicked:
        selected_job = label_to_job[selected_job_label]
        with st.spinner(f"Running {selected_job} ({selected_mode})..."):
            try:
                rc, out, err, seconds = _run_sync_job(selected_job, selected_mode, int(batch_size))
            except Exception as exc:
                st.error(f"Failed to start job: {exc}")
                return

        if rc == 0:
            st.success(f"Manual sync completed in {seconds}s.")
            st.cache_data.clear()
        else:
            st.error(f"Manual sync failed in {seconds}s (exit code {rc}).")

        st.caption("Command output")
        st.code((out or "").strip() or "<no stdout>")
        if err and err.strip():
            st.caption("Errors")
            st.code(err.strip())


def _render_image_import_page() -> None:
    st.subheader("Product Image Import")
    st.info(
        "Workflow: Step 1 indexes new image files into local PostgreSQL `product_images`. "
        "Step 2/3 uses `product_images.path` to upload images into Odoo `product.template` "
        "matching by Product Name (`name`)."
    )

    c1, c2 = st.columns([3, 2])
    with c1:
        input_dir = st.text_input(
            "Image Folder (local or UNC path)",
            value=r"\\192.168.2.177\Users\Public\Merchandise Pictures",
            key="img_input_dir",
        ).strip()
    with c2:
        db_table = st.text_input("Image Index Table", value="product_images", key="img_db_table").strip()

    c3, c4, c5, c9 = st.columns([2, 1, 1, 1])
    with c3:
        extensions_text = st.text_input(
            "Extensions",
            value=".jpg,.jpeg,.png,.webp,.gif,.bmp",
            key="img_extensions",
        ).strip()
    with c4:
        limit = st.number_input("Upload Limit", min_value=1, max_value=500000, value=20, step=20, key="img_limit")
    with c5:
        workers = st.number_input("Upload Workers", min_value=1, max_value=8, value=4, step=1, key="img_workers")
    with c9:
        run_dry = st.checkbox("Dry Run Only", value=True, key="img_dry")
    only_pending = st.checkbox("Upload only new/changed images (incremental)", value=True, key="img_only_pending")
    detect_changed = st.checkbox(
        "Deep detect modified files (slower, use only when needed)",
        value=False,
        key="img_detect_changed",
    )
    c10, c11 = st.columns([1, 1])
    with c10:
        filter_by_local_products = st.checkbox(
            "Filter by local products_list table",
            value=True,
            key="img_filter_local_products",
        )
    with c11:
        filter_by_odoo_missing = st.checkbox(
            "Filter by Odoo products with empty image_1920",
            value=True,
            key="img_filter_odoo_missing",
        )
    c12, c13 = st.columns([1, 1])
    with c12:
        local_products_table = st.text_input("Local Product Table", value="products_list_raw", key="img_local_products_table").strip()
    with c13:
        local_products_column = st.text_input("Local Product Name Column", value="product", key="img_local_products_column").strip()

    c6, c7, c8 = st.columns([1, 1, 2])
    with c6:
        index_clicked = st.button("Step 1: Index New Images", key="img_index_btn")
    with c7:
        upload_clicked = st.button("Step 2/3: Run Upload Flow", key="img_upload_btn")
    with c8:
        execute_live = st.checkbox("Allow live upload to Odoo (uncheck = dry-run)", value=False, key="img_live_ok")

    exts = {e.strip().lower() for e in extensions_text.split(",") if e.strip()}
    if not exts:
        st.error("Please provide at least one image extension.")
        return

    try:
        total_indexed, latest_indexed_at = _fetch_image_table_stats(db_table)
        st.caption(
            f"Indexed rows in `{db_table}`: {total_indexed}"
            + (f" | Latest indexed at: {latest_indexed_at}" if latest_indexed_at else "")
        )
    except Exception as exc:
        st.error(f"Failed reading image index table: {exc}")
        return

    if index_clicked:
        cfg = load_config()
        folder = normalize_input_dir(input_dir)
        progress = st.progress(0)
        progress_text = st.empty()
        with st.spinner("Indexing image paths into local database..."):
            try:
                local_keys: set[str] | None = None
                missing_keys: set[str] | None = None
                allowed_keys: set[str] | None = None
                if filter_by_local_products:
                    local_keys = _fetch_local_product_name_set(local_products_table, local_products_column)
                if filter_by_odoo_missing:
                    missing_keys = _fetch_odoo_missing_image_keys(model="product.template", key_field="name", image_field="image_1920")
                if local_keys is not None and missing_keys is not None:
                    allowed_keys = local_keys.intersection(missing_keys)
                else:
                    allowed_keys = local_keys if local_keys is not None else missing_keys
                st.caption(
                    "Index scope: "
                    + (f"local table keys={len(local_keys):,} | " if local_keys is not None else "")
                    + (f"odoo missing-image keys={len(missing_keys):,} | " if missing_keys is not None else "")
                    + (f"allowed intersection={len(allowed_keys):,}" if allowed_keys is not None else "no extra key filter")
                )

                def _on_progress(processed: int, total: int, inserted_count: int) -> None:
                    ratio = 0 if total <= 0 else int((processed / total) * 100)
                    ratio = max(0, min(100, ratio))
                    progress.progress(ratio)
                    progress_text.caption(
                        f"Index progress: {processed:,}/{total:,} files scanned | "
                        f"new rows inserted: {inserted_count:,}"
                    )

                inserted = index_images_to_db(
                    cfg=cfg,
                    input_dir=folder,
                    extensions=exts,
                    db_table=db_table,
                    progress_cb=_on_progress,
                    detect_changed=detect_changed,
                    allowed_names=allowed_keys,
                )
                progress.progress(100)
                st.success(f"Index complete. New rows inserted: {inserted}")
            except Exception as exc:
                st.error(f"Indexing failed: {exc}")

    if upload_clicked:
        cfg = load_config()
        folder = normalize_input_dir(input_dir)
        do_dry_run = (not execute_live) or bool(run_dry)
        upload_progress = st.progress(0)
        upload_progress_text = st.empty()
        with st.spinner("Running image upload flow using product_images table..."):
            try:
                local_keys: set[str] | None = None
                missing_keys: set[str] | None = None
                allowed_keys: set[str] | None = None
                if filter_by_local_products:
                    local_keys = _fetch_local_product_name_set(local_products_table, local_products_column)
                if filter_by_odoo_missing:
                    missing_keys = _fetch_odoo_missing_image_keys(model="product.template", key_field="name", image_field="image_1920")
                if local_keys is not None and missing_keys is not None:
                    allowed_keys = local_keys.intersection(missing_keys)
                else:
                    allowed_keys = local_keys if local_keys is not None else missing_keys
                st.caption(
                    "Upload scope: "
                    + (f"local table keys={len(local_keys):,} | " if local_keys is not None else "")
                    + (f"odoo missing-image keys={len(missing_keys):,} | " if missing_keys is not None else "")
                    + (f"allowed intersection={len(allowed_keys):,}" if allowed_keys is not None else "no extra key filter")
                )

                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    def _on_upload_progress(
                        processed: int,
                        total: int,
                        uploaded_count: int,
                        missing_count: int,
                        failed_count: int,
                    ) -> None:
                        ratio = 0 if total <= 0 else int((processed / total) * 100)
                        ratio = max(0, min(100, ratio))
                        upload_progress.progress(ratio)
                        upload_progress_text.caption(
                            f"Upload progress: {processed:,}/{total:,} | "
                            f"uploaded: {uploaded_count:,} | missing: {missing_count:,} | failed: {failed_count:,}"
                        )

                    result = run_import(
                        cfg=cfg,
                        input_dir=folder,
                        model="product.template",
                        key_field="name",
                        image_field="image_1920",
                        extensions=exts,
                        mapping_csv=None,
                        limit=int(limit),
                        dry_run=do_dry_run,
                        index_to_db=False,
                        from_db_table=True,
                        db_table=db_table,
                        only_pending=only_pending,
                        max_workers=int(workers),
                        progress_cb=_on_upload_progress,
                        allowed_keys=allowed_keys,
                    )
                log_text = buffer.getvalue().strip()
                upload_progress.progress(100)
                mode_label = "Dry-run validation" if do_dry_run else "Live upload"
                st.success(
                    f"{mode_label} finished: scanned={result.scanned}, "
                    f"uploaded={result.uploaded}, missing_target={result.missing_target}, failed={result.failed}"
                )
                st.caption("Execution output")
                st.code(log_text or "<no output>")
            except Exception as exc:
                st.error(f"Upload flow failed: {exc}")

    st.markdown("### Image Index Preview")
    try:
        preview_df = _fetch_image_table_preview(db_table, limit=20)
        if preview_df.empty:
            st.warning("No indexed images yet. Run Step 1 first.")
        else:
            st.dataframe(preview_df, width="stretch", hide_index=True)
    except Exception as exc:
        st.error(f"Failed to load preview: {exc}")

def _status_badge(status: str) -> str:
    if status == "OK":
        return "OK"
    if status == "WARN":
        return "WARN"
    if status == "DRIFT":
        return "DRIFT"
    return "INFO"
def _style_status(row: pd.Series) -> list[str]:
    status = row.get("status", "")
    if status == "OK":
        color = "#e8f5e9"
    elif status == "WARN":
        color = "#fff8e1"
    elif status == "DRIFT":
        color = "#ffebee"
    else:
        color = "#e3f2fd"
    return [f"background-color: {color}; color: #171421;"] * len(row)


def main() -> None:
    title = "APDI Odoo Integration Tools Platform"
    st.set_page_config(page_title=title, layout="wide")
    _apply_theme()
    st.markdown(f"<h1 class='hero-title'>{title}</h1>", unsafe_allow_html=True)
    st.markdown(
        "<div class='hero-sub'>Compares source counts/cursors in Odoo XML-RPC with local PostgreSQL sync tables.</div>",
        unsafe_allow_html=True,
    )

    @st.cache_data(ttl=60)
    def _cached_df() -> pd.DataFrame:
        return build_dataframe()

    df = _cached_df()
    tab_dashboard, tab_manual, tab_images = st.tabs(["Dashboard Overview", "Manual Sync", "Image Import"])
    with tab_dashboard:
        _render_dashboard_page(df)
    with tab_manual:
        _render_manual_page(df)
    with tab_images:
        _render_image_import_page()


if __name__ == "__main__":
    main()

