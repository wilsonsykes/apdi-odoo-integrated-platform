from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from src.config import AppConfig, load_config
from src.odoo_client import OdooXmlRpcClient
from src.postgres_loader import PostgresLoader


@dataclass(frozen=True)
class ImageImportResult:
    scanned: int
    uploaded: int
    missing_target: int
    failed: int


def normalize_input_dir(raw_path: str) -> Path:
    value = (raw_path or "").strip()
    if not value:
        raise ValueError("input-dir is empty")
    # Accept common UNC variants typed in UI: //server/share or \server\share
    if value.startswith("//"):
        value = "\\\\" + value.lstrip("/").replace("/", "\\")
    elif value.startswith("\\") and not value.startswith("\\\\"):
        value = "\\" + value
    return Path(value)


def index_images_to_db(
    cfg: AppConfig,
    input_dir: Path,
    extensions: set[str],
    db_table: str = "product_images",
    progress_cb: Callable[[int, int, int], None] | None = None,
    detect_changed: bool = False,
    allowed_names: set[str] | None = None,
) -> int:
    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"input-dir not found or not a folder: {input_dir}")
    loader = PostgresLoader(cfg.postgres)
    return _index_folder_to_db(
        loader=loader,
        table_name=db_table,
        input_dir=input_dir,
        extensions=extensions,
        progress_cb=progress_cb,
        detect_changed=detect_changed,
        allowed_names=allowed_names,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch import local images into Odoo image field (XML-RPC)."
    )
    parser.add_argument("--input-dir", required=True, help="Folder containing image files.")
    parser.add_argument(
        "--mapping-csv",
        default=None,
        help="Optional CSV with columns: filename,key. If omitted, filename stem is used as key.",
    )
    parser.add_argument(
        "--model",
        default="product.template",
        help="Target Odoo model (default: product.template).",
    )
    parser.add_argument(
        "--key-field",
        default="default_code",
        help="Field used to find record by key (default: default_code).",
    )
    parser.add_argument(
        "--image-field",
        default="image_1920",
        help="Binary image field to update (default: image_1920).",
    )
    parser.add_argument(
        "--extensions",
        default=".jpg,.jpeg,.png,.webp",
        help="Comma-separated extensions to scan (default: .jpg,.jpeg,.png,.webp).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Optional max files to process (default: 20).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate matches only; do not upload images.",
    )
    parser.add_argument(
        "--index-to-db",
        action="store_true",
        help="Index input-dir into product_images(name,path) before upload (legacy flow).",
    )
    parser.add_argument(
        "--from-db-table",
        action="store_true",
        help="Use product_images(name,path) rows as upload source instead of folder scan.",
    )
    parser.add_argument(
        "--db-table",
        default="product_images",
        help="Table used by legacy image index flow (default: product_images).",
    )
    parser.add_argument(
        "--all-rows",
        action="store_true",
        help="Upload all indexed rows from DB table (default is incremental pending only).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Parallel upload workers (default: 4).",
    )
    return parser.parse_args()


def _load_mapping_csv(path: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"filename", "key"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError("mapping-csv must contain columns: filename,key")
        for row in reader:
            filename = (row.get("filename") or "").strip()
            key = (row.get("key") or "").strip()
            if filename and key:
                mapping[filename.lower()] = key
    return mapping


def _resolve_key(file_path: Path, mapping: dict[str, str] | None) -> str:
    if mapping is None:
        return file_path.stem.strip()
    return mapping.get(file_path.name.lower(), "").strip()


def _validate_table_name(table_name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
        raise ValueError(f"Invalid table name: {table_name!r}")
    return table_name


def ensure_image_index_table(loader: PostgresLoader, table_name: str) -> None:
    table_name = _validate_table_name(table_name)
    with loader.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    name text NOT NULL,
                    path text NOT NULL,
                    created_at timestamp NOT NULL DEFAULT now(),
                    PRIMARY KEY (path)
                )
                """
            )
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS created_at timestamp NOT NULL DEFAULT now()")
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS file_mtime timestamp NULL")
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS file_size bigint NULL")
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS content_sha1 text NULL")
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS synced_at timestamp NULL")
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS last_error text NULL")
        conn.commit()


def _file_sha1(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _index_folder_to_db(
    loader: PostgresLoader,
    table_name: str,
    input_dir: Path,
    extensions: set[str],
    progress_cb: Callable[[int, int, int], None] | None = None,
    detect_changed: bool = False,
    allowed_names: set[str] | None = None,
) -> int:
    table_name = _validate_table_name(table_name)
    ensure_image_index_table(loader, table_name)
    inserted = 0
    allowed_keys = {x.strip() for x in allowed_names} if allowed_names else None
    candidates: list[Path] = []
    for root, _, files in os.walk(input_dir):
        for filename in files:
            p = Path(root) / filename
            if p.suffix.lower() in extensions:
                if allowed_keys is not None and p.stem.strip() not in allowed_keys:
                    continue
                candidates.append(p)
    total = len(candidates)
    if progress_cb:
        progress_cb(0, total, inserted)
    with loader.connect() as conn:
        with conn.cursor() as cur:
            existing: dict[str, tuple[datetime | None, int | None, str | None]] = {}
            if detect_changed:
                cur.execute(f"SELECT path, file_mtime, file_size, content_sha1 FROM {table_name}")
                existing = {
                    str(path): (file_mtime, int(file_size) if file_size is not None else None, content_sha1)
                    for path, file_mtime, file_size, content_sha1 in cur.fetchall()
                }
            for i, p in enumerate(candidates, start=1):
                name = p.stem.strip()
                path = str(p).replace("\\", "/")
                if not detect_changed:
                    cur.execute(
                        f"""
                        INSERT INTO {table_name} (name, path, created_at, synced_at, last_error)
                        VALUES (%s, %s, now(), NULL, NULL)
                        ON CONFLICT (path) DO NOTHING
                        """,
                        (name, path),
                    )
                    if cur.rowcount > 0:
                        inserted += 1
                    if progress_cb and (i % 100 == 0 or i == total):
                        progress_cb(i, total, inserted)
                    continue

                stat = p.stat()
                mtime = datetime.fromtimestamp(stat.st_mtime)
                size = int(stat.st_size)
                old = existing.get(path)

                # Fast path: unchanged file metadata, no DB write and no hashing.
                if old and old[0] == mtime and old[1] == size:
                    if progress_cb and (i % 100 == 0 or i == total):
                        progress_cb(i, total, inserted)
                    continue

                # Hash only new/changed files.
                sha1 = _file_sha1(p)
                cur.execute(
                    f"""
                    INSERT INTO {table_name} (name, path, file_mtime, file_size, content_sha1, created_at, synced_at, last_error)
                    VALUES (%s, %s, %s, %s, %s, now(), NULL, NULL)
                    ON CONFLICT (path) DO NOTHING
                    """,
                    (name, path, mtime, size, sha1),
                )
                if cur.rowcount > 0:
                    inserted += 1
                    if progress_cb and (i % 100 == 0 or i == total):
                        progress_cb(i, total, inserted)
                    continue

                # Existing row: update metadata and reset sync flag only if file changed.
                cur.execute(
                    f"""
                    UPDATE {table_name}
                    SET
                        name = %s,
                        file_mtime = %s,
                        file_size = %s,
                        content_sha1 = %s,
                        synced_at = CASE WHEN content_sha1 IS DISTINCT FROM %s THEN NULL ELSE synced_at END,
                        last_error = CASE WHEN content_sha1 IS DISTINCT FROM %s THEN NULL ELSE last_error END
                    WHERE path = %s
                    """,
                    (name, mtime, size, sha1, sha1, sha1, path),
                )
                if progress_cb and (i % 100 == 0 or i == total):
                    progress_cb(i, total, inserted)
        conn.commit()
    if progress_cb:
        progress_cb(total, total, inserted)
    return inserted


def _load_files_from_db(loader: PostgresLoader, table_name: str, only_pending: bool) -> list[tuple[str, Path]]:
    table_name = _validate_table_name(table_name)
    ensure_image_index_table(loader, table_name)
    rows: list[tuple[str, Path]] = []
    with loader.connect() as conn:
        with conn.cursor() as cur:
            if only_pending:
                cur.execute(f"SELECT name, path FROM {table_name} WHERE synced_at IS NULL ORDER BY name ASC")
            else:
                cur.execute(f"SELECT name, path FROM {table_name} ORDER BY name ASC")
            for name, path in cur.fetchall():
                rows.append((str(name or "").strip(), Path(str(path))))
    return rows


def _mark_db_upload_result(
    loader: PostgresLoader,
    table_name: str,
    path: Path,
    success: bool,
    error: str | None = None,
) -> None:
    table_name = _validate_table_name(table_name)
    with loader.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {table_name}
                SET
                    synced_at = CASE WHEN %s THEN now() ELSE synced_at END,
                    last_error = %s
                WHERE path = %s
                """,
                (success, error, str(path).replace("\\", "/")),
            )
        conn.commit()


def _find_target_record(
    odoo: OdooXmlRpcClient,
    model: str,
    key_field: str,
    key_value: str,
) -> dict[str, Any] | None:
    rows = odoo.execute_kw(
        model=model,
        method="search_read",
        args=[[(key_field, "=", key_value)]],
        kwargs={"fields": ["id", "name", key_field], "limit": 2, "order": "id asc"},
    )
    if not rows:
        return None
    if len(rows) > 1:
        raise RuntimeError(
            f"Multiple records matched {model}.{key_field}={key_value!r}. "
            "Use a unique key or provide tighter mapping."
        )
    return rows[0]


def run_import(
    cfg: AppConfig,
    input_dir: Path,
    model: str,
    key_field: str,
    image_field: str,
    extensions: set[str],
    mapping_csv: Path | None = None,
    limit: int = 0,
    dry_run: bool = False,
    index_to_db: bool = False,
    from_db_table: bool = False,
    db_table: str = "product_images",
    only_pending: bool = True,
    max_workers: int = 4,
    progress_cb: Callable[[int, int, int, int, int], None] | None = None,
    allowed_keys: set[str] | None = None,
) -> ImageImportResult:
    # Folder is required for local scan/index modes, not required when reading from DB table only.
    if (index_to_db or not from_db_table) and (not input_dir.exists() or not input_dir.is_dir()):
        raise FileNotFoundError(f"input-dir not found or not a folder: {input_dir}")

    mapping = _load_mapping_csv(mapping_csv) if mapping_csv else None
    loader = PostgresLoader(cfg.postgres)
    ensure_image_index_table(loader=loader, table_name=db_table)

    if index_to_db:
        inserted = _index_folder_to_db(loader=loader, table_name=db_table, input_dir=input_dir, extensions=extensions)
        print(f"Indexed to {db_table}: inserted={inserted}")

    file_items: list[tuple[str | None, Path]] = []
    if from_db_table:
        for name, path in _load_files_from_db(loader=loader, table_name=db_table, only_pending=only_pending):
            file_items.append((name, path))
    else:
        for p in sorted(input_dir.iterdir()):
            if p.is_file() and p.suffix.lower() in extensions:
                file_items.append((None, p))

    key_filter = {x.strip() for x in allowed_keys} if allowed_keys else None
    if key_filter is not None:
        filtered_items: list[tuple[str | None, Path]] = []
        for db_name, file_path in file_items:
            key = db_name.strip() if (from_db_table and db_name) else _resolve_key(file_path, mapping)
            if key in key_filter:
                filtered_items.append((db_name, file_path))
        file_items = filtered_items

    if limit > 0:
        file_items = file_items[:limit]

    max_workers = max(1, int(max_workers))

    uploaded = 0
    missing_target = 0
    failed = 0
    processed = 0
    total = len(file_items)
    if progress_cb:
        progress_cb(processed, total, uploaded, missing_target, failed)

    thread_local: threading.local = threading.local()

    def _get_client() -> OdooXmlRpcClient:
        client = getattr(thread_local, "odoo_client", None)
        if client is None:
            client = OdooXmlRpcClient(cfg.odoo)
            client.authenticate()
            thread_local.odoo_client = client
        return client

    def _process_one(idx: int, db_name: str | None, file_path: Path) -> dict[str, Any]:
        if not file_path.exists():
            return {
                "status": "missing",
                "path": file_path,
                "message": f"[{idx}/{total}] SKIP {file_path.name}: file not found",
                "db_mark": (False, "File not found"),
            }

        key = db_name.strip() if (from_db_table and db_name) else _resolve_key(file_path, mapping)
        if not key:
            return {
                "status": "missing",
                "path": file_path,
                "message": f"[{idx}/{total}] SKIP {file_path.name}: no key mapping found",
                "db_mark": (False, "No key mapping found"),
            }

        try:
            client = _get_client()
            target = _find_target_record(odoo=client, model=model, key_field=key_field, key_value=key)
            if target is None:
                return {
                    "status": "missing",
                    "path": file_path,
                    "message": f"[{idx}/{total}] MISS {file_path.name}: no {model} record for {key_field}={key!r}",
                    "db_mark": (False, "No target record found"),
                }
            target_id = int(target["id"])
            target_name = str(target.get("name") or "")
            target_key = str(target.get(key_field) or "")

            if dry_run:
                return {
                    "status": "uploaded",
                    "path": file_path,
                    "message": (
                        f"[{idx}/{total}] OK(dry-run) {file_path.name} -> "
                        f"{model}(id={target_id}, name={target_name!r}, {key_field}={target_key!r})"
                    ),
                    "db_mark": None,
                }

            encoded = base64.b64encode(file_path.read_bytes()).decode("ascii")
            ok = client.write(model=model, record_ids=[target_id], values={image_field: encoded})
            if not ok:
                return {
                    "status": "failed",
                    "path": file_path,
                    "message": f"[{idx}/{total}] FAIL {file_path.name}: Odoo write returned False",
                    "db_mark": (False, "Odoo write returned False"),
                }

            return {
                "status": "uploaded",
                "path": file_path,
                "message": (
                    f"[{idx}/{total}] UPLOADED {file_path.name} -> "
                    f"{model}(id={target_id}, name={target_name!r}, {key_field}={target_key!r})"
                ),
                "db_mark": (True, None),
            }
        except Exception as exc:
            return {
                "status": "failed",
                "path": file_path,
                "message": f"[{idx}/{total}] ERROR {file_path.name}: {exc}",
                "db_mark": (False, str(exc)),
            }

    def _handle_result(result: dict[str, Any]) -> None:
        nonlocal uploaded, missing_target, failed, processed
        status = str(result.get("status"))
        if status == "uploaded":
            uploaded += 1
        elif status == "missing":
            missing_target += 1
        else:
            failed += 1
        print(str(result.get("message") or ""))
        if from_db_table:
            db_mark = result.get("db_mark")
            if db_mark is not None:
                success, err = db_mark
                _mark_db_upload_result(
                    loader=loader,
                    table_name=db_table,
                    path=result["path"],
                    success=bool(success),
                    error=None if err is None else str(err),
                )
        processed += 1
        if progress_cb:
            progress_cb(processed, total, uploaded, missing_target, failed)

    if max_workers == 1 or total <= 1:
        for idx, (db_name, file_path) in enumerate(file_items, start=1):
            _handle_result(_process_one(idx, db_name, file_path))
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_process_one, idx, db_name, file_path)
                for idx, (db_name, file_path) in enumerate(file_items, start=1)
            ]
            for future in as_completed(futures):
                _handle_result(future.result())

    return ImageImportResult(
        scanned=len(file_items),
        uploaded=uploaded,
        missing_target=missing_target,
        failed=failed,
    )


def main() -> None:
    args = parse_args()
    cfg = load_config()

    exts = {e.strip().lower() for e in args.extensions.split(",") if e.strip()}
    mapping_csv = Path(args.mapping_csv) if args.mapping_csv else None
    result = run_import(
        cfg=cfg,
        input_dir=Path(args.input_dir),
        model=args.model,
        key_field=args.key_field,
        image_field=args.image_field,
        extensions=exts,
        mapping_csv=mapping_csv,
        limit=args.limit,
        dry_run=args.dry_run,
        index_to_db=args.index_to_db,
        from_db_table=args.from_db_table,
        db_table=args.db_table,
        only_pending=not args.all_rows,
        max_workers=args.workers,
    )
    print(
        "Image import finished: "
        f"scanned={result.scanned}, uploaded={result.uploaded}, "
        f"missing_target={result.missing_target}, failed={result.failed}"
    )


if __name__ == "__main__":
    main()
