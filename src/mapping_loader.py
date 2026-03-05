from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class FieldMapping:
    source: str
    target: str
    type: str = "text"
    transform: str | None = None
    null_if: list[Any] | None = None


@dataclass(frozen=True)
class CursorConfig:
    field: str
    tie_breaker: str = "id"


@dataclass(frozen=True)
class MappingConfig:
    name: str
    odoo_model: str
    target_table: str
    mode: str
    cursor: CursorConfig
    conflict_keys: list[str]
    fields: list[FieldMapping]
    order: str
    domain: list[list[Any]]
    full_sync_strategy: str
    batch_size: int | None = None


def load_mapping(mapping_path: Path) -> MappingConfig:
    payload = yaml.safe_load(mapping_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid mapping file: {mapping_path}")

    cursor_payload = payload.get("cursor", {})
    fields_payload = payload.get("fields", [])

    fields: list[FieldMapping] = []
    for raw in fields_payload:
        fields.append(
            FieldMapping(
                source=raw["source"],
                target=raw["target"],
                type=raw.get("type", "text"),
                transform=raw.get("transform"),
                null_if=raw.get("null_if"),
            )
        )

    return MappingConfig(
        name=payload["name"],
        odoo_model=payload["odoo_model"],
        target_table=payload["target_table"],
        mode=payload.get("mode", "incremental"),
        cursor=CursorConfig(
            field=cursor_payload.get("field", "write_date"),
            tie_breaker=cursor_payload.get("tie_breaker", "id"),
        ),
        conflict_keys=payload.get("conflict_keys", []),
        fields=fields,
        order=payload.get("order", "write_date asc, id asc"),
        domain=payload.get("domain", []),
        full_sync_strategy=payload.get("full_sync_strategy", "upsert"),
        batch_size=payload.get("batch_size"),
    )
