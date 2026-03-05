from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any


def _coerce_scalar(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        if len(value) >= 2:
            return value[1]
        return value[0] if value else None
    return value


def _is_nullish(value: Any, null_if: list[Any] | None) -> bool:
    if value is None:
        return True
    # Odoo XML-RPC frequently returns False for empty fields.
    if value is False:
        return True
    if null_if and value in null_if:
        return True
    if isinstance(value, str) and value.strip().lower() in {"", "none", "null", "nan"}:
        return True
    return False


def trim(value: Any) -> Any:
    value = _coerce_scalar(value)
    return value.strip() if isinstance(value, str) else value


def to_int(value: Any) -> int | None:
    value = _coerce_scalar(value)
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace(",", "").strip()
        if value == "":
            return None
    return int(float(value))


def to_numeric(value: Any) -> Decimal | None:
    value = _coerce_scalar(value)
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace(",", "").strip()
        if value == "":
            return None
    return Decimal(str(value))


def remove_commas_to_numeric(value: Any) -> Decimal | None:
    return to_numeric(value)


def to_date(value: Any) -> date | None:
    value = _coerce_scalar(value)
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        return date.fromisoformat(value[:10])
    raise ValueError(f"Unsupported date value: {value!r}")


def to_datetime(value: Any) -> datetime | None:
    value = _coerce_scalar(value)
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise ValueError(f"Unsupported datetime value: {value!r}")


def upper(value: Any) -> Any:
    value = _coerce_scalar(value)
    return value.upper() if isinstance(value, str) else value


def lower(value: Any) -> Any:
    value = _coerce_scalar(value)
    return value.lower() if isinstance(value, str) else value


def none_if_blank(value: Any) -> Any:
    value = _coerce_scalar(value)
    if isinstance(value, str) and not value.strip():
        return None
    return value


def text_100(value: Any) -> Any:
    value = _coerce_scalar(value)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:100]


TRANSFORM_REGISTRY = {
    "trim": trim,
    "to_int": to_int,
    "to_numeric": to_numeric,
    "to_date": to_date,
    "to_datetime": to_datetime,
    "upper": upper,
    "lower": lower,
    "remove_commas_to_numeric": remove_commas_to_numeric,
    "none_if_blank": none_if_blank,
    "text_100": text_100,
}


def apply_transform(value: Any, transform: str | None, null_if: list[Any] | None = None) -> Any:
    base_value = _coerce_scalar(value)
    if _is_nullish(base_value, null_if):
        return None
    if not transform:
        return base_value
    func = TRANSFORM_REGISTRY.get(transform)
    if func is None:
        raise ValueError(f"Unknown transform: {transform}")
    return func(base_value)
