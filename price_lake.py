from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from storage_utils import ensure_dir, format_iso_date


PRICE_EOD_COLUMNS = [
    "date",
    "ticker",
    "provider",
    "source_dataset",
    "instrument",
    "granularity",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "adj_open",
    "adj_high",
    "adj_low",
    "adj_close",
    "adj_volume",
    "price_change_1d",
    "price_pct_chg_1d",
    "total_return_1d",
    "volume_change_1d",
    "price_52wk_high",
    "price_52wk_low",
    "price_52wk_high_flag_1d",
    "price_52wk_low_flag_1d",
    "dividend",
    "split_ratio",
    "market_value",
    "company_id",
    "security",
    "figi",
    "composite_figi",
    "share_class_figi",
    "composite_ticker",
    "primary_listing",
    "frequency",
    "source",
    "source_system",
    "universe_key",
    "workflow_id",
    "workflow_run_id",
    "request_id",
    "requested_period",
    "effective_start_date",
    "effective_end_date",
    "source_object_uri",
    "legacy_object_path",
    "legacy_generation",
    "run_id",
    "processed_at",
    "record_hash",
]

_FLOAT_COLUMNS = {
    "open",
    "high",
    "low",
    "close",
    "volume",
    "adj_open",
    "adj_high",
    "adj_low",
    "adj_close",
    "adj_volume",
    "price_change_1d",
    "price_pct_chg_1d",
    "total_return_1d",
    "volume_change_1d",
    "price_52wk_high",
    "price_52wk_low",
    "dividend",
    "split_ratio",
    "market_value",
}
_INT_COLUMNS = set()
_BOOL_COLUMNS = {"price_52wk_high_flag_1d", "price_52wk_low_flag_1d", "primary_listing"}
_DATE_COLUMNS = {"date", "effective_start_date", "effective_end_date"}


def _optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def _to_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    return int(text) if text.isdigit() else None


def _to_bool(value: Any) -> Optional[bool]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off"}:
        return False
    return None


def _to_date(value: Any) -> Optional[date]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(format_iso_date(value))


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _record_hash(row: Dict[str, Any]) -> str:
    payload = {
        key: row.get(key)
        for key in PRICE_EOD_COLUMNS
        if key not in {"record_hash", "processed_at"}
    }
    encoded = json.dumps(payload, default=_json_default, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def canonical_price_eod_rows(
    rows: Iterable[Dict[str, Any]],
    *,
    context: Dict[str, Any],
    processed_at: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    processed_timestamp = processed_at or datetime.now(timezone.utc)
    if processed_timestamp.tzinfo is None:
        processed_timestamp = processed_timestamp.replace(tzinfo=timezone.utc)

    canonical_rows: List[Dict[str, Any]] = []
    for row in rows:
        canonical: Dict[str, Any] = {
            "date": _to_date(_first_non_empty(row.get("date"), context.get("date"))),
            "ticker": _optional_str(_first_non_empty(row.get("ticker"), context.get("ticker"))),
            "provider": _optional_str(_first_non_empty(row.get("provider"), context.get("provider"))),
            "source_dataset": _optional_str(_first_non_empty(row.get("source_dataset"), context.get("source_dataset"))),
            "instrument": _optional_str(
                _first_non_empty(
                    row.get("instrument"),
                    row.get("primary_ric"),
                    row.get("ric"),
                    context.get("primary_ric"),
                    context.get("ric"),
                    row.get("ticker"),
                    context.get("ticker"),
                )
            ),
            "granularity": _optional_str(_first_non_empty(row.get("bar_granularity"), row.get("granularity"), context.get("bar_granularity"))),
            "price_change_1d": _to_float(_first_non_empty(row.get("price_change_1d"), row.get("price_chg"), row.get("price_chg_1d"))),
            "price_52wk_high_flag_1d": _to_bool(_first_non_empty(row.get("price_52wk_high_flag_1d"), row.get("price_52wk_high_flg_1d"))),
            "price_52wk_low_flag_1d": _to_bool(_first_non_empty(row.get("price_52wk_low_flag_1d"), row.get("price_52wk_low_flg_1d"))),
            "source_object_uri": _optional_str(_first_non_empty(row.get("source_object_uri"), row.get("source_uri"), context.get("source_object_uri"), context.get("source_uri"))),
            "run_id": _optional_str(_first_non_empty(row.get("run_id"), context.get("run_id"))),
            "processed_at": processed_timestamp.astimezone(timezone.utc),
        }
        for key in PRICE_EOD_COLUMNS:
            if key in canonical:
                continue
            if key in _DATE_COLUMNS:
                canonical[key] = _to_date(_first_non_empty(row.get(key), context.get(key)))
            elif key in _FLOAT_COLUMNS:
                canonical[key] = _to_float(_first_non_empty(row.get(key), context.get(key)))
            elif key in _INT_COLUMNS:
                canonical[key] = _to_int(_first_non_empty(row.get(key), context.get(key)))
            elif key in _BOOL_COLUMNS:
                canonical[key] = _to_bool(_first_non_empty(row.get(key), context.get(key)))
            else:
                canonical[key] = _optional_str(_first_non_empty(row.get(key), context.get(key)))
        canonical["record_hash"] = _record_hash(canonical)
        canonical_rows.append({key: canonical.get(key) for key in PRICE_EOD_COLUMNS})
    return canonical_rows


def _price_eod_schema():
    import pyarrow as pa

    fields = []
    for column in PRICE_EOD_COLUMNS:
        if column in _DATE_COLUMNS:
            arrow_type = pa.date32()
        elif column == "processed_at":
            arrow_type = pa.timestamp("us", tz="UTC")
        elif column in _FLOAT_COLUMNS:
            arrow_type = pa.float64()
        elif column in _INT_COLUMNS:
            arrow_type = pa.int64()
        elif column in _BOOL_COLUMNS:
            arrow_type = pa.bool_()
        else:
            arrow_type = pa.string()
        fields.append(pa.field(column, arrow_type))
    return pa.schema(fields)


def write_price_eod_parquet(path: Path, rows: Iterable[Dict[str, Any]]) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    ensure_dir(path.parent)
    table = pa.Table.from_pylist(list(rows), schema=_price_eod_schema())
    pq.write_table(table, path, compression="snappy")
    return path
