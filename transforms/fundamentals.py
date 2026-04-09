from __future__ import annotations

from datetime import date, datetime
import re
from typing import Any, Dict, List, Optional

from transforms.lseg_fields import fundamentals_statement_config


STATEMENT_CONFIG = fundamentals_statement_config()


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"", "none", "null", "nan", "nat"}
    return False


def _to_scalar(value: Any) -> Any:
    if _is_missing(value):
        return None
    item = getattr(value, "item", None)
    if callable(item):
        try:
            value = item()
        except Exception:  # noqa: BLE001
            pass
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _to_iso_date(value: Any) -> Any:
    if _is_missing(value):
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    try:
        if "T" in text or " " in text:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return text


def _to_snake_case(value: str) -> str:
    text = re.sub(r"[^0-9A-Za-z]+", "_", value.strip())
    text = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text.lower()


def _metric_column_name(name: Any) -> str:
    if _is_missing(name):
        return ""
    text = str(name).strip()
    leaf = text.split(".")[-1] if "." in text else text
    return _to_snake_case(leaf)


def _infer_statement(entry: Dict[str, Any]) -> str:
    statement = str(entry.get("statement") or "").strip().lower()
    if statement:
        return _to_snake_case(statement)
    if not _is_missing(entry.get("std_balance_sheet_all")):
        return "balance_sheet"
    if not _is_missing(entry.get("std_cash_flow_all")):
        return "cashflow_statement"
    return "income_statement"


def _statement_prefix(statement: str) -> str:
    normalized = _to_snake_case(str(statement or "").strip())
    if normalized == "balance_sheet":
        return "bs"
    if normalized == "cashflow_statement":
        return "cf"
    return "is"


def _extract_statement_value(entry: Dict[str, Any]) -> Any:
    for key in (
        "statement_value",
        "std_income_statement_all",
        "std_balance_sheet_all",
        "std_cash_flow_all",
    ):
        if key in entry and not _is_missing(entry.get(key)):
            return entry.get(key)
    return None


def _pivot_long_form_rows(entries: List[Dict[str, Any]], *, fallback_instrument: str = "") -> List[Dict[str, Any]]:
    buckets: Dict[tuple[str, str], Dict[str, Any]] = {}

    for entry in entries:
        metric_name = _metric_column_name(entry.get("name"))
        if not metric_name:
            continue

        instrument_raw = _to_scalar(entry.get("instrument"))
        instrument = None if _is_missing(instrument_raw) else str(instrument_raw).strip().upper()
        if not instrument:
            instrument = str(fallback_instrument or "").strip().upper()

        statement = _infer_statement(entry)
        statement_prefix = _statement_prefix(statement)
        period_absolute_raw = _to_scalar(entry.get("financial_period_absolute"))
        period_absolute = None if _is_missing(period_absolute_raw) else str(period_absolute_raw)
        period_start_date = _to_iso_date(entry.get("period_start_date"))
        period_end_date = _to_iso_date(entry.get("period_end_date"))
        bucket_key = (instrument or "", period_absolute or "")

        bucket = buckets.setdefault(
            bucket_key,
            {
                "instrument": instrument or None,
                "financial_period_absolute": period_absolute,
                "period_start_date": period_start_date,
                "period_end_date": period_end_date,
            },
        )
        if statement == "income_statement":
            if period_start_date is not None:
                bucket["period_start_date"] = period_start_date
            if period_end_date is not None:
                bucket["period_end_date"] = period_end_date
        else:
            if bucket.get("period_start_date") is None and period_start_date is not None:
                bucket["period_start_date"] = period_start_date
            if bucket.get("period_end_date") is None and period_end_date is not None:
                bucket["period_end_date"] = period_end_date

        metric_value = _to_scalar(_extract_statement_value(entry))
        metric_column = f"{statement_prefix}_{metric_name}"
        if metric_column not in bucket or _is_missing(bucket.get(metric_column)):
            bucket[metric_column] = metric_value

    rows = list(buckets.values())
    rows.sort(
        key=lambda row: (
            str(row.get("instrument") or ""),
            str(row.get("financial_period_absolute") or ""),
            str(row.get("period_end_date") or ""),
            str(row.get("period_start_date") or ""),
        )
    )
    return rows


def prod_fundamentals_data(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    entries = payload.get("data") or []
    if not isinstance(entries, list):
        return []

    normalized_entries = [entry for entry in entries if isinstance(entry, dict)]
    if not normalized_entries:
        return []

    has_long_form = any(
        "name" in entry
        and any(
            key in entry
            for key in (
                "statement_value",
                STATEMENT_CONFIG["income_statement"]["value_key"],
                STATEMENT_CONFIG["balance_sheet"]["value_key"],
                STATEMENT_CONFIG["cashflow_statement"]["value_key"],
            )
        )
        for entry in normalized_entries
    )
    if has_long_form:
        fallback_instrument = str(payload.get("ric") or payload.get("ticker") or "").strip().upper()
        return _pivot_long_form_rows(normalized_entries, fallback_instrument=fallback_instrument)

    flattened: List[Dict[str, Any]] = []
    for entry in normalized_entries:
        record = dict(entry)
        if "period_start_date" in record:
            record["period_start_date"] = _to_iso_date(record.get("period_start_date"))
        if "period_end_date" in record:
            record["period_end_date"] = _to_iso_date(record.get("period_end_date"))
        flattened.append(record)
    return flattened
