from __future__ import annotations

from datetime import date, datetime
import math
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional

from transforms.lseg_fields import prices_field_to_snake_case


FIELD_TO_SNAKE_CASE: Dict[str, str] = prices_field_to_snake_case()
SNAKE_CASE_FIELDS: List[str] = list(dict.fromkeys(FIELD_TO_SNAKE_CASE.values()))

FIELD_SYNONYMS: Dict[str, List[str]] = {
    "TR.RIC": ["TR.RICCODE", "RIC", "RIC Code"],
    "TR.PrimaryRIC": ["TR.RICCODE", "Primary RIC", "Primary Issue RIC", "RIC", "RIC Code"],
    "TR.TotalReturn52Wk": ["52 Week Total Return"],
    "TR.PriceToMeanPriceTarget": ["Price To Price Target Mean"],
    "TR.FwdPtoEPSSmartEst": ["Price / EPS (SmartEstimate ®)"],
    "TR.PtoEPSMeanEst": ["Price / EPS (Mean Estimate)"],
    "TR.PEGSmart": ["Price / Earnings To Growth Ratio (SmartEstimate ®)"],
    "TR.PtoEBTSmartEst": ["Price / EBITDA (SmartEstimate ®)"],
    "TR.PtoBPSSmartEst": ["Price / Book Value Per Share (SmartEstimate ®)"],
    "TR.PtoCPXSmartEst": ["Price / CAPEX (SmartEstimate ®)"],
}
FIELD_SYNONYMS_LOOKUP: Dict[str, List[str]] = {
    str(key).upper(): list(values) for key, values in FIELD_SYNONYMS.items()
}


def _normalize_column_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def _column_key_parts(column_key: Any) -> List[str]:
    if isinstance(column_key, (tuple, list)):
        parts: List[str] = []
        for item in column_key:
            parts.extend(_column_key_parts(item))
        return parts
    text = str(column_key).strip()
    return [text] if text else []


def _column_matches_candidate(column_key: Any, candidate: str) -> bool:
    candidate_text = str(candidate or "").strip()
    if not candidate_text:
        return False
    variants: List[str] = [candidate_text]
    if candidate_text.upper().startswith("TR."):
        trimmed = candidate_text[3:]
        variants.append(trimmed)
        if "." in trimmed:
            variants.append(trimmed.split(".", 1)[1])
    normalized_variants = {
        _normalize_column_name(value)
        for value in variants
        if _normalize_column_name(value)
    }
    lower_variants = {value.lower() for value in variants if value}
    for part in _column_key_parts(column_key):
        part_variants = {part, re.sub(r"\s*\([^)]*\)\s*$", "", part).strip()}
        if "|" in part:
            left, right = part.split("|", 1)
            part_variants.add(left.strip())
            part_variants.add(right.strip())
        normalized_values = {
            _normalize_column_name(value)
            for value in part_variants
            if _normalize_column_name(value)
        }
        lower_values = {value.lower() for value in part_variants if value}
        for lower_variant in lower_variants:
            if any(
                part_value == lower_variant or part_value.startswith(f"{lower_variant}(")
                for part_value in lower_values
            ):
                return True
        if normalized_variants.intersection(normalized_values):
            return True
    return False


def _field_aliases(field_name: str) -> List[str]:
    field_text = str(field_name or "").strip()
    if not field_text:
        return []
    aliases = [field_text]
    if field_text.upper().startswith("TR."):
        trimmed = field_text[3:]
        aliases.append(trimmed)
        if "." in trimmed:
            aliases.append(trimmed.split(".", 1)[1])
    for extra in FIELD_SYNONYMS_LOOKUP.get(field_text.upper(), []):
        if extra not in aliases:
            aliases.append(extra)
    return aliases


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"", "none", "null", "nan", "nat"}
    return False


def _to_scalar(value: Any) -> Any:
    if _is_missing(value):
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    item = getattr(value, "item", None)
    if callable(item):
        try:
            value = item()
        except Exception:  # noqa: BLE001
            pass
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _to_float(value: Any) -> Optional[float]:
    if _is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pick_value(row: Dict[str, Any], aliases: Iterable[str]) -> Any:
    for alias in aliases:
        for key, row_value in row.items():
            if _is_missing(row_value):
                continue
            if _column_matches_candidate(key, alias):
                return row_value
    return None


def _derived_intraday_fields(entry: Mapping[str, Any]) -> Dict[str, Optional[float]]:
    close_value = _to_float(entry.get("close"))
    total_return_1d = _to_float(entry.get("total_return_1d"))
    price_chg: Optional[float] = None
    if close_value is not None and total_return_1d is not None:
        price_chg = close_value + total_return_1d
    price_pct_chg: Optional[float] = None
    if total_return_1d is not None:
        price_pct_chg = total_return_1d / 100.0
    return {
        "price_chg": price_chg,
        "price_pct_chg": price_pct_chg,
    }


def prod_prices_data(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    entries = payload.get("data") or []
    if not isinstance(entries, list):
        return []

    flattened: List[Dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        row: Dict[str, Any] = {
            "date": entry.get("date"),
            "instrument": entry.get("instrument"),
        }
        for field_name in SNAKE_CASE_FIELDS:
            row[field_name] = entry.get(field_name)
        for requested_field, snake_field in FIELD_TO_SNAKE_CASE.items():
            if not _is_missing(row.get(snake_field)):
                continue
            value = _pick_value(entry, aliases=_field_aliases(requested_field))
            if _is_missing(value):
                continue
            row[snake_field] = _to_scalar(value)
        if _is_missing(row.get("instrument")):
            instrument = _pick_value(entry, aliases=("instrument", "tr.instrument", "ric", "tr.ric", "ric code"))
            if not _is_missing(instrument):
                row["instrument"] = str(instrument).strip().upper()
        row.update(_derived_intraday_fields(row))
        flattened.append(row)
    return flattened
