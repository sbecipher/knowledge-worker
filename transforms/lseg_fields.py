from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict


@lru_cache(maxsize=1)
def _load_registry() -> Dict[str, Any]:
    path = Path(__file__).with_name("lseg_fields.json")
    return json.loads(path.read_text(encoding="utf-8"))


def fundamentals_statement_config() -> Dict[str, Dict[str, str]]:
    config = _load_registry()["fundamentals"]["statement_config"]
    return {
        str(statement): {
            "prefix": str(values["prefix"]),
            "value_key": str(values["value_key"]),
            "value_label": str(values["value_label"]),
        }
        for statement, values in config.items()
    }


def prices_field_to_snake_case() -> Dict[str, str]:
    mapping = _load_registry()["intraday"]["field_to_snake_case"]
    return {str(key): str(value) for key, value in mapping.items()}
