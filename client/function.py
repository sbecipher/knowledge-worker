import asyncio
from typing import Any, Dict, Iterable

import client


def _first_value(value: Any) -> Any:
    if isinstance(value, (list, tuple)) and value:
        return value[0]
    return value


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _to_tickers(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, Iterable):
        return ",".join(str(item) for item in value if str(item).strip())
    return str(value or "")


def _merge_inputs(query: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(query)
    merged.update(payload)
    return merged


def _build_argv(data: Dict[str, Any]) -> list:
    tickers = _to_tickers(data.get("tickers") or data.get("TICKERS"))
    start_date = data.get("start_date") or data.get("START_DATE")
    end_date = data.get("end_date") or data.get("END_DATE")

    missing = [name for name, value in [("tickers", tickers), ("start_date", start_date), ("end_date", end_date)] if not value]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    argv = ["--tickers", tickers, "--start-date", str(start_date), "--end-date", str(end_date)]

    if data.get("intraday_frequency"):
        argv += ["--intraday-frequency", str(data["intraday_frequency"])]
    if data.get("fundamentals_mode"):
        argv += ["--fundamentals-mode", str(data["fundamentals_mode"])]
    if data.get("intraday_mode"):
        argv += ["--intraday-mode", str(data["intraday_mode"])]
    if data.get("workflow_id"):
        argv += ["--workflow-id", str(data["workflow_id"])]
    if data.get("workflow_name"):
        argv += ["--workflow-name", str(data["workflow_name"])]
    if data.get("task_queue"):
        argv += ["--task-queue", str(data["task_queue"])]
    if data.get("address"):
        argv += ["--address", str(data["address"])]

    if _to_bool(data.get("edgar_source")):
        argv.append("--edgar-source")
    if _to_bool(data.get("metadata_only")):
        argv.append("--metadata-only")
    if _to_bool(data.get("edgar_only")):
        argv.append("--edgar-only")

    return argv


def marketflow_handler(request):
    payload = request.get_json(silent=True) or {}
    query = {k: _first_value(v) for k, v in (request.args or {}).items()}
    data = _merge_inputs(query, payload)
    try:
        argv = _build_argv(data)
        args = client.parse_args(argv)
        result = asyncio.run(client.start_workflow(args))
    except (ValueError, SystemExit) as exc:
        return {"ok": False, "error": str(exc)}, 400
    except Exception as exc:  # pragma: no cover - runtime visibility for Cloud Functions
        return {"ok": False, "error": str(exc)}, 500
    return {"ok": True, "result": result}, 200
