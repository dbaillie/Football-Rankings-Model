"""Load europe calibration summary JSON produced by scripts/analyse_europe_calibration.py."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

OUTPUT_EUROPE_DIR = Path(__file__).resolve().parents[2] / "output" / "europe"
_CALIBRATION_JSON = OUTPUT_EUROPE_DIR / "calibration_summary.json"

_mt_seen: float | None = None
_cached_payload: dict[str, Any] | None = None


def calibration_summary_path() -> Path:
    return _CALIBRATION_JSON.resolve()


def clear_calibration_summary_cache() -> None:
    global _mt_seen, _cached_payload
    _mt_seen = None
    _cached_payload = None


def load_calibration_summary() -> dict[str, Any] | None:
    """Current calibration_summary.json contents, or None if missing / invalid."""
    global _mt_seen, _cached_payload
    path = _CALIBRATION_JSON
    if not path.exists():
        return None
    mt = float(path.stat().st_mtime_ns)
    if _mt_seen == mt and _cached_payload is not None:
        return _cached_payload
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        _mt_seen = mt
        _cached_payload = None
        return None
    if not isinstance(data, dict):
        _mt_seen = mt
        _cached_payload = None
        return None
    _mt_seen = mt
    _cached_payload = data
    return data
