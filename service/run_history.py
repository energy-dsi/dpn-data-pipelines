# Copyright 2026 DSI Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# +---------+----------------------------------------------------------+---------------+-------------+
# | Version | Description                                              | Change Owner  | Change Date |
# +---------+----------------------------------------------------------+---------------+-------------+
# | 1.0.0   | Initial version                                           | DSI Team      | 2026-09-14  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
Lightweight run history.

Every adaptor/schema_mapper container already emits structured JSON log
lines (via utils/step_logger.py and dpn_observability_sdk) carrying
event.name, pipeline.run_id, pipeline.operation, duration, and per-event
detail (files copied/skipped, destination topic, etc.) — this module just
reads `podman logs`, picks out the completion/failure events, and persists
them here so a "Recent Runs" view survives a container being recreated
(Pause/Resume/Stop all do a `podman rm` + `podman run`, which wipes the
container's own log buffer).

This is deliberately a stopgap, not a replacement for real observability:
the same containers already emit full OTEL traces/metrics/logs to
OTEL_EXPORTER_OTLP_ENDPOINT, which currently points at an unreachable
in-cluster collector hostname in this local demo. Wiring that up to a real
collector + backend (or moving to Airflow's own run history) is the
better long-term answer — see service/README.md.
"""
from __future__ import annotations

import json
import os
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

_DATA_DIR = Path(__file__).parent / "data"
_HISTORY_FILE = _DATA_DIR / "run_history.json"
_lock = threading.Lock()

_TRACKED_EVENTS = {"pipeline.step.completed", "pipeline.step.failed", "pipeline.cycle.complete"}

# attributes keys already surfaced as their own record fields — everything
# else in `attributes` is kept as free-form "detail" for display.
_KNOWN_ATTR_KEYS = {
    "event.name",
    "pipeline.operation",
    "pipeline.run_id",
    "pipeline.duration_ms",
    "pipeline.stage",
    "pipeline.type",
    "pipeline.role",
    "component.name",
    "pipeline.mode",
    "pipeline.triggered_by",
    "pipeline.dag_run_id",
    "pipeline.dag_id",
    "pipeline.started_at",
}

# Retention: keep at most MAX_RECORDS runs, or only the last MAX_AGE_DAYS
# days of them — whichever is more restrictive (an active deployment
# hits the count cap first; a quiet one hits the age cap first, so old
# runs don't linger indefinitely just because nothing's happened since).
# Enforced once, here, at write time — every reader (get_history()) sees
# an already-pruned store, not just a display-time slice of a larger one.
# Overridable per deployment via env vars (e.g. a longer window for an
# environment that wants more audit history); read once at import time,
# same convention as service/env_builder.py's platform-level settings.
MAX_RECORDS = int(os.getenv("DPN_RUN_HISTORY_MAX_RECORDS", "100"))
MAX_AGE_DAYS = int(os.getenv("DPN_RUN_HISTORY_MAX_AGE_DAYS", "30"))


def _read_all() -> list[dict]:
    if not _HISTORY_FILE.exists():
        return []
    with _HISTORY_FILE.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _within_max_age(timestamp: str | None) -> bool:
    """False for anything unparsable — safer to prune a malformed record
    than let it accumulate forever outside the intended retention window."""
    if not timestamp:
        return False
    try:
        ts = datetime.fromisoformat(timestamp)
    except ValueError:
        return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts >= datetime.now(UTC) - timedelta(days=MAX_AGE_DAYS)


def _write_all(records: list[dict]) -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    records = [r for r in records if _within_max_age(r.get("timestamp"))]
    # Cap retention per pipeline family (stage "adaptor" == producer,
    # "extractor" == consumer — the same split history.html's view-switch
    # filters on) rather than across the whole store. A single shared cap
    # let a chattier role's runs evict the other role's history entirely,
    # so selecting "Latest 100 Runs" for the quieter role could silently
    # show far fewer than 100 even when that many genuinely happened.
    by_stage: dict[str | None, list[dict]] = {}
    for r in records:
        by_stage.setdefault(r.get("stage"), []).append(r)
    capped: list[dict] = []
    for stage_records in by_stage.values():
        stage_records.sort(key=lambda r: r.get("timestamp") or "")
        capped.extend(stage_records[-MAX_RECORDS:])
    capped.sort(key=lambda r: r.get("timestamp") or "")
    with _HISTORY_FILE.open("w", encoding="utf-8") as fh:
        json.dump(capped, fh, indent=2)


def _parse_line(product_type: str, stage: str, line: str) -> dict[str, Any] | None:
    line = line.strip()
    if not line or not line.startswith("{"):
        return None
    try:
        obj = json.loads(line)
    except (json.JSONDecodeError, ValueError):
        return None

    attrs = obj.get("attributes") or {}
    event = attrs.get("event.name")
    if event not in _TRACKED_EVENTS:
        return None

    return {
        "product_type": product_type,
        "stage": attrs.get("pipeline.stage", stage),
        "event": event,
        "operation": attrs.get("pipeline.operation"),
        "run_id": attrs.get("pipeline.run_id"),
        "duration_ms": attrs.get("pipeline.duration_ms"),
        # "manual" here is the self-service "Run Now" button (see
        # utils/scheduler_backend.py's run_once()) — surfaced as its own
        # field, not left inside detail, so history.html can flag it
        # distinctly from a scheduled "interval"/"kafka-trigger" run.
        "triggered_by": attrs.get("pipeline.triggered_by"),
        "timestamp": obj.get("timestamp"),
        "body": obj.get("body"),
        "detail": {k: v for k, v in attrs.items() if k not in _KNOWN_ATTR_KEYS},
    }


def record_from_logs(product_type: str, stage: str, log_text: str) -> int:
    """
    Parse `log_text` (as returned by podman_ops.logs) for tracked events and
    persist any not already recorded. Returns the number of new records
    added.
    """
    parsed = [
        rec for line in log_text.splitlines()
        if (rec := _parse_line(product_type, stage, line)) is not None
    ]
    if not parsed:
        return 0

    with _lock:
        existing = _read_all()
        seen = {
            (r["product_type"], r["stage"], r.get("run_id"), r["event"], r.get("operation"), r["timestamp"])
            for r in existing
        }
        new_records = [
            rec for rec in parsed
            if (rec["product_type"], rec["stage"], rec.get("run_id"), rec["event"], rec.get("operation"), rec["timestamp"])
            not in seen
        ]
        if new_records:
            _write_all(existing + new_records)
        return len(new_records)


def record_failure(product_type: str, stage: str, message: str) -> None:
    """
    Directly persist a run that never produced a structured JSON log line
    at all — see ui/app.py's _run_once_and_record(). "Run Now"
    can fail before the pipeline code ever runs (the container isn't up,
    a Kubernetes RBAC 403 on pods/exec, a dropped connection, ...); that
    failure is a plain exception message, not one of the JSON lines
    record_from_logs()/_parse_line() know how to recognize, so without
    this the failure would vanish silently — Run History would just show
    nothing for that click, indistinguishable from Run Now never having
    been pressed.
    """
    with _lock:
        existing = _read_all()
        existing.append({
            "product_type": product_type,
            "stage": stage,
            "event": "pipeline.step.failed",
            "operation": "run_once",
            "run_id": None,
            "duration_ms": None,
            "triggered_by": "manual",
            "timestamp": datetime.now(UTC).isoformat(),
            "body": message,
            # Same shape StepLogger.step_failed() puts in a real failure's
            # detail (see utils/step_logger.py) — history.html's Detail
            # column reads `detail`, not `body`.
            "detail": {"error.message": message},
        })
        _write_all(existing)


def get_history(
    product_type: str | None = None, stage: str | None = None, limit: int = MAX_RECORDS
) -> list[dict]:
    with _lock:
        records = _read_all()
    if product_type:
        records = [r for r in records if r["product_type"] == product_type]
    if stage:
        records = [r for r in records if r.get("stage") == stage]
    records.sort(key=lambda r: r.get("timestamp") or "", reverse=True)
    return records[:limit]
