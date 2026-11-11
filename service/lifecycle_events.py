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
One-off lifecycle events for actions the self-service UI itself takes on a
data product or consumer pipeline — created/started/stopped/paused/resumed/
deleted/run-triggered.

Distinct from dpn_observability_sdk.heartbeat.HeartbeatLogger, which each
product's own pod runs to emit a periodic "still alive" pulse: those only
exist while a pod is up, so they can't cover the exact moment of create,
start, pause, resume, delete or Run Now — actions this UI (ui/app.py)
performs itself, sometimes on a product that isn't even running yet (e.g.
create) or right before its pod goes away (delete). Emitting a distinctly
named event here, from the one process that actually knows *why* a
Deployment is being created/changed/removed, tags each action with the same
component.name/event.name shape a heartbeat uses so both land in the same
OTEL Collector pipeline (see dpn_observability_sdk.otel_logger.OtelLogger)
and can be correlated by dashboards already grouping on component.name.
"""
from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any

from dpn_observability_sdk.otel_logger import OtelLogger

# Plain stdlib logger, used only to report a failure to emit below — never
# for the lifecycle event itself. Matches service/precheck.py's own
# _logger convention, for the same reason: this must never depend on the
# same OTel/truststore machinery it's reporting a failure of.
_fallback_logger = logging.getLogger(__name__)

# Built lazily, on first emit() call rather than at import time — matching
# every blueprint main.py's own Logging().create_logger() call inside
# __init__ rather than at module scope, since constructing it eagerly here
# would mean every module that merely imports lifecycle_events (e.g. a test
# collecting ui.app) pays for OTel SDK setup even when it never emits an
# event.
_logger: Any = None


def _get_logger() -> Any:
    global _logger
    if _logger is None:
        _logger = OtelLogger().create_logger(os.getenv("SERVICE_NAME", "dpn-self-service"))
    return _logger


def emit(component_name: str, event: str, **metadata: Any) -> None:
    """
    Emit one structured "component.<event>" log line for component_name —
    e.g. emit(product.adaptor_container_name, "deleted", product_type=...).

    event becomes both the human-readable log body and the event.name
    attribute (component.<event>), matching HeartbeatLogger's own
    "component.heartbeat" / "heartbeat.started" / "heartbeat.stopped" naming
    so all of a product's lifecycle signals — periodic heartbeats and these
    one-off actions alike — share one naming scheme in the same collector.

    Fails soft, same posture as service/precheck.py's two checks: this is
    an observability side-effect of a UI action, not a precondition for it
    — a missing OTel truststore, unreachable collector, or any other setup
    problem here must never turn into a 500 on the actual create/start/
    stop/pause/resume/delete/Run Now request that triggered it.
    """
    try:
        _get_logger().info(
            f"{component_name}: {event}",
            extra={
                "event.name": f"component.{event}",
                "component.name": component_name,
                "lifecycle.timestamp": datetime.now(UTC).isoformat(),
                **metadata,
            },
        )
    except Exception:  # noqa: BLE001 - observability setup issue, not a caller error
        _fallback_logger.warning(
            "Could not emit lifecycle event %r for %r", event, component_name, exc_info=True
        )
