# Copyright DSI Project
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
# | 1.0.0   | Initial version                                          | DSI Team      | 2026-05-01  |
# +---------+----------------------------------------------------------+---------------+-------------+


"""
Step Logger

Provides unified structured logging for pipeline steps.

Features:
---------
- Standardized step lifecycle logging (start, end, failed, skipped)
- Duration tracking per step
- StatsD metrics emission (optional)
- OpenTelemetry-compatible structured logs

Each pipeline stage uses StepLogger to ensure:
- Consistent observability
- Correlated logs (via PipelineContext)
- Performance visibility (duration metrics)
"""

from __future__ import annotations

import logging
import os
import socket
import time
from typing import Any

from utils.pipeline_context import PipelineContext


# ---------------------------------------------------------------------------
# StatsD Client (Optional)
# ---------------------------------------------------------------------------

class _StatsD:
    """
    Minimal StatsD client.

    Sends UDP metrics if STATSD_HOST is configured.
    """

    def __init__(self) -> None:
        self._host = os.getenv("STATSD_HOST") or None
        self._port = int(os.getenv("STATSD_PORT", "8125"))

        self._sock = (
            socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            if self._host
            else None
        )

    def _send(self, message: str) -> None:
        """Send a UDP message to StatsD backend."""
        if self._sock:
            try:
                self._sock.sendto(message.encode(), (self._host, self._port))
            except OSError:
                # Fail silently (non-blocking metrics)
                pass

    def counter(
        self,
        name: str,
        value: int = 1,
        tags: dict[str, Any] | None = None,
    ) -> None:
        """Emit counter metric."""
        self._send(f"{name}{self._format_tags(tags)}:{value}|c")

    def gauge(
        self,
        name: str,
        value: float,
        tags: dict[str, Any] | None = None,
    ) -> None:
        """Emit gauge metric."""
        self._send(f"{name}{self._format_tags(tags)}:{value:.3f}|g")

    @staticmethod
    def _format_tags(tags: dict[str, Any] | None) -> str:
        """Format StatsD tags."""
        if not tags:
            return ""
        return "|#" + ",".join(f"{k}:{v}" for k, v in tags.items())


# ---------------------------------------------------------------------------
# StepLogger
# ---------------------------------------------------------------------------

class StepLogger:
    """
    Structured step-level logger.

    Wraps a logging.Logger instance and:
    - Tracks execution duration
    - Emits structured logs
    - Emits StatsD metrics

    Each operation is identified by:
        run_id + operation_name
    """

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._statsd = _StatsD()

        # Tracks start time for each operation
        self._timers: dict[str, float] = {}

    # -----------------------------------------------------------------------
    # Step Lifecycle Methods
    # -----------------------------------------------------------------------

    def step_start(
        self,
        ctx: PipelineContext,
        operation: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """
        Log step start event.

        Args:
            ctx: Pipeline context
            operation: Operation name
            extra: Optional additional context
        """
        key = self._build_key(ctx, operation)
        self._timers[key] = time.perf_counter()

        tags = self._build_tags(ctx, operation)

        self._statsd.counter(
            "pipeline.step.started",
            tags=tags,
        )

        payload = {
            "event.name": "pipeline.step.started",
            "pipeline.operation": operation,
            **ctx.as_log_extra(),
        }

        if extra:
            payload.update(extra)

        self._logger.info(
            "[%s] step started — %s (run_id=%s)",
            ctx.pipeline_stage,
            operation,
            ctx.run_id[:8],
            extra=payload,
        )

    def step_end(
        self,
        ctx: PipelineContext,
        operation: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """
        Log step completion event.

        Args:
            ctx: Pipeline context
            operation: Operation name
            extra: Optional additional context
        """
        duration_ms = self._compute_duration(ctx, operation)
        tags = self._build_tags(ctx, operation)

        self._statsd.counter("pipeline.step.completed", tags=tags)
        self._statsd.gauge(
            "pipeline.step.duration_ms",
            duration_ms,
            tags=tags,
        )

        payload = {
            "event.name": "pipeline.step.completed",
            "pipeline.operation": operation,
            "pipeline.duration_ms": round(duration_ms, 3),
            **ctx.as_log_extra(),
        }

        if extra:
            payload.update(extra)

        self._logger.info(
            "[%s] step completed — %s (%.1f ms, run_id=%s)",
            ctx.pipeline_stage,
            operation,
            duration_ms,
            ctx.run_id[:8],
            extra=payload,
        )

    def step_failed(
        self,
        ctx: PipelineContext,
        operation: str,
        exc: BaseException,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """
        Log step failure event.

        Args:
            ctx: Pipeline context
            operation: Operation name
            exc: Exception raised
            extra: Optional additional context
        """
        duration_ms = self._compute_duration(ctx, operation)
        tags = self._build_tags(ctx, operation)

        self._statsd.counter("pipeline.step.failed", tags=tags)
        self._statsd.gauge(
            "pipeline.step.duration_ms",
            duration_ms,
            tags=tags,
        )

        payload = {
            "event.name": "pipeline.step.failed",
            "pipeline.operation": operation,
            "pipeline.duration_ms": round(duration_ms, 3),
            "error.type": type(exc).__name__,
            "error.message": str(exc),
            **ctx.as_log_extra(),
        }

        if extra:
            payload.update(extra)

        self._logger.error(
            "[%s] step FAILED — %s (%s, run_id=%s)",
            ctx.pipeline_stage,
            operation,
            type(exc).__name__,
            ctx.run_id[:8],
            extra=payload,
            exc_info=True,
        )

    def step_skipped(
        self,
        ctx: PipelineContext,
        reason: str = "EXECUTION_MODE=manual",
        extra: dict[str, Any] | None = None,
    ) -> None:
        """
        Log skipped execution.

        Args:
            ctx: Pipeline context
            reason: Skip reason
            extra: Optional additional context
        """
        self._statsd.counter(
            "pipeline.step.skipped",
            tags=self._build_tags(ctx, "skip"),
        )

        payload = {
            "event.name": "pipeline.step.skipped",
            "pipeline.skip_reason": reason,
            **ctx.as_log_extra(),
        }

        if extra:
            payload.update(extra)

        self._logger.info(
            "[%s] run skipped — %s (run_id=%s)",
            ctx.pipeline_stage,
            reason,
            ctx.run_id[:8],
            extra=payload,
        )

    def pipeline_banner(
        self,
        ctx: PipelineContext,
        service_name: str,
        config_summary: dict[str, Any],
    ) -> None:
        """
        Log pipeline startup banner.

        Args:
            ctx: Pipeline context
            service_name: Service identifier
            config_summary: Runtime configuration snapshot
        """
        self._logger.info(
            "[%s] pipeline starting — %s (run_id=%s)",
            ctx.pipeline_stage,
            service_name,
            ctx.run_id[:8],
            extra={
                "event.name": "pipeline.startup",
                "pipeline.service": service_name,
                "pipeline.config": config_summary,
                **ctx.as_log_extra(),
            },
        )

    # -----------------------------------------------------------------------
    # Internal Helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _build_key(ctx: PipelineContext, operation: str) -> str:
        """Build unique key per run + operation."""
        return f"{ctx.run_id}:{operation}"

    @staticmethod
    def _build_tags(ctx: PipelineContext, operation: str) -> dict[str, str]:
        """Build StatsD tag set."""
        return {
            "stage": ctx.pipeline_stage,
            "type": ctx.pipeline_type,
            "role": ctx.pipeline_role,
            "operation": operation,
            "mode": ctx.mode,
        }

    def _compute_duration(
        self,
        ctx: PipelineContext,
        operation: str,
    ) -> float:
        """
        Compute step duration in milliseconds.

        Returns 0.0 if no start time found.
        """
        start = self._timers.pop(
            self._build_key(ctx, operation),
            None,
        )

        if start is None:
            return 0.0

        return (time.perf_counter() - start) * 1000.0