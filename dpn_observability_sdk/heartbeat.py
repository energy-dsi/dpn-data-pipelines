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
# | 1.0.0   | Initial version                                          | DSI Team      | 2026-06-26  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
Heartbeat Logging Utility.

This module provides a reusable heartbeat mechanism that emits periodic log
entries to indicate that a component is alive and healthy. These heartbeat
logs can be used to create monitoring dashboards in OpenSearch or other
observability platforms.

Usage::

    from utils.heartbeat import HeartbeatLogger
    from utils.otel_logger import OtelLogger
    
    logger = OtelLogger().create_logger()
    heartbeat = HeartbeatLogger(
        logger=logger,
        component_name="consumer-file-extractor",
        interval_seconds=900  # 15 minutes
    )
    
    # Start heartbeat in background thread
    heartbeat.start()
    
    # Your application logic here
    # ...
    
    # Stop heartbeat when shutting down
    heartbeat.stop()

Environment Variables:
----------------------
HEARTBEAT_INTERVAL_SECONDS : Override default heartbeat interval (default: 900 = 15 minutes)
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import UTC, datetime
from typing import Any


class HeartbeatLogger:
    """
    Periodic heartbeat logger for component health monitoring.
    
    Emits structured log entries at regular intervals to indicate that
    a component is alive and operational. These logs include:
    - Component name
    - Timestamp
    - Uptime
    - Custom metadata
    
    The heartbeat runs in a background daemon thread and can be started
    and stopped independently of the main application logic.
    """
    
    DEFAULT_INTERVAL_SECONDS: int = 300  # 5 minutes
    
    def __init__(
        self,
        logger: logging.Logger,
        component_name: str,
        interval_seconds: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize the heartbeat logger.
        
        Parameters
        ----------
        logger:
            Logger instance to use for emitting heartbeat logs.
        component_name:
            Name of the component (e.g., "consumer-file-extractor").
        interval_seconds:
            Heartbeat interval in seconds. If None, uses HEARTBEAT_INTERVAL_SECONDS
            environment variable or DEFAULT_INTERVAL_SECONDS (900 = 15 minutes).
        metadata:
            Optional additional metadata to include in every heartbeat log.
        """
        self.logger = logger
        self.component_name = component_name
        self.metadata = metadata or {}
        
        # Determine heartbeat interval
        if interval_seconds is not None:
            self.interval_seconds = interval_seconds
        else:
            self.interval_seconds = int(
                os.getenv("HEARTBEAT_INTERVAL_SECONDS", str(self.DEFAULT_INTERVAL_SECONDS))
            )
        
        # Thread control
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._start_time: datetime | None = None
        self._heartbeat_count = 0
        
    def _emit_heartbeat(self) -> None:
        """
        Emit a single heartbeat log entry.
        
        The log includes:
        - Component name
        - Current timestamp
        - Uptime since start
        - Heartbeat sequence number
        - Custom metadata
        """
        now = datetime.now(UTC)
        uptime_seconds = 0
        
        if self._start_time:
            uptime_seconds = int((now - self._start_time).total_seconds())
        
        self._heartbeat_count += 1
        
        log_extra = {
            "event.name": "component.heartbeat",
            "component.name": self.component_name,
            "heartbeat.timestamp": now.isoformat(),
            "heartbeat.sequence": self._heartbeat_count,
            "component.uptime_seconds": uptime_seconds,
            "heartbeat.interval_seconds": self.interval_seconds,
            "component.status": "healthy",
        }
        
        # Merge custom metadata
        log_extra.update(self.metadata)
        
        self.logger.info(
            f"Heartbeat: {self.component_name} is healthy",
            extra=log_extra,
        )
    
    def _heartbeat_loop(self) -> None:
        """
        Background thread loop that emits heartbeats at regular intervals.
        
        This method runs in a daemon thread and will automatically stop
        when the main program exits or when stop() is called.
        """
        self._start_time = datetime.now(UTC)
        
        # Emit initial heartbeat immediately
        self._emit_heartbeat()
        
        while not self._stop_event.is_set():
            # Wait for the interval or until stop is signaled
            if self._stop_event.wait(timeout=self.interval_seconds):
                # Stop was signaled
                break
            
            # Emit heartbeat
            self._emit_heartbeat()
    
    def start(self) -> None:
        """
        Start the heartbeat logger in a background thread.
        
        The heartbeat will emit immediately upon start, then at regular
        intervals. If already started, this method does nothing.
        """
        if self._thread is not None and self._thread.is_alive():
            self.logger.warning(
                "Heartbeat already running",
                extra={"component.name": self.component_name},
            )
            return
        
        self._stop_event.clear()
        self._heartbeat_count = 0
        
        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"heartbeat-{self.component_name}",
            daemon=True,
        )
        self._thread.start()
        
        self.logger.info(
            "Heartbeat logger started",
            extra={
                "event.name": "heartbeat.started",
                "component.name": self.component_name,
                "heartbeat.interval_seconds": self.interval_seconds,
            },
        )
    
    def stop(self, timeout: float = 5.0) -> None:
        """
        Stop the heartbeat logger.
        
        Signals the background thread to stop and waits for it to finish.
        
        Parameters
        ----------
        timeout:
            Maximum time to wait for the thread to stop (in seconds).
        """
        if self._thread is None or not self._thread.is_alive():
            return
        
        self._stop_event.set()
        self._thread.join(timeout=timeout)
        
        self.logger.info(
            "Heartbeat logger stopped",
            extra={
                "event.name": "heartbeat.stopped",
                "component.name": self.component_name,
                "heartbeat.total_count": self._heartbeat_count,
            },
        )
    
    def update_metadata(self, metadata: dict[str, Any]) -> None:
        """
        Update the metadata that will be included in future heartbeats.
        
        This allows dynamic metadata to be updated without restarting
        the heartbeat logger.
        
        Parameters
        ----------
        metadata:
            Dictionary of metadata to merge with existing metadata.
        """
        self.metadata.update(metadata)
    
    def is_running(self) -> bool:
        """
        Check if the heartbeat logger is currently running.
        
        Returns
        -------
        bool
            True if the heartbeat thread is alive, False otherwise.
        """
        return self._thread is not None and self._thread.is_alive()

# Made with Bob
