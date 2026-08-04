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
Kafka Transaction Utility

This module provides a wrapper around the `confluent-kafka` library with:

- Reliable producer configuration (acks=all, compression, keepalive)
- Structured logging compatible with OpenTelemetry (OTel)
- W3C TraceContext propagation for distributed tracing
- Helper utilities for producing and consuming Kafka messages

Key Features:
-------------
1. Producer:
   - JSON serialization
   - OTel trace injection into headers
   - Delivery callback logging

2. Consumer:
   - JSON deserialization
   - Trace extraction for distributed tracing continuity
   - Per-message error isolation

Example:
--------
    kt = KafkaTransection(bootstrap_server="localhost:9092")
    kt.send_message(target_topic="my-topic", message={"key": "value"})
"""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Optional

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
)

from utils.otel_logger import Logging


# ---------------------------------------------------------------------------
# OpenTelemetry (OTel) Optional Support
# ---------------------------------------------------------------------------
try:
    from opentelemetry import propagate as _otel_propagate  # type: ignore
    from opentelemetry.trace.propagation.tracecontext import (  # type: ignore
        TraceContextTextMapPropagator,
    )

    _OTEL_AVAILABLE = True
    _propagator = TraceContextTextMapPropagator()

except ImportError:  # pragma: no cover
    _OTEL_AVAILABLE = False
    _propagator = None  # type: ignore[assignment]


class _HeaderCarrier(dict):
    """
    Adapter for OpenTelemetry propagator to Kafka headers.

    Converts between OTel text map format and Kafka header format.

    Kafka expects headers as:
        List[Tuple[str, bytes]]

    OTel works with:
        Dict[str, str]
    """

    def set(self, key: str, value: str) -> None:  # noqa: A003
        """Set header value (encoded as bytes)."""
        self[key] = value.encode()

    def get(
        self,
        key: str,
        default: Optional[str] = None,
    ) -> Optional[str]:  # noqa: A003
        """Retrieve header value and decode if needed."""
        raw = super().get(key)
        if raw is None:
            return default
        return raw.decode() if isinstance(raw, bytes) else raw

    def keys(self) -> list[str]:  # noqa: A003
        """Return header keys."""
        return list(super().keys())


class KafkaTransection:
    """
    Kafka Producer/Consumer Utility.

    Provides:
    - Safe message production with delivery guarantees
    - JSON serialization/deserialization
    - OTel trace propagation support

    Parameters
    ----------
    bootstrap_server : Optional[str]
        Kafka bootstrap server(s)
    logger : Optional[logging.Logger]
        Pre-configured logger
    boostrap_server : Optional[str]
        Deprecated spelling (backward compatibility)

    Notes
    -----
    If both `bootstrap_server` and `boostrap_server` are provided,
    `bootstrap_server` takes precedence.
    """

    def __init__(
        self,
        bootstrap_server: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        boostrap_server: Optional[str] = None,
        component_name: Optional[str] = None,
    ) -> None:
        """Initialize Kafka transaction utility."""
        resolved = bootstrap_server or boostrap_server

        if not resolved:
            raise ValueError(
                "Kafka bootstrap server must be provided via "
                "`bootstrap_server` (or legacy `boostrap_server`)."
            )

        self.bootstrap_server: str = resolved
        self.component_name: Optional[str] = component_name
        self._logger: logging.Logger = (
            logger or Logging().create_logger()
        )

        # Lazy-initialized producer
        self._producer: Optional[Producer] = None

    # -----------------------------------------------------------------------
    # Producer Methods
    # -----------------------------------------------------------------------
    def _get_producer(self) -> Producer:
        """
        Lazily initialize Kafka producer with production-safe settings.

        Returns
        -------
        Producer
        """
        if self._producer is None:
            self._producer = Producer(
                {
                    "bootstrap.servers": self.bootstrap_server,
                    "acks": "all",
                    "enable.idempotence": False,
                    "compression.type": "lz4",
                    "socket.keepalive.enable": True,
                }
            )
        return self._producer

    def send_message(
        self,
        target_topic: str,
        message: dict[str, Any],
    ) -> None:
        """
        Send a JSON message to Kafka topic.

        Features:
        - JSON serialization
        - OTel trace context injection
        - Delivery logging

        Parameters
        ----------
        target_topic : str
            Destination Kafka topic
        message : dict
            Message payload
        """
        if not target_topic:
            raise ValueError("target_topic must not be empty.")

        # Inject trace context into headers
        carrier = _HeaderCarrier()
        if _OTEL_AVAILABLE and _propagator:
            _otel_propagate.inject(carrier)

        def _on_delivery(
            err: Optional[KafkaError],
            msg: Message,
        ) -> None:
            """Kafka delivery callback."""
            if err:
                self._logger.error(
                    "Kafka delivery failed",
                    extra={
                        "topic": target_topic,
                        "error": str(err),
                        "component.name": self.component_name,
                    },
                )
            else:
                self._logger.info(
                    "Kafka message delivered",
                    extra={
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "component.name": self.component_name,
                    },
                )

        payload = json.dumps(message, default=str).encode("utf-8")

        self._get_producer().produce(
            topic=target_topic,
            value=payload,
            headers=list(carrier.items()),
            callback=_on_delivery,
        )

        # Ensures synchronous-like delivery
        self._get_producer().flush()

    def flush(self, timeout: float = 30.0) -> None:
        """
        Flush pending Kafka messages.

        Parameters
        ----------
        timeout : float
            Maximum wait time in seconds
        """
        if self._producer is not None:
            self._get_producer().flush(timeout=timeout)

    # -----------------------------------------------------------------------
    # Consumer Methods
    # -----------------------------------------------------------------------
    def consume_messages(
        self,
        source_topic: str,
        group_id: str,
        handler: Callable[[dict[str, Any]], None],
        poll_timeout: float = 1.0,
    ) -> None:
        """
        Consume messages continuously and process via handler.

        Features:
        - JSON deserialization
        - OTel trace extraction
        - Error isolation per message

        Parameters
        ----------
        source_topic : str
            Topic to consume from
        group_id : str
            Consumer group identifier
        handler : Callable
            Function to process each message payload
        poll_timeout : float
            Poll timeout in seconds
        """
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_server,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
                "session.timeout.ms": 30_000,
                "heartbeat.interval.ms": 10_000,
            }
        )

        consumer.subscribe([source_topic])

        self._logger.info(
            "Kafka consumer subscribed",
            extra={
                "topic": source_topic,
                "group_id": group_id,
                "component.name": self.component_name,
            },
        )

        try:
            while True:
                msg: Optional[Message] = consumer.poll(
                    timeout=poll_timeout
                )

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                # Extract trace context from headers
                raw_headers = msg.headers() or []
                if _OTEL_AVAILABLE and _propagator:
                    ctx_carrier = _HeaderCarrier(
                        {k: v for k, v in raw_headers}
                    )
                    _otel_propagate.extract(ctx_carrier)

                try:
                    payload = json.loads(msg.value().decode("utf-8"))

                    self._logger.info(
                        "Kafka message received",
                        extra={
                            "topic": source_topic,
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                            "component.name": self.component_name,
                        },
                    )

                    handler(payload)

                except json.JSONDecodeError as exc:
                    self._logger.error(
                        "JSON decode failed",
                        extra={"error": str(exc), "component.name": self.component_name},
                    )

                except Exception as exc:  # noqa: BLE001
                    self._logger.error(
                        "Message handler exception",
                        extra={"error": str(exc), "component.name": self.component_name},
                        exc_info=True,
                    )

        finally:
            consumer.close()
            self._logger.info(
                "Kafka consumer closed",
                extra={"topic": source_topic, "component.name": self.component_name},
            )
