"""
Topic Adaptor – Producer-side topic ingestion pathway.

This module defines a Kafka-based Topic Adaptor responsible for consuming
messages from a source topic (`srcTopicName`) and forwarding them unchanged
to a downstream mapper topic (`mapperTopicName`).

If `mapperTopicName` is not explicitly configured, it is derived automatically
as `<srcTopicName>-trfm` and created on the Kafka broker if it does not exist.

Typical topic flow:
    srcTopicName  →  TopicAdaptor  →  mapperTopicName
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv

from utils.otel_logger import OtelLogger as Logging
# from utils.topic_utils import ensure_exists, resolve

from utils.topic_utils import TopicResolver, KafkaTopicManager

from utils.topic_config_validator import AdaptorConfigValidator

load_dotenv()


class TopicAdaptor:
    """
    Kafka Topic Adaptor (Producer Side).

    This class encapsulates the full lifecycle of a Kafka topic adaptor:
    - Reads configuration from environment variables
    - Ensures downstream topic existence
    - Subscribes to a source topic
    - Forwards messages to a mapper topic
    - Handles retries and logging in a resilient loop

    The adaptor is designed to be safe to restart and idempotent with respect
    to topic creation.
    """

    SERVICE_NAME = "producer-topic-adaptor"

    def __init__(self) -> None:
        """
        Initialize the TopicAdaptor.

        Responsibilities:
        - Load environment configuration
        - Resolve mapper topic name
        - Ensure mapper topic exists
        - Initialize Kafka producer
        - Initialize structured logger
        """
        self.logger = Logging().create_logger()

        self.topic_resolver = TopicResolver()

        self.bootstrap_server: str = os.getenv("bootstrapServer", "")
        self.src_topic: str = os.getenv("srcTopicName", "")
        self.group_id: str = os.getenv(
            "srcGroupId", self.SERVICE_NAME
        )
        self.retry_delay: int = int(
            os.getenv("consumerRetryDelaySecs", "5")
        )

        self.mapper_topic: str = self.topic_resolver.resolve(
            os.getenv("mapperTopicName"),
            self.src_topic,
            "trfm",
        )

        self.kafka_topic_manager = KafkaTopicManager(bootstrap_server = self.bootstrap_server, logger = self.logger)

        self.kafka_topic_manager.ensure_exists(
            topic_name = self.mapper_topic,
        )

        self.producer = Producer(
            {"bootstrap.servers": self.bootstrap_server}
        )

    # ──────────────────────────────────────────────
    # Startup / Configuration Logging
    # ──────────────────────────────────────────────

    def log_startup_banner(self) -> None:
        """
        Log a formatted startup banner with configuration details.

        This provides visibility into runtime configuration and is helpful
        for diagnostics during startup and deployments.
        """
        lines = [
            "---------- Producer - Topic Adaptor Config Information ----------",
            f"srcTopicName    : {self.src_topic}",
            f"mapperTopicName : {self.mapper_topic}",
            f"bootstrapServer : {self.bootstrap_server}",
            f"srcGroupId      : {self.group_id}",
        ]

        width = max(len(l) for l in lines) + 4
        border = "+" + "-" * (width - 2) + "+"

        self.logger.info(border)
        for line in lines:
            self.logger.info(f"| {line.ljust(width - 4)} |")
        self.logger.info(border)

    # ──────────────────────────────────────────────
    # Kafka Callbacks
    # ──────────────────────────────────────────────

    def _on_delivery(self, err, msg) -> None:
        """
        Kafka producer delivery callback.

        Called asynchronously by the Kafka producer after a message
        is either successfully delivered or permanently failed.

        Parameters
        ----------
        err:
            Delivery error (None on success).
        msg:
            The Kafka message that was produced.
        """
        if err:
            self.logger.error(
                "Kafka message delivery failed",
                extra={
                    "event.name": "message.delivery.failed",
                    "service.name": self.SERVICE_NAME,
                    "messaging.system": "kafka",
                    "error.message": str(err),
                },
            )
        else:
            self.logger.info(
                "Kafka message delivered",
                extra={
                    "event.name": "message.delivery.completed",
                    "service.name": self.SERVICE_NAME,
                    "messaging.system": "kafka",
                    "messaging.destination.name": msg.topic(),
                    "messaging.kafka.partition": msg.partition(),
                    "messaging.kafka.message.offset": msg.offset(),
                },
            )

    # ──────────────────────────────────────────────
    # Consumer Setup
    # ──────────────────────────────────────────────

    def _create_consumer(self) -> Consumer:
        """
        Create and configure a Kafka consumer instance.

        Returns
        -------
        Consumer
            A subscribed Kafka consumer ready to poll messages.
        """
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_server,
                "group.id": self.group_id,
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            }
        )

        consumer.subscribe([self.src_topic])

        self.logger.info(
            "Subscribed to source topic",
            extra={
                "event.name": "consumer.subscribed",
                "service.name": self.SERVICE_NAME,
                "messaging.system": "kafka",
                "messaging.destination.name": self.src_topic,
            },
        )

        return consumer

    # ──────────────────────────────────────────────
    # Message Processing
    # ──────────────────────────────────────────────

    def _process_message(self, msg) -> None:
        """
        Process a single Kafka message.

        The message is forwarded unchanged from the source topic
        to the mapper topic, with detailed latency and lifecycle
        logging for observability.

        Parameters
        ----------
        msg:
            Kafka message consumed from the source topic.
        """
        process_start_utc = datetime.now(UTC)

        self.logger.info(
            "Message processing started",
            extra={
                "event.name": "message.processing.started",
                "service.name": self.SERVICE_NAME,
                "messaging.system": "kafka",
                "messaging.operation": "consume",
                "messaging.destination.name": self.src_topic,
                "messaging.kafka.partition": msg.partition(),
                "messaging.kafka.message.offset": msg.offset(),
                "message.size.bytes": (
                    len(msg.value()) if msg.value() else 0
                ),
                "process.start.time": process_start_utc.isoformat(),
            },
        )

        try:
            self.producer.produce(
                topic=self.mapper_topic,
                value=msg.value(),
                key=msg.key(),
                headers=msg.headers() or [],
                callback=self._on_delivery,
            )

            self.producer.poll(0)

            process_end_utc = datetime.now(UTC)

            self.logger.info(
                "Message processing completed",
                extra={
                    "event.name": "message.processing.completed",
                    "service.name": self.SERVICE_NAME,
                    "messaging.system": "kafka",
                    "messaging.operation": "forward",
                    "messaging.destination.name": self.mapper_topic,
                    "process.start.time": process_start_utc.isoformat(),
                    "process.end.time": process_end_utc.isoformat(),
                    "process.duration.ms": int(
                        (process_end_utc - process_start_utc)
                        .total_seconds()
                        * 1000
                    ),
                },
            )

        except Exception as exc:  # noqa: BLE001
            process_end_utc = datetime.now(UTC)

            self.logger.error(
                "Message processing failed",
                extra={
                    "event.name": "message.processing.failed",
                    "service.name": self.SERVICE_NAME,
                    "messaging.system": "kafka",
                    "process.start.time": process_start_utc.isoformat(),
                    "process.end.time": process_end_utc.isoformat(),
                    "error.message": str(exc),
                },
                exc_info=True,
            )

    # ──────────────────────────────────────────────
    # Main Run Loop
    # ──────────────────────────────────────────────

    def run(self) -> None:
        """
        Run the Topic Adaptor.

        This method contains a resilient infinite loop that:
        - Creates a consumer
        - Polls messages
        - Forwards messages
        - Handles Kafka and unexpected errors
        - Retries after failures with a configurable delay
        """
        self.log_startup_banner()

        while True:
            consumer: Optional[Consumer] = None

            try:
                consumer = self._create_consumer()

                while True:
                    msg = consumer.poll(timeout=1.0)

                    if msg is None:
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(msg.error())

                    self._process_message(msg)

            except KafkaException as exc:
                self.logger.error(
                    "Kafka error – retrying",
                    extra={
                        "event.name": "kafka.retry",
                        "service.name": self.SERVICE_NAME,
                        "error.message": str(exc),
                        "retry.delay.seconds": self.retry_delay,
                    },
                )

            except Exception as exc:  # noqa: BLE001
                self.logger.error(
                    "Unexpected error – retrying",
                    extra={
                        "event.name": "unexpected.retry",
                        "service.name": self.SERVICE_NAME,
                        "error.message": str(exc),
                        "retry.delay.seconds": self.retry_delay,
                    },
                    exc_info=True,
                )

            finally:
                self.producer.flush()
                if consumer:
                    consumer.close()

            time.sleep(self.retry_delay)


def main() -> None: # pragma: no cover 
    """
    Application entry point.

    Instantiates and runs the TopicAdaptor.
    """
    AdaptorConfigValidator().validate_all()
    TopicAdaptor().run()


if __name__ == "__main__":  # pragma: no cover
    main()