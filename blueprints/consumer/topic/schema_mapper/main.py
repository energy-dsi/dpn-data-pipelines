"""
Topic Schema Mapper – Producer‑Side Ingestion Pipeline.

This service consumes messages from a Kafka *mapper topic*, validates the
message payload against a data product schema, enriches each message with
metadata headers, and forwards valid messages to a configured *target topic*.

Key responsibilities:
- Consume messages from the mapper topic
- Perform schema validation
- Attach standardized Kafka headers
- Forward messages to the target topic
- Retry on recoverable Kafka errors

Designed to run continuously as a long‑lived service.
"""

from __future__ import annotations

import os
import re
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv

from utils.otel_logger import OtelLogger as Logging
# from utils.topic_utils import self.kafka_topic_manager.ensure_exists, self.topic_resolver.resolve

from utils.topic_utils import TopicResolver, KafkaTopicManager
from utils.topic_consumer_config_validator import SchemaMapperValidator


load_dotenv()


def sanitize_identifier(value: str, default: str = "unknown") -> str:
    """
    Sanitize identifiers used in topic names and Kafka headers.

    Keeps only alphanumeric characters and forces lowercase.
    Ensures Kafka‑safe and filename‑safe identifiers.

    Args:
        value: Raw identifier value.
        default: Fallback value if input is empty or invalid.

    Returns:
        Sanitized lowercase string.
    """
    cleaned = re.sub(r"[^A-Za-z0-9]", "", value or default)
    return cleaned.lower() or default


class TopicSchemaMapper:
    """
    Topic Schema Mapper service.

    This class encapsulates the full lifecycle of the schema‑mapping pipeline:
    - configuration loading
    - Kafka topic preparation
    - Kafka consumer & producer management
    - message validation and forwarding
    - fault handling and retry behaviour

    The service is intentionally stateful and long‑running.
    """

    def __init__(self) -> None:
        """
        Initialize the mapper service.

        Loads environment configuration, ensures required Kafka topics
        exist, initializes logging, and creates the Kafka producer.
        """
        self.bootstrap_server: str = os.getenv("bootstrapServer", "")
        self.topic_resolver = TopicResolver()
        self.logger = Logging().create_logger()
        self.kafka_topic_manager = KafkaTopicManager(bootstrap_server = self.bootstrap_server, logger = self.logger)
        self.src_topic = os.getenv("mapperTopicName", "")

        self.group_id = os.getenv(
            "mapperGroupId",
            "producer-topic-schema-mapper",
        )

        self.retry_delay = int(
            os.getenv("consumerRetryDelaySecs", "5")
        )

        self.mapper_topic = self.topic_resolver.resolve(
            os.getenv("mapperTopicName"),
            self.src_topic,
            "trfm",
        )

        self._create_producer()

    # ──────────────────────────────
    # Configuration
    # ──────────────────────────────
    def _create_target_topic(self) -> None:
        """
        Create the topic name dynamically.
        """


        # Resolve mapper and target topics.
        # If not explicitly provided, they are auto‑derived from srcTopicName.

        target_topic_create = "dpn-consumer-"+self.schema_type + "-" + self.org_name + "-" + self.product_type + "-target"

        self.target_topic = self.topic_resolver.resolve(
            target_topic_create,
            self.src_topic,
            "target",
        )

        self._log_startup_banner()

    # ──────────────────────────────
    # Kafka Setup
    # ──────────────────────────────
    def _ensure_topics(self) -> None:
        """
        Ensure required Kafka topics exist on the broker.

        Topics are created if missing.
        This ensures idempotent startup behavior.
        """
        self.kafka_topic_manager.ensure_exists(
            self.mapper_topic
        )
        self.kafka_topic_manager.ensure_exists(
            self.target_topic
        )

    def _create_consumer(self) -> Consumer:
        """
        Create and return a new Kafka consumer instance.

        Consumers are recreated on failure to guarantee a clean state
        during retries.
        """
        return Consumer(
            {
                "bootstrap.servers": self.bootstrap_server,
                "group.id": self.group_id,
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            }
        )

    def _create_producer(self) -> None:
        """
        Create a Kafka producer instance.

        The producer is stable across retries and flushed on shutdown.
        """
        self.producer = Producer(
            {"bootstrap.servers": self.bootstrap_server}
        )

    # ──────────────────────────────
    # Logging Helpers
    # ──────────────────────────────
    def _log_startup_banner(self) -> None:
        """
        Log a formatted startup banner with effective configuration.

        This makes configuration validation visible at runtime
        and simplifies operational troubleshooting.
        """
        lines = [
            "------ Producer - Topic Schema Mapper Config ------",
            f"srcTopicName        : {self.src_topic}",
            f"mapperTopicName     : {self.mapper_topic}",
            f"targetTopicName     : {self.target_topic}",
            f"bootstrapServer     : {self.bootstrap_server}",
            f"mapperGroupId       : {self.group_id}",
            f"schemaType          : {self.schema_type}",
            f"orgName             : {self.org_name}",
            f"productType         : {self.product_type}",
        ]

        width = max(len(l) for l in lines) + 4
        border = "+" + "-" * (width - 2) + "+"

        self.logger.info(border)
        for line in lines:
            self.logger.info(f"| {line.ljust(width - 4)} |")
        self.logger.info(border)

    # ──────────────────────────────
    # Message Processing
    # ──────────────────────────────
    def _schema_validation(self, value: bytes) -> bool:
        """
        Validate message content against the configured schema.

        Currently a placeholder — always returns True.
        Designed to be replaced by real schema validation logic.

        Args:
            value: Raw Kafka message payload.

        Returns:
            True if message is valid, otherwise False.
        """
        self.logger.info(
            "Schema validation invoked",
            extra={
                "event.name": "schema.validation.started",
                "schema.type": self.schema_type,
                "message.size.bytes": len(value),
            },
        )
        return True

    def _build_headers(self, offset: int):
        """
        Build standardized Kafka message headers.

        Headers enable downstream services to reconstruct filenames,
        schemas, and lineage metadata.

        Args:
            offset: Kafka offset of the consumed message.

        Returns:
            List of Kafka header key/value tuples.
        """
        return [
            ("schemaType", self.schema_type.encode()),
            ("orgName", self.org_name.encode()),
            ("productType", self.product_type.encode()),
            ("offset", str(offset).encode()),
        ]

    def _delivery_callback(self, err, msg) -> None:
        """
        Kafka producer delivery callback.

        Logs delivery success or failure for observability.
        """
        if err:
            self.logger.error(
                "Kafka message delivery failed",
                extra={
                    "event.name": "message.delivery.failed",
                    "error.message": str(err),
                },
            )
        else:
            self.logger.info(
                "Kafka message delivered",
                extra={
                    "event.name": "message.delivery.completed",
                    "messaging.destination.name": msg.topic(),
                    "messaging.kafka.partition": msg.partition(),
                    "messaging.kafka.message.offset": msg.offset(),
                },
            )

    # ──────────────────────────────
    # Main Processing Loop
    # ──────────────────────────────
    def run(self) -> None:
        """
        Run the mapper service indefinitely.

        Handles consumer lifecycle, retries on failure, and enforces
        a delay between restarts.
        """
        while True:
            consumer = self._create_consumer()

            try:
                consumer.subscribe([self.mapper_topic])
                self.logger.info(
                    "Subscribed to mapper topic",
                    extra={
                        "event.name": "consumer.subscribed",
                        "messaging.destination.name": self.mapper_topic,
                    },
                )

                self._consume_loop(consumer)

            except KafkaException as exc:
                self.logger.error(
                    "Kafka error – retrying",
                    extra={
                        "event.name": "kafka.retry",
                        "error.message": str(exc),
                        "retry.delay.seconds": self.retry_delay,
                    },
                )

            except Exception as exc:  # noqa: BLE001
                self.logger.error(
                    "Unexpected error – retrying",
                    extra={
                        "event.name": "unexpected.retry",
                        "error.message": str(exc),
                        "retry.delay.seconds": self.retry_delay,
                    },
                    exc_info=True,
                )

            finally:
                self.producer.flush()
                consumer.close()

            time.sleep(self.retry_delay)

    def _consume_loop(self, consumer: Consumer) -> None:
        """
        Poll Kafka and dispatch messages for processing.

        Args:
            consumer: Active Kafka consumer.
        """
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            self._process_message(msg)

    def _process_message(self, msg) -> None:
        """
        Process a single Kafka message end‑to‑end.

        Handles validation, header enrichment, forwarding,
        and metrics/logging.

        Args:
            msg: Kafka message object.
        """
        value = msg.value()
        start = datetime.now(UTC)
        header_dict = {key: value.decode() for key, value in msg.headers()}

        self.org_name = header_dict.get("orgName")
        self.schema_type = header_dict.get("schemaType")
        self.product_type = header_dict.get("productType")

        self._create_target_topic()
        self._ensure_topics()

        if not value:
            self.logger.warning(
                "Empty message – skipped",
                extra={"event.name": "message.empty.skipped"},
            )
            return

        if not self._schema_validation(value):
            self.logger.warning(
                "Schema validation failed",
                extra={"event.name": "schema.validation.failed"},
            )
            return

        headers = self._build_headers(msg.offset())

        self.producer.produce(
            topic=self.target_topic,
            value=value,
            key=msg.key(),
            headers=headers,
            callback=self._delivery_callback,
        )
        self.producer.poll(0)

        end = datetime.now(UTC)

        self.logger.info(
            "Message processing completed",
            extra={
                "event.name": "message.processing.completed",
                "process.duration.ms": int(
                    (end - start).total_seconds() * 1000
                ),
            },
        )

if __name__ == "__main__":  # pragma: no cover
    SchemaMapperValidator().validate_all()
    TopicSchemaMapper().run()
