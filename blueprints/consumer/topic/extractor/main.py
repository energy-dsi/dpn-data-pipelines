"""
Topic Extractor – Kafka Consumer (Class-Based Implementation)

This module implements a Kafka consumer service that listens to a source topic
(`srcTopicName`) and forwards each consumed message to a downstream topic
(`mapperTopicName`), preserving all headers and message metadata.

Key Responsibilities:
---------------------
- Consume messages from a source Kafka topic
- Forward messages to a downstream topic
- Preserve all message headers (schema metadata, offsets, etc.)
- Log processing lifecycle with observability metadata
- Retry on failure with configurable delay

Topic Flow:
-----------
srcTopicName  →  [TopicExtractor]  →  mapperTopicName

Environment Variables:
----------------------
bootstrapServer         : Kafka broker address
srcTopicName            : Source Kafka topic
mapperTopicName         : Destination topic (auto-derived if empty)
srcGroupId              : Kafka consumer group ID
consumerRetryDelaySecs  : Retry delay in seconds (default: 5)
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv

from utils.otel_logger import OtelLogger as Logging

from utils.topic_utils import TopicResolver, KafkaTopicManager

from utils.topic_consumer_config_validator import ExtractorValidator

load_dotenv()


class TopicExtractor:
    """
    Kafka Topic Extractor Service.

    This class encapsulates the logic required to:
    - Subscribe to a Kafka topic
    - Consume messages continuously
    - Forward messages to another topic while preserving headers
    - Provide structured logging for observability
    """

    def __init__(self) -> None:
        """
        Initialize the TopicExtractor service.

        - Reads configuration from environment variables
        - Resolves destination topic
        - Ensures destination topic exists
        - Initializes Kafka producer
        """

        # Initialize structured logger
        self.logger = Logging().create_logger()

        self.topic_resolver = TopicResolver()

        # ── Load Environment Variables ───────────────────────────
        self.bootstrap_server: str = os.getenv("bootstrapServer", "")
        self.src_topic: str = os.getenv("srcTopicName", "")

        self.group_id: str = os.getenv(
            "srcGroupId", "consumer-topic-extractor"
        )

        self.retry_delay: int = int(
            os.getenv("consumerRetryDelaySecs", "5")
        )

        self.kafka_topic_manager = KafkaTopicManager(bootstrap_server = self.bootstrap_server, logger = self.logger)


        # Resolve downstream topic (auto-derive if not provided)
        self.mapper_topic: str = self.topic_resolver.resolve(
            os.getenv("mapperTopicName"),
            self.src_topic,
            "trfm",
        )

        # Ensure destination topic exists in Kafka broker
        self.kafka_topic_manager.ensure_exists(
            self.mapper_topic,
        )

        # Log startup configuration
        self._log_startup_banner()

        # Initialize Kafka producer
        self.producer = Producer(
            {"bootstrap.servers": self.bootstrap_server}
        )

    # ────────────────────────────────────────────────────────────
    # Logging Helpers
    # ────────────────────────────────────────────────────────────
    def _log_startup_banner(self) -> None:
        """
        Log a formatted startup banner containing configuration details.
        """

        lines = [
            "---------- Consumer - Topic Extractor Config Information ----------",
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

    # ────────────────────────────────────────────────────────────
    # Kafka Producer Callback
    # ────────────────────────────────────────────────────────────
    def _on_delivery(self, err, msg) -> None:
        """
        Kafka delivery callback executed after message production.

        Args:
            err: Error object if delivery failed, otherwise None
            msg: Kafka message metadata
        """

        if err:
            # Log delivery failure
            self.logger.error(
                "Kafka message delivery failed",
                extra={
                    "event.name": "message.delivery.failed",
                    "service.name": "consumer-topic-extractor",
                    "messaging.system": "kafka",
                    "error.message": str(err),
                },
            )
        else:
            # Log successful delivery
            self.logger.info(
                "Kafka message delivered",
                extra={
                    "event.name": "message.delivery.completed",
                    "service.name": "consumer-topic-extractor",
                    "messaging.system": "kafka",
                    "messaging.destination.name": msg.topic(),
                    "messaging.kafka.partition": msg.partition(),
                    "messaging.kafka.message.offset": msg.offset(),
                },
            )

    # ────────────────────────────────────────────────────────────
    # Message Processing
    # ────────────────────────────────────────────────────────────
    def _process_message(self, msg) -> None:
        """
        Process a single Kafka message.

        Steps:
        - Log processing start
        - Forward message to destination topic with headers intact
        - Log success or failure

        Args:
            msg: Kafka message object
        """

        process_start_utc = datetime.now(UTC)

        # Log processing start
        self.logger.info(
            "Message processing started",
            extra={
                "event.name": "message.processing.started",
                "service.name": "consumer-topic-extractor",
                "messaging.system": "kafka",
                "messaging.operation": "consume",
                "messaging.destination.name": self.src_topic,
                "messaging.kafka.partition": msg.partition(),
                "messaging.kafka.message.offset": msg.offset(),
                "message.size.bytes": len(msg.value()) if msg.value() else 0,
                "process.start.time": process_start_utc.isoformat(),
            },
        )

        try:
            # Forward message while preserving headers and key
            self.producer.produce(
                topic=self.mapper_topic,
                value=msg.value(),
                key=msg.key(),
                headers=msg.headers() or [],
                callback=self._on_delivery,
            )

            # Trigger delivery callback events
            self.producer.poll(0)

            process_end_utc = datetime.now(UTC)

            # Log successful processing
            self.logger.info(
                "Message processing completed",
                extra={
                    "event.name": "message.processing.completed",
                    "service.name": "consumer-topic-extractor",
                    "messaging.system": "kafka",
                    "messaging.operation": "forward",
                    "messaging.destination.name": self.mapper_topic,
                    "messaging.kafka.partition": msg.partition(),
                    "messaging.kafka.message.offset": msg.offset(),
                    "process.start.time": process_start_utc.isoformat(),
                    "process.end.time": process_end_utc.isoformat(),
                    "process.duration.ms": int(
                        (process_end_utc - process_start_utc).total_seconds() * 1000
                    ),
                },
            )

        except Exception as exc:  # noqa: BLE001
            process_end_utc = datetime.now(UTC)

            # Log failure with stack trace
            self.logger.error(
                "Message processing failed",
                extra={
                    "event.name": "message.processing.failed",
                    "service.name": "consumer-topic-extractor",
                    "messaging.system": "kafka",
                    "messaging.destination.name": self.mapper_topic,
                    "messaging.kafka.partition": msg.partition(),
                    "messaging.kafka.message.offset": msg.offset(),
                    "process.start.time": process_start_utc.isoformat(),
                    "process.end.time": process_end_utc.isoformat(),
                    "process.duration.ms": int(
                        (process_end_utc - process_start_utc).total_seconds() * 1000
                    ),
                    "error.message": str(exc),
                },
                exc_info=True,
            )

    # ────────────────────────────────────────────────────────────
    # Main Consumer Loop
    # ────────────────────────────────────────────────────────────
    def run(self) -> None:
        """
        Start the Kafka consumer loop.

        Behavior:
        - Continuously consume messages
        - Handle Kafka errors gracefully
        - Retry with delay on failure
        """

        while True:
            # Create a new consumer instance on each retry cycle
            consumer = Consumer(
                {
                    "bootstrap.servers": self.bootstrap_server,
                    "group.id": self.group_id,
                    "auto.offset.reset": "latest",
                    "enable.auto.commit": True,
                }
            )

            # Subscribe to source topic
            consumer.subscribe([self.src_topic])

            self.logger.info(
                "Subscribed to source topic",
                extra={
                    "event.name": "consumer.subscribed",
                    "service.name": "consumer-topic-extractor",
                    "messaging.system": "kafka",
                    "messaging.destination.name": self.src_topic,
                },
            )

            try:
                while True:
                    # Poll for messages
                    msg = consumer.poll(timeout=1.0)

                    if msg is None:
                        continue

                    # Handle Kafka-level errors
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(msg.error())

                    # Process valid message
                    self._process_message(msg)

            except KafkaException as exc:
                # Kafka-specific error handling
                self.logger.error(
                    "Kafka error – retrying",
                    extra={
                        "event.name": "kafka.retry",
                        "service.name": "consumer-topic-extractor",
                        "messaging.system": "kafka",
                        "error.message": str(exc),
                        "retry.delay.seconds": self.retry_delay,
                    },
                )

            except Exception as exc:  # noqa: BLE001
                # Catch-all for unexpected failures
                self.logger.error(
                    "Unexpected error – retrying",
                    extra={
                        "event.name": "unexpected.retry",
                        "service.name": "consumer-topic-extractor",
                        "error.message": str(exc),
                        "retry.delay.seconds": self.retry_delay,
                    },
                    exc_info=True,
                )

            finally:
                # Ensure resources are cleaned up
                self.producer.flush()
                consumer.close()

            # Wait before retrying
            time.sleep(self.retry_delay)


# ───────────────────────────────────────────────────────────────
# Entry Point
# ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    Application entry point.

    Instantiates and starts the TopicExtractor service.
    """
    ExtractorValidator().validate_all()
    TopicExtractor().run()