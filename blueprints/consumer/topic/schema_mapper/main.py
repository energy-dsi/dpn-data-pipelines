"""
Kafka Topic Schema Mapper (Consumer)

This service:
- Consumes messages from mapper topic
- Extracts metadata from headers
- Dynamically resolves target topic
- Ensures valid topics are created
- Skips invalid messages (no 'unknown' topics)

Behavior:
---------
✅ Only creates topics when ALL fields are valid
❌ Does NOT create dpn-unknown-* topics
✅ Supports camelCase + snake_case headers
"""

from __future__ import annotations

import os
import re
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv

from utils.otel_logger import OtelLogger as Logging
from utils.topic_utils import TopicResolver, KafkaTopicManager
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend
from utils.step_logger import StepLogger


load_dotenv()

_TIMEOUT = int(os.getenv("TOPIC_TASK_TIMEOUT_SECS", "600"))


def _sanitize(value: str | None, default: str = "unknown") -> str:
    """
    Sanitize string for safe Kafka topic naming.
    """
    cleaned = re.sub(r"[^A-Za-z0-9]", "", value or default).lower()
    return cleaned or default


class TopicSchemaMapper:
    """
    Kafka Consumer Schema Mapper

    Responsibilities:
    - Consume messages
    - Extract and validate headers
    - Dynamically resolve target topic
    - Produce enriched messages
    """

    def __init__(self) -> None:
        self.logger = Logging().create_logger()

        # Kafka config
        self.bootstrap = os.getenv("bootstrapServer", "")
        self.src_topic = os.getenv("mapperTopicName", "")
        self.group_id = os.getenv("mapperGroupId", "consumer_topic_mapper")
        self.retry_delay = int(os.getenv("consumerRetryDelaySecs", "5"))

        # Default metadata (used only if headers missing, but NOT for topic creation)
        self.schema_type = _sanitize(os.getenv("schemaType"))
        self.org_name = _sanitize(os.getenv("orgName"))
        self.product_type = _sanitize(os.getenv("productType"))

        # Topic resolver
        self.tr = TopicResolver()

        # Resolve mapper topic
        self.mapper_topic = self.tr.resolve(
            os.getenv("mapperTopicName"),
            self.src_topic,
            "trfm",
        )

        self.target_topic = ""

        # Topic manager
        self.km = KafkaTopicManager(
            bootstrap_server=self.bootstrap,
            logger=self.logger,
        )

        # Ensure mapper topic exists
        self.km.ensure_exists(self.mapper_topic)

        # Kafka producer
        self.producer = Producer({"bootstrap.servers": self.bootstrap})


    def _resolve_target(self, headers: dict) -> None:
        """
        Resolve target topic ONLY if metadata is valid.
        Prevents creating 'unknown' topics.
        """

        headers = {k.lower(): v for k, v in headers.items()}

        # Support both naming styles
        schema_type = _sanitize(
            headers.get("schema_type") or headers.get("schematype")
        )
        org_name = _sanitize(
            headers.get("org_name") or headers.get("orgname")
        )
        product_type = _sanitize(
            headers.get("product_type") or headers.get("producttype")
        )

        # 🚨 CRITICAL: Skip invalid metadata
        if (
            schema_type == "unknown"
            or org_name == "unknown"
            or product_type == "unknown"
        ):
            self.logger.warning(
                "Skipping message due to invalid metadata",
                extra={
                    "schema_type": schema_type,
                    "org_name": org_name,
                    "product_type": product_type,
                },
            )
            self.target_topic = ""  # ensures no produce
            return

        topic_name = (
            f"dpn-consumer-{org_name}-{schema_type}-{product_type}-target"
        )

        resolved = self.tr.resolve(
            topic_name,
            self.src_topic,
            "target",
        )

        if resolved != self.target_topic:
            self.target_topic = resolved

            self.logger.info(f"Ensuring topic exists: {self.target_topic}")

            try:
                self.km.ensure_exists(self.target_topic)
            except Exception as e:
                self.logger.error(f"Topic creation failed: {e}")


    def _on_delivery(self, err, msg) -> None:
        """Kafka delivery callback"""
        if err:
            self.logger.error("delivery failed", extra={"error": str(err)})


    def _process(self, msg, ctx: PipelineContext, step_log: StepLogger) -> None:
        """
        Process a single Kafka message.
        """

        operation = "mapper_msg"
        start_time = datetime.now(UTC)

        step_log.step_start(ctx, operation, extra={"partition": msg.partition()})

        try:
            # Decode headers
            headers = {
                k: v.decode() if isinstance(v, bytes) else v
                for k, v in (msg.headers() or [])
            }

            print("HEADERS:", headers)

            # Resolve topic
            self._resolve_target(headers)

            # ✅ Skip if invalid
            if not self.target_topic:
                self.logger.warning("Skipping produce (no valid target topic)")
                return

            print("TARGET:", self.target_topic)

            enriched_headers = [
                ("schemaType", _sanitize(headers.get("schema_type") or headers.get("schematype")).encode()),
                ("orgName", _sanitize(headers.get("org_name") or headers.get("orgname")).encode()),
                ("productType", _sanitize(headers.get("product_type") or headers.get("producttype")).encode()),
                ("processedAt", datetime.now(UTC).isoformat().encode()),
                ("offset",str(msg.offset())),
            ]

            self.producer.produce(
                topic=self.target_topic,
                value=msg.value(),
                key=msg.key(),
                headers=enriched_headers,
                callback=self._on_delivery,
            )

            self.producer.poll(0)

            duration_ms = int(
                (datetime.now(UTC) - start_time).total_seconds() * 1000
            )

            step_log.step_end(
                ctx,
                operation,
                extra={
                    "dst": self.target_topic,
                    "duration_ms": duration_ms,
                },
            )

        except Exception as exc:
            step_log.step_failed(ctx, operation, exc=exc)
            raise


    def run_window(self, ctx, step_log, stop_after=None):
        """
        Main Kafka polling loop.
        """

        deadline = time.monotonic() + stop_after if stop_after else None

        while True:
            if deadline and time.monotonic() >= deadline:
                break

            consumer = Consumer({
                "bootstrap.servers": self.bootstrap,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            })

            # ✅ Use resolved mapper topic
            consumer.subscribe([self.mapper_topic])

            try:
                while True:
                    if deadline and time.monotonic() >= deadline:
                        break

                    msg = consumer.poll(1.0)

                    if msg is None:
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(msg.error())

                    self._process(msg, ctx, step_log)

            finally:
                self.producer.flush()
                consumer.close()

            time.sleep(self.retry_delay)


def run(ctx: PipelineContext) -> None:
    """Pipeline entry point."""

    mapper = TopicSchemaMapper()
    step_log = StepLogger(mapper.logger)

    step_log.pipeline_banner(
        ctx,
        service_name="consumer-schema-mapper",
        config_summary={"topic": mapper.mapper_topic},
    )

    timeout = _TIMEOUT if ctx.triggered_by == "kafka-trigger" else None

    mapper.run_window(ctx, step_log, stop_after=timeout)


if __name__ == "__main__":
    get_backend().execute(
        run,
        pipeline_stage="schema_mapper",
        pipeline_type="topic",
        pipeline_role="consumer",
    )

# {"product_type":"eqbdpggas","processed_at":"2026-06-02T14:04:06.603385+00:00","org_name":"neso","schema_type":"eqbd"}