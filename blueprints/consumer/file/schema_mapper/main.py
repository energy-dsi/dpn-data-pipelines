# Copyright DSI Project — Apache 2.0
# v1.0.0 Initial | v1.1.0 Kafka trigger + StepLogger 2026-05-26

"""
/home/claude/dpn-complete/consumer/file/schema_mapper/main.py

Schema Mapper (file pipeline)

Modes:
- Drain mode (Kafka-trigger)
- Continuous consumer (standalone / interval)

K8s Configuration:
- SCHEDULER_BACKEND=kafka-trigger
- PRODUCT_NAME=consumer-file
"""

from __future__ import annotations

import base64
import json
import os
import time

from confluent_kafka import Consumer, KafkaError, KafkaException
from azure.core.exceptions import AzureError, HttpResponseError
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPIError

from utils.config_validator import validate_cloud_config, validate_kafka_config
from utils.data_transection import DataTransection
from utils.exception_handler import HandleExceptions
from utils.kafka_transection import KafkaTransection
from utils.otel_logger import OtelLogger as Logging
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend
from utils.step_logger import StepLogger
from utils.topic_utils import TopicResolver, KafkaTopicManager

# Load environment variables
load_dotenv()

# Idle timeout for drain mode
_DRAIN_IDLE = int(os.getenv("DRAIN_IDLE_SECS", "15"))


class SchemaMapper:
    """
    Handles schema mapping workflow:
    1. Reads file from source storage
    2. Validates schema (stub)
    3. Moves file to target storage
    4. Publishes event to Kafka
    """

    def __init__(self):
        # Cloud & Kafka configuration
        self.cloud_provider = os.getenv("cloudProviderType", "azure")
        self.target_topic = os.getenv("targetTopicName", "")
        self.source_topic = os.getenv("mapperTopicName", "")
        self.bootstrap = os.getenv("bootstrapServer", "")

        # Storage configuration
        self.src_conn = base64.b64decode(os.getenv("mapperConnectionString", "")).decode()
        self.tgt_conn = base64.b64decode(os.getenv("targetConnectionString", "")).decode()
        self.src_container = os.getenv("mapperContainerName", "")
        self.tgt_container = os.getenv("targetContainerName", "")

        # Naming configuration
        self.org_name = os.getenv("orgName", "")
        self.schema_type = os.getenv("schemaType", "")
        self.file_name = None

        # AWS configuration (optional)
        self.aws_endpoint = os.getenv("AWS_ENDPOINT_URL") or None
        self.aws_key_id = base64.b64decode(os.getenv("AWS_ACCESS_KEY_ID", "")).decode()
        self.aws_secret = base64.b64decode(os.getenv("AWS_SECRET_ACCESS_KEY", "")).decode()
        self.aws_region = os.getenv("AWS_REGION", "us-east-1")

        # Logger
        self.logger = Logging().create_logger()

        # Validate configurations
        validate_cloud_config(
            cloud_provider=self.cloud_provider,
            azure_fields=["mapperConnectionString", "targetConnectionString"],
            logger=self.logger,
        )
        validate_kafka_config(logger=self.logger)

        # Resolve mapper topic name
        topic_resolver = TopicResolver()
        self.target_topic = topic_resolver.resolve(
            self.target_topic,
            self.target_topic,
            "trfm"
        )

        # Ensure mapper topic exists
        topic_manager = KafkaTopicManager(
            bootstrap_server=self.bootstrap,
            logger=self.logger
        )

        topic_manager.ensure_exists(self.target_topic)

        # Storage transaction handler
        self.data_trans = DataTransection(
            source_azure_conn_str=self.src_conn,
            source_container_name=self.src_container,
            target_container_name=self.tgt_container,
            source_blob_name=None,
            target_blob_name=None,
            target_azure_conn_str=self.tgt_conn,
            aws_endpoint_url=self.aws_endpoint,
            aws_access_key_id=self.aws_key_id,
            aws_secret_access_key=self.aws_secret,
            aws_region=self.aws_region,
            logger=self.logger,
        )

        # Kafka transaction handler
        self.kafka_trans = KafkaTransection(
            bootstrap_server=self.bootstrap,
            logger=self.logger
        )

    def read_file(self, file):
        """
        Read file from configured cloud storage.
        """
        self.data_trans.source_blob_name = file
        return self.data_trans.data_read(cloud_vendor=self.cloud_provider)

    def validate(self, data):
        """
        Validate schema.
        NOTE: Currently a stub implementation.
        """
        self.logger.info(
            "schema validation (stub)",
            extra={"schema_type": self.schema_type}
        )
        return True

    def move_file(self, file):
        """
        Move (copy) file to target container with normalized naming.
        """
        self.file_name = file

        response = self.data_trans.file_copy(
            cloud_vendor=self.cloud_provider,
            file_name=file,
            dest_file_name=self.file_name
        )

        return response.copied

    def publish_event(self, moved):
        """
        Publish Kafka event if file move succeeded.
        """
        if moved:
            self.kafka_trans.send_message(
                target_topic=self.target_topic,
                message={
                    "sourceType": "S3"
                    if self.cloud_provider.upper() == "AWS"
                    else self.cloud_provider.upper(),
                    "storageContainer": self.tgt_container,
                    "path": self.file_name,
                },
            )


def _process(mapper, step_log, ctx, payload):
    """
    Process a single Kafka message payload.
    """
    file_name = payload.get("path", "")
    if not file_name:
        return

    exc_h = HandleExceptions()

    step_log.step_start(ctx, "mapper_msg", extra={"file": file_name})

    try:
        # Read file
        data = mapper.read_file(file_name)

        # Validate & process
        if mapper.validate(data):
            moved = mapper.move_file(file_name)
            mapper.publish_event(moved)

        step_log.step_end(ctx, "mapper_msg", extra={"file": file_name})

    # Azure errors
    except (HttpResponseError, AzureError) as e:
        step_log.step_failed(ctx, "mapper_msg", exc=e)
        exc_h.handle_storage_exception(e, "Azure")

    # AWS errors
    except (ClientError, BotoCoreError) as e:
        step_log.step_failed(ctx, "mapper_msg", exc=e)
        exc_h.handle_storage_exception(e, "AWS S3")

    # GCP errors
    except GoogleAPIError as e:
        step_log.step_failed(ctx, "mapper_msg", exc=e)
        exc_h.handle_storage_exception(e, "GCP")

    # Generic errors
    except Exception as e:
        step_log.step_failed(ctx, "mapper_msg", exc=e)
        exc_h.handle_storage_exception(e, "")


def run(ctx: PipelineContext) -> None:
    """
    Entry point for pipeline execution.
    Decides execution mode based on trigger.
    """
    mapper = SchemaMapper()
    step_log = StepLogger(mapper.logger)

    # Log pipeline startup banner
    step_log.pipeline_banner(
        ctx,
        service_name="consumer-file-schema-mapper",
        config_summary={
            "sourceKafkaTopic": mapper.source_topic,
            "targetTopic": mapper.target_topic,
            "SCHEDULER_BACKEND": os.getenv("SCHEDULER_BACKEND", "standalone"),
            "PRODUCT_NAME": os.getenv("PRODUCT_NAME", "consumer-file"),
        },
    )

    handler = lambda p: _process(mapper, step_log, ctx, p)

    # Choose execution mode
    if ctx.triggered_by == "kafka-trigger":
        _drain(mapper, step_log, ctx, handler)
    else:
        _continuous(mapper, step_log, ctx, handler)


def _drain(mapper, step_log, ctx, handler):
    """
    Drain mode:
    Consumes messages until idle timeout is reached.
    """
    consumer = Consumer({
        "bootstrap.servers": mapper.bootstrap,
        "group.id": "consumer_file_mapper",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([mapper.source_topic])

    step_log.step_start(ctx, "drain_queue")

    processed = 0
    last_msg = time.monotonic()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Exit if idle timeout reached
                if time.monotonic() - last_msg >= _DRAIN_IDLE:
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                handler(json.loads(msg.value().decode()))
                processed += 1
                last_msg = time.monotonic()
            except Exception:
                pass

    finally:
        consumer.close()

    step_log.step_end(ctx, "drain_queue", extra={"processed": processed})


def _continuous(mapper, step_log, ctx, handler):
    """
    Continuous consumption mode:
    Keeps consuming messages indefinitely with retry logic.
    """
    retry = int(os.getenv("consumerRetryDelaySecs", "5"))

    step_log.step_start(ctx, "consumer_loop")

    while True:
        try:
            mapper.kafka_trans.consume_messages(
                source_topic=mapper.source_topic,
                group_id="consumer_file_mapper",
                handler=handler,
            )
        except Exception as exc:
            step_log.step_failed(ctx, "consumer_loop", exc=exc)
            time.sleep(retry)
            step_log.step_start(ctx, "consumer_loop")


if __name__ == "__main__":
    """
    Application entrypoint.
    Uses scheduler backend to execute pipeline.
    """
    get_backend().execute(
        run,
        pipeline_stage="schema_mapper",
        pipeline_type="file",
        pipeline_role="consumer",
    )