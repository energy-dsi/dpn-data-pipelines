# Copyright DSI Project — Apache 2.0
# v1.0.0 Initial | v1.1.0 Kafka trigger + StepLogger 2026-05-26

"""
/home/claude/dpn-complete/consumer/topic/extractor/main.py

Kafka topic extractor (consumer).
This service consumes messages from a source topic and forwards them
to a mapper (transform) topic.

Kubernetes Environment Variables:
    SCHEDULER_BACKEND=kafka-trigger
    PRODUCT_NAME=consumer-topic
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from opentelemetry import context as _otel_context, propagate as _otel_propagate
from dotenv import load_dotenv


from dpn_observability_sdk.otel_logger import OtelLogger as Logging
from dpn_observability_sdk.otel_tracer import OtelTracer
from dpn_observability_sdk.otel_metrics import OtelMetrics
from dpn_observability_sdk.otel_instrumentation import traced, timed_metric
from dpn_observability_sdk.heartbeat import HeartbeatLogger
from utils.topic_utils import TopicResolver, KafkaTopicManager
from utils.pipeline_context import PipelineContext
from utils.scheduler_backend import get_backend
from utils.step_logger import StepLogger

load_dotenv()

# Timeout used when running in triggered mode
_TIMEOUT = int(os.getenv("TOPIC_TASK_TIMEOUT_SECS", "600"))


class TopicForwarder:
    """
    Kafka Topic Forwarder.

    Responsible for:
    - Consuming messages from the source topic
    - Forwarding them to the mapper topic
    - Handling Kafka producer/consumer lifecycle
    - Logging and step tracking
    """

    SERVICE_NAME = "consumer-topic-extractor"

    def __init__(self) -> None:
        """Initialize Kafka producer, topic configuration, and logging."""

        # Initialize OpenTelemetry
        self.tracer = OtelTracer.initialize(
            service_name="consumer-topic-extractor",
            service_version="1.0.0"
        )
        self.meter = OtelMetrics.initialize(
            service_name="consumer-topic-extractor",
            service_version="1.0.0"
        )
        
        # Create metrics
        self.messages_forwarded = self.meter.create_counter(
            name="messages_forwarded_total",
            description="Total messages forwarded",
            unit="1",
        )
        
        self.forward_duration = self.meter.create_histogram(
            name="message_forward_duration",
            description="Message forwarding duration",
            unit="ms",
        )
        
        self.messages_consumed = self.meter.create_counter(
            name="messages_consumed_total",
            description="Total messages consumed from source topic",
            unit="1",
        )

        # Initialize heartbeat logger (will be started in main)
        self.heartbeat: HeartbeatLogger | None = None

        self.logger = Logging().create_logger()

        # Kafka configuration from environment
        self.bootstrap = os.getenv("bootstrapServer", "")
        self.src_topic = os.getenv("srcTopicName", "")
        self.group_id = os.getenv("srcGroupId", self.SERVICE_NAME)
        self.retry_delay = int(os.getenv("consumerRetryDelaySecs", "5"))

        # Resolve mapper topic name
        topic_resolver = TopicResolver()
        self.mapper_topic = topic_resolver.resolve(
            os.getenv("mapperTopicName"),
            self.src_topic,
            "trfm"
        )

        # Ensure mapper topic exists
        topic_manager = KafkaTopicManager(
            bootstrap_server=self.bootstrap,
            logger=self.logger
        )
        topic_manager.ensure_exists(self.src_topic)
        topic_manager.ensure_exists(self.mapper_topic)

        # Kafka producer initialization
        self.producer = Producer({"bootstrap.servers": self.bootstrap})

        self.logger.info(
            "extractor initialised",
            extra={
                "srcTopicName": self.src_topic,
                "mapperTopicName": self.mapper_topic,
                "srcGroupId": self.group_id,
                "PRODUCT_NAME": os.getenv("PRODUCT_NAME"),
                "SCHEDULER_BACKEND": os.getenv("SCHEDULER_BACKEND"),
                "TOPIC_TASK_TIMEOUT_SECS": os.getenv("TOPIC_TASK_TIMEOUT_SECS"),
            }
        )


    def _on_delivery(self, err, msg) -> None:
        """
        Kafka delivery callback.

        Args:
            err: Delivery error, if any.
            msg: Kafka message metadata.
        """
        if err:
            self.logger.error(
                "delivery failed",
                extra={"error": str(err)}
            )
    @traced(span_name="forward_message")
    def _forward(self, msg, ctx: PipelineContext, step_log: StepLogger) -> None:
        """
        Forward a single Kafka message to the mapper topic.

        Args:
            msg: Kafka message received from source topic.
            ctx: Pipeline execution context.
            step_log: StepLogger instance for structured logging.
        """
        # Empty context creates a new root span → new trace_id per message.
        # Without this, all messages in the same consumer_window share one trace_id.
        with self.tracer.start_as_current_span(
            "forward_message", context=_otel_context.Context()
        ) as span:
            operation = "forward"
            start_time = datetime.now(UTC)
            
            # Add span attributes
            span.set_attribute("kafka.source_topic", msg.topic())
            span.set_attribute("kafka.destination_topic", self.mapper_topic)
            span.set_attribute("kafka.partition", msg.partition())
            span.set_attribute("kafka.offset", msg.offset())
            span.set_attribute("message.key", str(msg.key()) if msg.key() else "null")
            
            # Log step start with partition and offset
            step_log.step_start(
                ctx,
                operation,
                extra={
                    "partition": msg.partition(),
                    "offset": msg.offset()
                }
            )

            try:
                # schema_mapper pod can restore the same trace_id.
                carrier: dict[str, str] = {}
                _otel_propagate.inject(carrier)
                outgoing_headers = list(msg.headers() or []) + [
                    (k, v.encode() if isinstance(v, str) else v)
                    for k, v in carrier.items()
                ]
                # Produce message to mapper topic
                self.producer.produce(
                    topic=self.mapper_topic,
                    value=msg.value(),
                    key=msg.key(),
                    headers=outgoing_headers,
                    callback=self._on_delivery
                )

                # Trigger delivery callback
                self.producer.poll(0)

                # Calculate processing duration in milliseconds
                duration_ms = int(
                    (datetime.now(UTC) - start_time).total_seconds() * 1000
                )
                
                # Record metrics
                self.messages_forwarded.add(1, {
                    "source_topic": msg.topic(),
                    "destination_topic": self.mapper_topic,
                    "status": "success"
                })
                
                self.forward_duration.record(duration_ms, {
                    "source_topic": msg.topic(),
                    "destination_topic": self.mapper_topic
                })
                
                span.set_attribute("forward.status", "success")
                span.set_attribute("forward.duration_ms", duration_ms)

                # Log successful completion
                step_log.step_end(
                    ctx,
                    operation,
                    extra={
                        "dst": self.mapper_topic,
                        "duration_ms": duration_ms
                    }
                )

            except Exception as exc:
                # Record error metrics
                self.messages_forwarded.add(1, {
                    "source_topic": msg.topic(),
                    "destination_topic": self.mapper_topic,
                    "status": "error"
                })
                
                span.set_attribute("forward.status", "error")
                span.set_attribute("error.type", type(exc).__name__)
                span.record_exception(exc)
                
                # Log failure and rethrow exception
                step_log.step_failed(ctx, operation, exc=exc)
                raise

    @traced(span_name="consumer_window")
    @timed_metric("consumer_window_duration", "Duration of consumer window")
    def run_window(
        self,
        ctx: PipelineContext,
        step_log: StepLogger,
        stop_after: int | None = None
    ) -> None:
        """
        Run the consumer loop for a specified duration or indefinitely.

        Args:
            ctx: Pipeline execution context.
            step_log: StepLogger instance.
            stop_after: Optional time limit (in seconds).
        """
        with self.tracer.start_as_current_span("consumer_window") as span:
            span.set_attribute("kafka.source_topic", self.src_topic)
            span.set_attribute("kafka.mapper_topic", self.mapper_topic)
            span.set_attribute("kafka.group_id", self.group_id)
            span.set_attribute("window.timeout_seconds", stop_after or 0)
            
            messages_processed = 0
            
            # Calculate deadline if timeout is specified
            deadline = time.monotonic() + stop_after if stop_after else None

            while True:
                # Exit if deadline reached
                if deadline and time.monotonic() >= deadline:
                    break

                # Create Kafka consumer
                consumer = Consumer({
                    "bootstrap.servers": self.bootstrap,
                    "group.id": self.group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": True,
                })

                consumer.subscribe([self.src_topic])

                try:
                    while True:
                        if deadline and time.monotonic() >= deadline:
                            break

                        msg = consumer.poll(timeout=1.0)

                        # No message received
                        if msg is None:
                            continue

                        # Handle Kafka error messages
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                continue
                            raise KafkaException(msg.error())

                        # Record message consumption
                        self.messages_consumed.add(1, {
                            "source_topic": self.src_topic,
                            "partition": str(msg.partition())
                        })
                        
                        messages_processed += 1

                        # Forward valid message
                        self._forward(msg, ctx, step_log)

                except KafkaException as kafka_error:
                    span.set_attribute("error.type", "kafka_error")
                    span.record_exception(kafka_error)
                    self.logger.error(
                        "kafka error",
                        extra={
                            "error": str(kafka_error),
                            "retry": self.retry_delay
                        }
                    )

                except Exception as unexpected_error:
                    span.set_attribute("error.type", "unexpected_error")
                    span.record_exception(unexpected_error)
                    self.logger.error(
                        "unexpected",
                        extra={"error": str(unexpected_error)},
                        exc_info=True
                    )

                finally:
                    # Ensure all buffered messages are sent
                    self.producer.flush()

                    # Close consumer cleanly
                    consumer.close()

                # Exit if deadline reached after cleanup
                if deadline and time.monotonic() >= deadline:
                    break

                # Retry delay before restarting consumer loop
                time.sleep(self.retry_delay)
            
            span.set_attribute("messages.processed", messages_processed)
            span.set_attribute("window.status", "completed")


@traced(span_name="consumer_topic_extractor_pipeline")
@timed_metric("pipeline_total_duration", "Total pipeline execution time")
def run(ctx: PipelineContext) -> None:
    """
    Entry point for the topic extractor pipeline stage.

    Args:
        ctx: Pipeline execution context.
    """
    tracer = OtelTracer.get_tracer(__name__)
    
    with tracer.start_as_current_span("extractor_pipeline") as span:
        span.set_attribute("pipeline.type", "consumer-topic-extractor")
        span.set_attribute("pipeline.triggered_by", ctx.triggered_by)
        span.set_attribute("pipeline.run_id", ctx.run_id)
        
        forwarder = TopicForwarder()
        step_log = StepLogger(forwarder.logger)

        # Log pipeline start banner
        step_log.pipeline_banner(
            ctx,
            service_name="consumer-topic-extractor",
            config_summary={
                "srcTopicName": forwarder.src_topic,
                "mapperTopicName": forwarder.mapper_topic,
                "SCHEDULER_BACKEND": os.getenv("SCHEDULER_BACKEND", "standalone"),
                "PRODUCT_NAME": os.getenv("PRODUCT_NAME", "consumer-topic"),
            },
        )

        operation = "extractor_window"

        # Start main extraction step
        step_log.step_start(ctx, operation)

        # Apply timeout only for kafka-trigger mode
        timeout = _TIMEOUT if ctx.triggered_by in ["kafka-trigger", "interval"] else None
        span.set_attribute("pipeline.timeout_seconds", timeout or 0)

        try:
            forwarder.run_window(ctx, step_log, stop_after=timeout)
            step_log.step_end(ctx, operation)
            span.set_attribute("pipeline.status", "success")

        except Exception as exc:
            span.set_attribute("pipeline.status", "error")
            span.set_attribute("error.type", type(exc).__name__)
            span.record_exception(exc)
            step_log.step_failed(ctx, operation, exc=exc)
            raise


if __name__ == "__main__":
    # Execute pipeline using configured scheduler backend
    
    temp_forwarder = TopicForwarder()
    heartbeat = HeartbeatLogger(
        logger=temp_forwarder.logger,
        component_name="consumer-topic-extractor",
        metadata={
            "source_topic": temp_forwarder.src_topic,
            "mapper_topic": temp_forwarder.mapper_topic,
            "scheduler_backend": os.getenv("SCHEDULER_BACKEND", "standalone"),
        },
    )
    heartbeat.start()
    get_backend().execute(
        run,
        pipeline_stage="extractor",
        pipeline_type="topic",
        pipeline_role="consumer",
    )