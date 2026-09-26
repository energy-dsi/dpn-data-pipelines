import json
import pytest
import time
from unittest.mock import MagicMock, patch

from utils.kafka_trigger import (
    KafkaTriggerBackend,
    StatusPublisher,
    build_trigger_message,
    publish_trigger,
    get_status_start_offset,
    poll_for_status,
)


# -----------------------------------------------------------
# Fixtures
# -----------------------------------------------------------

@pytest.fixture
def mock_logger():
    logger = MagicMock()
    return logger


@pytest.fixture
def mock_ctx():
    ctx = MagicMock()
    ctx.pipeline_stage = "dev"
    ctx.pipeline_type = "typeA"
    ctx.pipeline_role = "roleA"
    ctx.run_id = "run123"
    ctx.dag_run_id = "dagrun"
    ctx.dag_id = "dag"
    ctx.mode = "automatic"
    ctx.as_log_extra.return_value = {}
    return ctx


# -----------------------------------------------------------
# StatusPublisher tests
# -----------------------------------------------------------

@patch("utils.kafka_trigger.Producer")
def test_status_publisher_publish(mock_producer_cls, mock_logger, mock_ctx):
    producer = MagicMock()
    mock_producer_cls.return_value = producer

    pub = StatusPublisher("localhost", mock_logger)

    pub.publish(mock_ctx, status="completed", duration_ms=123)

    assert producer.produce.called
    assert producer.flush.called
    mock_logger.info.assert_called()


# -----------------------------------------------------------
# _execute_pipeline tests
# -----------------------------------------------------------

@patch("utils.kafka_trigger.PipelineContext")
def test_execute_pipeline_success(mock_ctx_cls):
    backend = KafkaTriggerBackend()

    ctx = MagicMock()
    ctx.mode = "automatic"
    ctx.run_id = "run123"
    mock_ctx_cls.from_trigger_message.return_value = ctx

    status_pub = MagicMock()

    def run_fn(c):
        pass

    backend._execute_pipeline(
        payload={"run_id": "run123"},
        run_fn=run_fn,
        status_pub=status_pub,
        pipeline_stage="dev",
        pipeline_type="t",
        pipeline_role="r",
    )

    status_pub.publish.assert_called()
    assert backend._last_processed_run_id == "run123"


@patch("utils.kafka_trigger.PipelineContext")
def test_execute_pipeline_failure(mock_ctx_cls):
    backend = KafkaTriggerBackend()

    ctx = MagicMock()
    ctx.mode = "automatic"
    mock_ctx_cls.from_trigger_message.return_value = ctx

    status_pub = MagicMock()

    def run_fn(c):
        raise ValueError("boom")

    backend._execute_pipeline(
        payload={"run_id": "runX"},
        run_fn=run_fn,
        status_pub=status_pub,
        pipeline_stage="dev",
        pipeline_type="t",
        pipeline_role="r",
    )

    status_pub.publish.assert_called()
    assert backend._last_processed_run_id == "runX"


@patch("utils.kafka_trigger.PipelineContext")
def test_execute_pipeline_already_running(mock_ctx_cls):
    backend = KafkaTriggerBackend()
    backend._pipeline_running.set()

    ctx = MagicMock()
    mock_ctx_cls.from_trigger_message.return_value = ctx

    status_pub = MagicMock()

    backend._execute_pipeline(
        payload={"run_id": "run1"},
        run_fn=lambda x: x,
        status_pub=status_pub,
        pipeline_stage="dev",
        pipeline_type="t",
        pipeline_role="r",
    )

    status_pub.publish.assert_called()


@patch("utils.kafka_trigger.PipelineContext")
def test_execute_pipeline_duplicate_run(mock_ctx_cls):
    backend = KafkaTriggerBackend()
    backend._last_processed_run_id = "run1"

    ctx = MagicMock()
    mock_ctx_cls.from_trigger_message.return_value = ctx

    status_pub = MagicMock()

    backend._execute_pipeline(
        payload={"run_id": "run1"},
        run_fn=lambda x: x,
        status_pub=status_pub,
        pipeline_stage="dev",
        pipeline_type="t",
        pipeline_role="r",
    )

    status_pub.publish.assert_called()


@patch("utils.kafka_trigger.PipelineContext")
def test_execute_pipeline_manual_mode(mock_ctx_cls):
    backend = KafkaTriggerBackend()

    ctx = MagicMock()
    ctx.mode = "manual"
    mock_ctx_cls.from_trigger_message.return_value = ctx

    status_pub = MagicMock()

    backend._execute_pipeline(
        payload={"run_id": "run2"},
        run_fn=lambda x: x,
        status_pub=status_pub,
        pipeline_stage="dev",
        pipeline_type="t",
        pipeline_role="r",
    )

    status_pub.publish.assert_called()


# -----------------------------------------------------------
# _wait_for_trigger tests
# -----------------------------------------------------------

@patch("utils.kafka_trigger.Consumer")
def test_wait_for_trigger_valid(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer

    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = json.dumps({
        "stage": "dev",
        "pipeline_type": "t",
        "pipeline_role": "r",
        "product": "",
        "run_id": "123"
    }).encode()

    consumer.poll.side_effect = [msg]

    backend = KafkaTriggerBackend()
    backend._product = ""

    result = backend._wait_for_trigger(
        group_id="g",
        pipeline_stage="dev",
        pipeline_type="t",
        pipeline_role="r",
    )

    assert result["run_id"] == "123"
    consumer.commit.assert_called()


@patch("utils.kafka_trigger.Consumer")
def test_wait_for_trigger_invalid_json(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer

    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = b"invalid-json"

    consumer.poll.side_effect = [msg, None]

    backend = KafkaTriggerBackend()

    # break loop by raising after second poll
    with pytest.raises(StopIteration):
        consumer.poll.side_effect = [msg, StopIteration]
        backend._wait_for_trigger(
            group_id="g",
            pipeline_stage="dev",
            pipeline_type="t",
            pipeline_role="r",
        )


# -----------------------------------------------------------
# Helper function tests
# -----------------------------------------------------------

def test_build_trigger_message():
    msg = build_trigger_message(
        stage="dev",
        pipeline_type="t",
        pipeline_role="r",
        product="p",
        run_id="1",
        dag_run_id="dr",
        dag_id="d",
    )

    assert msg["stage"] == "dev"
    assert msg["run_id"] == "1"


@patch("utils.kafka_trigger.Producer")
def test_publish_trigger(mock_producer_cls):
    producer = MagicMock()
    mock_producer_cls.return_value = producer

    message = {"run_id": "1"}

    publish_trigger(message, "localhost")

    assert producer.produce.called
    assert producer.flush.called


@patch("utils.kafka_trigger.Consumer")
def test_get_status_start_offset(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer

    consumer.get_watermark_offsets.return_value = (0, 10)

    offset = get_status_start_offset("localhost")

    assert offset == 10


@patch("utils.kafka_trigger.Consumer")
def test_get_status_start_offset_exception(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer

    consumer.get_watermark_offsets.side_effect = Exception()

    offset = get_status_start_offset("localhost")

    assert offset == 0


@patch("utils.kafka_trigger.Consumer")
def test_poll_for_status_found(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer

    payload = {
        "run_id": "123",
        "stage": "dev",
        "product": "p"
    }

    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = json.dumps(payload).encode()

    consumer.poll.side_effect = [msg]

    result = poll_for_status(
        expected_run_id="123",
        expected_stage="dev",
        expected_product="p",
        bootstrap_server="localhost",
        start_offset=0,
        timeout_secs=1,
    )

    assert result["run_id"] == "123"


@patch("utils.kafka_trigger.Consumer")
def test_poll_for_status_timeout(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer

    consumer.poll.return_value = None

    result = poll_for_status(
        expected_run_id="123",
        expected_stage="dev",
        expected_product="p",
        bootstrap_server="localhost",
        start_offset=0,
        timeout_secs=0.1,
    )

    assert result is None