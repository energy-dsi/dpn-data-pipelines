import json
import pytest
import time
from unittest.mock import MagicMock, patch

from confluent_kafka import KafkaError, KafkaException

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
# execute() main loop
# -----------------------------------------------------------

@patch("utils.kafka_trigger.StatusPublisher")
@patch("utils.kafka_trigger.KafkaTopicManager")
def test_execute_loop_ensures_topics_then_waits_and_runs_pipeline(
    mock_topic_mgr_cls, mock_status_pub_cls
):
    backend = KafkaTriggerBackend()
    backend._wait_for_trigger = MagicMock(
        side_effect=[{"run_id": "1"}, StopIteration("stop the while True loop")]
    )
    backend._execute_pipeline = MagicMock()

    with pytest.raises(StopIteration):
        backend.execute(
            lambda ctx: None,
            pipeline_stage="dev",
            pipeline_type="t",
            pipeline_role="r",
            component_name="comp",
        )

    topic_manager = mock_topic_mgr_cls.return_value
    topic_manager.ensure_exists.assert_any_call("dpn-pipeline-control")
    topic_manager.ensure_exists.assert_any_call("dpn-pipeline-status")
    mock_status_pub_cls.assert_called_once()
    backend._execute_pipeline.assert_called_once()
    assert backend._wait_for_trigger.call_count == 2


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


@patch("utils.kafka_trigger.Consumer")
def test_wait_for_trigger_skips_none_poll_then_processes_message(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer

    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = json.dumps({
        "stage": "dev", "pipeline_type": "t", "pipeline_role": "r", "product": "",
        "run_id": "123",
    }).encode()
    consumer.poll.side_effect = [None, msg]

    backend = KafkaTriggerBackend()
    backend._product = ""

    result = backend._wait_for_trigger(
        group_id="g", pipeline_stage="dev", pipeline_type="t", pipeline_role="r"
    )

    assert result["run_id"] == "123"


@patch("utils.kafka_trigger.Consumer")
def test_wait_for_trigger_partition_eof_continues(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer

    eof_msg = MagicMock()
    eof_error = MagicMock()
    eof_error.code.return_value = KafkaError._PARTITION_EOF
    eof_msg.error.return_value = eof_error

    good_msg = MagicMock()
    good_msg.error.return_value = None
    good_msg.value.return_value = json.dumps({
        "stage": "dev", "pipeline_type": "t", "pipeline_role": "r", "product": "",
        "run_id": "456",
    }).encode()

    consumer.poll.side_effect = [eof_msg, good_msg]

    backend = KafkaTriggerBackend()
    backend._product = ""

    result = backend._wait_for_trigger(
        group_id="g", pipeline_stage="dev", pipeline_type="t", pipeline_role="r"
    )

    assert result["run_id"] == "456"


@patch("utils.kafka_trigger.Consumer")
def test_wait_for_trigger_raises_kafka_exception_on_real_error(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer

    err_msg = MagicMock()
    kafka_err = MagicMock()
    kafka_err.code.return_value = KafkaError._ALL_BROKERS_DOWN
    err_msg.error.return_value = kafka_err

    consumer.poll.side_effect = [err_msg]

    backend = KafkaTriggerBackend()

    with pytest.raises(KafkaException):
        backend._wait_for_trigger(
            group_id="g", pipeline_stage="dev", pipeline_type="t", pipeline_role="r"
        )


@patch("utils.kafka_trigger.Consumer")
def test_wait_for_trigger_skips_non_matching_message(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer

    non_matching = MagicMock()
    non_matching.error.return_value = None
    non_matching.value.return_value = json.dumps({
        "stage": "other", "pipeline_type": "t", "pipeline_role": "r", "product": "",
    }).encode()

    matching = MagicMock()
    matching.error.return_value = None
    matching.value.return_value = json.dumps({
        "stage": "dev", "pipeline_type": "t", "pipeline_role": "r", "product": "",
        "run_id": "789",
    }).encode()

    consumer.poll.side_effect = [non_matching, matching]

    backend = KafkaTriggerBackend()
    backend._product = ""

    result = backend._wait_for_trigger(
        group_id="g", pipeline_stage="dev", pipeline_type="t", pipeline_role="r"
    )

    assert result["run_id"] == "789"
    assert consumer.commit.call_count == 2


@patch("utils.kafka_trigger.Consumer")
def test_wait_for_trigger_swallows_consumer_close_error(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer
    consumer.close.side_effect = Exception("already closed")

    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = json.dumps({
        "stage": "dev", "pipeline_type": "t", "pipeline_role": "r", "product": "",
        "run_id": "999",
    }).encode()
    consumer.poll.side_effect = [msg]

    backend = KafkaTriggerBackend()
    backend._product = ""

    result = backend._wait_for_trigger(
        group_id="g", pipeline_stage="dev", pipeline_type="t", pipeline_role="r"
    )

    assert result["run_id"] == "999"  # must not raise despite consumer.close() blowing up


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


@patch("utils.kafka_trigger.Producer")
def test_publish_trigger_delivery_callback_success_then_failure(mock_producer_cls):
    """publish_trigger() registers _on_delivery as produce()'s callback but
    never invokes it itself (that's librdkafka's job on the real client) —
    grab it and call it directly to cover both of its branches."""
    producer = MagicMock()
    mock_producer_cls.return_value = producer

    publish_trigger({"run_id": "1"}, "localhost")

    _, kwargs = producer.produce.call_args
    on_delivery = kwargs["callback"]

    on_delivery(None, MagicMock())  # no error — must not raise

    with pytest.raises(RuntimeError, match="Trigger delivery failed"):
        on_delivery("broker unavailable", MagicMock())


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


@patch("utils.kafka_trigger.Consumer")
def test_poll_for_status_skips_error_message_then_finds_match(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer

    err_msg = MagicMock()
    err_msg.error.return_value = MagicMock()  # truthy "there is an error" object

    good_msg = MagicMock()
    good_msg.error.return_value = None
    good_msg.value.return_value = json.dumps({
        "run_id": "123", "stage": "dev", "product": "p",
    }).encode()

    consumer.poll.side_effect = [err_msg, good_msg]

    result = poll_for_status(
        expected_run_id="123",
        expected_stage="dev",
        expected_product="p",
        bootstrap_server="localhost",
        start_offset=0,
        timeout_secs=2,
    )

    assert result["run_id"] == "123"


@patch("utils.kafka_trigger.Consumer")
def test_poll_for_status_skips_invalid_json_then_finds_match(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer

    bad_msg = MagicMock()
    bad_msg.error.return_value = None
    bad_msg.value.return_value = b"not-json"

    good_msg = MagicMock()
    good_msg.error.return_value = None
    good_msg.value.return_value = json.dumps({
        "run_id": "123", "stage": "dev", "product": "p",
    }).encode()

    consumer.poll.side_effect = [bad_msg, good_msg]

    result = poll_for_status(
        expected_run_id="123",
        expected_stage="dev",
        expected_product="p",
        bootstrap_server="localhost",
        start_offset=0,
        timeout_secs=2,
    )

    assert result["run_id"] == "123"