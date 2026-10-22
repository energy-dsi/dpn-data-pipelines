import pytest
from unittest.mock import MagicMock

from utils.topic_forwarder import TopicForwarder


# ----------------------------------------------------------------------
# Dummy Context
# ----------------------------------------------------------------------

class DummyCtx:
    def __init__(self, triggered_by="kafka-trigger"):
        self.triggered_by = triggered_by
        self.pipeline_stage = "stage"

    def as_log_extra(self):
        return {"ctx": "value"}


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------

@pytest.fixture
def step_log():
    return MagicMock()


@pytest.fixture
def forwarder(monkeypatch):
    monkeypatch.setattr(
        "utils.topic_forwarder.TopicResolver",
        lambda: MagicMock(resolve=lambda *a, **k: "mapper-topic"),
    )

    monkeypatch.setattr(
        "utils.topic_forwarder.KafkaTopicManager",
        lambda *a, **k: MagicMock(ensure_exists=lambda x: None),
    )

    producer = MagicMock()
    monkeypatch.setattr(
        "utils.topic_forwarder.Producer",
        lambda *a, **k: producer,
    )

    return TopicForwarder("svc")


# ----------------------------------------------------------------------
# run dispatch
# ----------------------------------------------------------------------

def test_run_dispatch(forwarder, step_log):
    ctx = DummyCtx("kafka-trigger")
    forwarder._run_drain = MagicMock()
    forwarder.run(ctx, step_log)
    forwarder._run_drain.assert_called()

    ctx = DummyCtx("other")
    forwarder._run_continuous = MagicMock()
    forwarder.run(ctx, step_log)
    forwarder._run_continuous.assert_called()


# ----------------------------------------------------------------------
# ✅ Drain mode FULL coverage
# ----------------------------------------------------------------------

def test_drain_idle_exit(monkeypatch, forwarder, step_log):
    ctx = DummyCtx()

    consumer = MagicMock()
    consumer.poll.return_value = None

    monkeypatch.setattr("utils.topic_forwarder.Consumer", lambda *a, **k: consumer)

    times = [1, 20]
    monkeypatch.setattr("utils.topic_forwarder.time.monotonic",
                        lambda: times.pop(0) if times else 20)

    forwarder._run_drain(ctx, step_log)

    assert step_log.step_start.called
    assert step_log.step_end.called


def test_drain_idle_continue_before_timeout(monkeypatch, forwarder, step_log):
    """Covers the `continue` on a None poll whose idle_time has not yet
    reached _DRAIN_IDLE_SECS (as opposed to test_drain_idle_exit, which only
    ever sees idle_time already past the threshold)."""
    ctx = DummyCtx()

    consumer = MagicMock()
    consumer.poll.side_effect = [None, None]

    monkeypatch.setattr("utils.topic_forwarder.Consumer", lambda *a, **k: consumer)

    # last_message_time=1; first None: idle=5-1=4 (<15, continue);
    # second None: idle=20-1=19 (>=15, break)
    times = [1, 5, 20]
    monkeypatch.setattr(
        "utils.topic_forwarder.time.monotonic",
        lambda: times.pop(0) if times else 20,
    )

    forwarder._run_drain(ctx, step_log)

    assert consumer.poll.call_count == 2
    assert step_log.step_end.called


def test_drain_partition_eof(monkeypatch, forwarder, step_log):
    from utils.topic_forwarder import KafkaError

    ctx = DummyCtx()

    msg = MagicMock()
    err = MagicMock()
    err.code.return_value = KafkaError._PARTITION_EOF
    msg.error.return_value = err

    consumer = MagicMock()
    consumer.poll.side_effect = [msg, None]

    monkeypatch.setattr("utils.topic_forwarder.Consumer", lambda *a, **k: consumer)

    times = [1, 20]
    monkeypatch.setattr("utils.topic_forwarder.time.monotonic",
                        lambda: times.pop(0) if times else 20)

    forwarder._run_drain(ctx, step_log)


def test_drain_kafka_error(monkeypatch, forwarder, step_log):
    from utils.topic_forwarder import KafkaException

    ctx = DummyCtx()

    msg = MagicMock()
    err = MagicMock()
    err.code.return_value = 999
    msg.error.return_value = err

    consumer = MagicMock()
    consumer.poll.return_value = msg

    monkeypatch.setattr("utils.topic_forwarder.Consumer", lambda *a, **k: consumer)

    with pytest.raises(KafkaException):
        forwarder._run_drain(ctx, step_log)


# ✅ ✅ THIS IS THE MISSING COVERAGE BLOCK (IMPORTANT)
def test_drain_forward_success(monkeypatch, forwarder, step_log):
    ctx = DummyCtx()

    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = b"data"

    consumer = MagicMock()
    consumer.poll.side_effect = [msg, None, None]

    monkeypatch.setattr("utils.topic_forwarder.Consumer", lambda *a, **k: consumer)

    # force full execution path
    times = [1, 2, 20]
    monkeypatch.setattr(
        "utils.topic_forwarder.time.monotonic",
        lambda: times.pop(0),
    )

    forwarder._forward_one = MagicMock()

    forwarder._run_drain(ctx, step_log)

    # ✅ THESE LINES WERE NOT COVERED BEFORE
    forwarder._forward_one.assert_called()
    consumer.commit.assert_called()


# ----------------------------------------------------------------------
# ✅ Continuous mode FULL coverage
# ----------------------------------------------------------------------

def test_continuous_msg_none(monkeypatch, forwarder, step_log):
    ctx = DummyCtx("other")

    consumer = MagicMock()
    consumer.poll.side_effect = [None, KeyboardInterrupt()]

    monkeypatch.setattr("utils.topic_forwarder.Consumer", lambda *a, **k: consumer)

    try:
        forwarder._run_continuous(ctx, step_log)
    except KeyboardInterrupt:
        pass


def test_continuous_partition_eof(monkeypatch, forwarder, step_log):
    from utils.topic_forwarder import KafkaError

    ctx = DummyCtx("other")

    msg = MagicMock()
    err = MagicMock()
    err.code.return_value = KafkaError._PARTITION_EOF
    msg.error.return_value = err

    consumer = MagicMock()
    consumer.poll.side_effect = [msg, KeyboardInterrupt()]

    monkeypatch.setattr("utils.topic_forwarder.Consumer", lambda *a, **k: consumer)

    try:
        forwarder._run_continuous(ctx, step_log)
    except KeyboardInterrupt:
        pass


def test_continuous_kafka_exception(monkeypatch, forwarder, step_log):
    ctx = DummyCtx("other")

    msg = MagicMock()
    err = MagicMock()
    err.code.return_value = 999
    msg.error.return_value = err

    consumer = MagicMock()
    consumer.poll.side_effect = [msg, KeyboardInterrupt()]

    monkeypatch.setattr("utils.topic_forwarder.Consumer", lambda *a, **k: consumer)

    monkeypatch.setattr(
        "utils.topic_forwarder.time.sleep",
        lambda x: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    try:
        forwarder._run_continuous(ctx, step_log)
    except KeyboardInterrupt:
        pass


def test_continuous_generic_exception(monkeypatch, forwarder, step_log):
    ctx = DummyCtx("other")

    msg = MagicMock()
    msg.error.return_value = None

    consumer = MagicMock()
    consumer.poll.return_value = msg

    monkeypatch.setattr("utils.topic_forwarder.Consumer", lambda *a, **k: consumer)

    forwarder._forward_one = MagicMock(side_effect=Exception("boom"))

    monkeypatch.setattr(
        "utils.topic_forwarder.time.sleep",
        lambda x: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    try:
        forwarder._run_continuous(ctx, step_log)
    except KeyboardInterrupt:
        pass

    assert step_log.step_failed.called


def test_continuous_retries_and_restarts_step_start(monkeypatch, forwarder, step_log):
    """Covers the outer while-True loop actually restarting after a retry
    delay — the new Consumer being created and step_log.step_start() being
    called again (line 324) — as opposed to the other continuous-mode tests,
    which all stop the loop on their very first iteration."""
    ctx = DummyCtx("other")

    consumers_created = []

    def make_consumer(*a, **k):
        c = MagicMock()
        if not consumers_created:
            c.poll.side_effect = [Exception("boom")]
        else:
            c.poll.side_effect = [KeyboardInterrupt()]
        consumers_created.append(c)
        return c

    monkeypatch.setattr("utils.topic_forwarder.Consumer", make_consumer)
    monkeypatch.setattr("utils.topic_forwarder.time.sleep", lambda x: None)

    try:
        forwarder._run_continuous(ctx, step_log)
    except KeyboardInterrupt:
        pass

    assert len(consumers_created) == 2
    assert step_log.step_failed.called
    # Once at the top of _run_continuous, once more after the retry delay
    # when the outer loop restarts.
    assert step_log.step_start.call_count >= 2


# ----------------------------------------------------------------------
# _forward_one
# ----------------------------------------------------------------------

def test_forward_one_success(forwarder, step_log):
    ctx = DummyCtx()

    msg = MagicMock()
    msg.partition.return_value = 1
    msg.offset.return_value = 10
    msg.value.return_value = b"abc"
    msg.key.return_value = b"k"
    msg.headers.return_value = []

    forwarder._forward_one(msg, ctx, step_log)

    assert step_log.step_start.called
    assert step_log.step_end.called


def test_forward_one_failure(forwarder, step_log):
    ctx = DummyCtx()

    msg = MagicMock()
    msg.partition.return_value = 1
    msg.offset.return_value = 1
    msg.value.return_value = b"abc"

    forwarder.producer.produce = MagicMock(side_effect=Exception("fail"))

    with pytest.raises(Exception):
        forwarder._forward_one(msg, ctx, step_log)

    assert step_log.step_failed.called


# ----------------------------------------------------------------------
# delivery callback
# ----------------------------------------------------------------------

def test_on_delivery_error(forwarder):
    forwarder.logger = MagicMock()

    err = Exception("fail")
    msg = MagicMock()

    forwarder._on_delivery(err, msg)

    forwarder.logger.error.assert_called()