from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from dpn_observability_sdk.heartbeat import HeartbeatLogger


def test_init_with_explicit_interval():
    logger = Mock()

    hb = HeartbeatLogger(
        logger=logger,
        component_name="test-component",
        interval_seconds=123,
        metadata={"env": "dev"},
    )

    assert hb.interval_seconds == 123
    assert hb.component_name == "test-component"
    assert hb.metadata == {"env": "dev"}
    assert hb._heartbeat_count == 0
    assert hb._thread is None


def test_init_with_env_var(monkeypatch):
    monkeypatch.setenv("HEARTBEAT_INTERVAL_SECONDS", "456")

    hb = HeartbeatLogger(
        logger=Mock(),
        component_name="test-component",
    )

    assert hb.interval_seconds == 456


def test_init_with_default_interval(monkeypatch):
    monkeypatch.delenv("HEARTBEAT_INTERVAL_SECONDS", raising=False)

    hb = HeartbeatLogger(
        logger=Mock(),
        component_name="test-component",
    )

    assert hb.interval_seconds == HeartbeatLogger.DEFAULT_INTERVAL_SECONDS


def test_emit_heartbeat_without_start_time():
    logger = Mock()

    hb = HeartbeatLogger(
        logger=logger,
        component_name="component-a",
        interval_seconds=30,
        metadata={"region": "uks"},
    )

    hb._emit_heartbeat()

    assert hb._heartbeat_count == 1

    logger.info.assert_called_once()

    args, kwargs = logger.info.call_args

    assert args[0] == "Heartbeat: component-a is healthy"

    extra = kwargs["extra"]

    assert extra["event.name"] == "component.heartbeat"
    assert extra["component.name"] == "component-a"
    assert extra["heartbeat.sequence"] == 1
    assert extra["component.uptime_seconds"] == 0
    assert extra["heartbeat.interval_seconds"] == 30
    assert extra["component.status"] == "healthy"
    assert extra["region"] == "uks"


def test_emit_heartbeat_with_uptime():
    logger = Mock()

    hb = HeartbeatLogger(
        logger=logger,
        component_name="component-b",
    )

    hb._start_time = datetime.now(UTC) - timedelta(seconds=25)

    hb._emit_heartbeat()

    extra = logger.info.call_args.kwargs["extra"]

    assert extra["heartbeat.sequence"] == 1
    assert extra["component.uptime_seconds"] >= 24


def test_update_metadata():
    hb = HeartbeatLogger(
        logger=Mock(),
        component_name="component",
        metadata={"a": 1},
    )

    hb.update_metadata({"b": 2})

    assert hb.metadata == {"a": 1, "b": 2}


def test_is_running_false_when_thread_none():
    hb = HeartbeatLogger(
        logger=Mock(),
        component_name="component",
    )

    assert hb.is_running() is False


def test_is_running_false_when_thread_dead():
    hb = HeartbeatLogger(
        logger=Mock(),
        component_name="component",
    )

    thread = Mock()
    thread.is_alive.return_value = False

    hb._thread = thread

    assert hb.is_running() is False


def test_is_running_true():
    hb = HeartbeatLogger(
        logger=Mock(),
        component_name="component",
    )

    thread = Mock()
    thread.is_alive.return_value = True

    hb._thread = thread

    assert hb.is_running() is True


def test_start_when_already_running():
    logger = Mock()

    hb = HeartbeatLogger(
        logger=logger,
        component_name="component",
    )

    thread = Mock()
    thread.is_alive.return_value = True

    hb._thread = thread

    hb.start()

    logger.warning.assert_called_once_with(
        "Heartbeat already running",
        extra={"component.name": "component"},
    )


def test_start_creates_background_thread(monkeypatch):
    logger = Mock()

    captured = {}

    class FakeThread:
        def __init__(self, target, name, daemon):
            captured["target"] = target
            captured["name"] = name
            captured["daemon"] = daemon

        def start(self):
            captured["started"] = True

        def is_alive(self):
            return False

    monkeypatch.setattr(
        "dpn_observability_sdk.heartbeat.threading.Thread",
        FakeThread,
    )

    hb = HeartbeatLogger(
        logger=logger,
        component_name="my-component",
        interval_seconds=99,
    )

    hb.start()

    assert captured["started"] is True
    assert captured["target"] == hb._heartbeat_loop
    assert captured["name"] == "heartbeat-my-component"
    assert captured["daemon"] is True

    logger.info.assert_called_with(
        "Heartbeat logger started",
        extra={
            "event.name": "heartbeat.started",
            "component.name": "my-component",
            "heartbeat.interval_seconds": 99,
        },
    )


def test_stop_when_thread_none():
    hb = HeartbeatLogger(
        logger=Mock(),
        component_name="component",
    )

    hb.stop()


def test_stop_when_thread_not_alive():
    hb = HeartbeatLogger(
        logger=Mock(),
        component_name="component",
    )

    thread = Mock()
    thread.is_alive.return_value = False

    hb._thread = thread

    hb.stop()


def test_stop_running_thread():
    logger = Mock()

    hb = HeartbeatLogger(
        logger=logger,
        component_name="component",
    )

    thread = Mock()
    thread.is_alive.return_value = True

    hb._thread = thread
    hb._heartbeat_count = 7

    hb.stop(timeout=2)

    assert hb._stop_event.is_set()

    thread.join.assert_called_once_with(timeout=2)

    logger.info.assert_called_with(
        "Heartbeat logger stopped",
        extra={
            "event.name": "heartbeat.stopped",
            "component.name": "component",
            "heartbeat.total_count": 7,
        },
    )


def test_heartbeat_loop_breaks_when_stop_event_signaled():
    hb = HeartbeatLogger(
        logger=Mock(),
        component_name="component",
    )

    hb._emit_heartbeat = Mock()

    stop_event = Mock()
    stop_event.is_set.return_value = False
    stop_event.wait.return_value = True

    hb._stop_event = stop_event

    hb._heartbeat_loop()

    hb._emit_heartbeat.assert_called_once()


def test_heartbeat_loop_emits_multiple_heartbeats():
    hb = HeartbeatLogger(
        logger=Mock(),
        component_name="component",
        interval_seconds=1,
    )

    emit_mock = Mock()
    hb._emit_heartbeat = emit_mock

    stop_event = Mock()

    stop_event.wait.side_effect = [False]

    states = [False, True]

    def is_set():
        return states.pop(0)

    stop_event.is_set.side_effect = is_set

    hb._stop_event = stop_event

    hb._heartbeat_loop()

    assert emit_mock.call_count == 2