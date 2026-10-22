# Copyright DSI Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Unit tests for utils/kafka_transection.py (KafkaTransection).

Confluent Kafka's Producer/Consumer classes are mocked throughout - no real
broker connection is made. Follows the mocking conventions established in
tests/utils_test/test_kafka_trigger.py.
"""
from __future__ import annotations

import json

import pytest
from confluent_kafka import KafkaError, KafkaException
from unittest.mock import MagicMock, patch

from utils.kafka_transection import KafkaTransection, _HeaderCarrier


# ---------------------------------------------------------------------------
# _HeaderCarrier
# ---------------------------------------------------------------------------
def test_header_carrier_set_encodes_to_bytes():
    carrier = _HeaderCarrier()
    carrier.set("traceparent", "00-abc-def-01")
    assert carrier["traceparent"] == b"00-abc-def-01"


def test_header_carrier_get_decodes_bytes_value():
    carrier = _HeaderCarrier({"traceparent": b"00-abc-def-01"})
    assert carrier.get("traceparent") == "00-abc-def-01"


def test_header_carrier_get_returns_str_value_unchanged():
    carrier = _HeaderCarrier({"traceparent": "already-a-str"})
    assert carrier.get("traceparent") == "already-a-str"


def test_header_carrier_get_missing_key_returns_default():
    carrier = _HeaderCarrier()
    assert carrier.get("missing") is None
    assert carrier.get("missing", "fallback") == "fallback"


def test_header_carrier_keys_returns_list():
    carrier = _HeaderCarrier({"a": b"1", "b": b"2"})
    assert sorted(carrier.keys()) == ["a", "b"]


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------
def test_init_requires_bootstrap_server():
    with pytest.raises(ValueError):
        KafkaTransection()


def test_init_boostrap_server_legacy_spelling(mock_logger):
    kt = KafkaTransection(boostrap_server="legacy:9092", logger=mock_logger)
    assert kt.bootstrap_server == "legacy:9092"


def test_init_bootstrap_server_takes_precedence(mock_logger):
    kt = KafkaTransection(
        bootstrap_server="preferred:9092",
        boostrap_server="legacy:9092",
        logger=mock_logger,
    )
    assert kt.bootstrap_server == "preferred:9092"


def test_init_custom_logger_used_directly(mock_logger):
    kt = KafkaTransection(bootstrap_server="localhost:9092", logger=mock_logger)
    assert kt._logger is mock_logger


@patch("utils.kafka_transection.Logging")
def test_init_default_logger_created_when_not_supplied(mock_logging_cls):
    created_logger = MagicMock()
    mock_logging_cls.return_value.create_logger.return_value = created_logger

    kt = KafkaTransection(bootstrap_server="localhost:9092")

    mock_logging_cls.return_value.create_logger.assert_called_once()
    assert kt._logger is created_logger


def test_init_component_name_stored(mock_logger):
    kt = KafkaTransection(
        bootstrap_server="localhost:9092",
        logger=mock_logger,
        component_name="my-component",
    )
    assert kt.component_name == "my-component"


# ---------------------------------------------------------------------------
# _get_producer
# ---------------------------------------------------------------------------
@patch("utils.kafka_transection.build_kafka_client_config")
@patch("utils.kafka_transection.Producer")
def test_get_producer_lazy_singleton(mock_producer_cls, mock_build_config, mock_logger):
    mock_build_config.return_value = {"bootstrap.servers": "localhost:9092"}
    producer_instance = MagicMock()
    mock_producer_cls.return_value = producer_instance

    kt = KafkaTransection(bootstrap_server="localhost:9092", logger=mock_logger)

    first = kt._get_producer()
    second = kt._get_producer()

    assert first is producer_instance
    assert second is producer_instance
    mock_producer_cls.assert_called_once()

    call_kwargs = mock_producer_cls.call_args[0][0]
    assert call_kwargs["acks"] == "all"
    assert call_kwargs["enable.idempotence"] is False
    assert call_kwargs["compression.type"] == "lz4"
    assert call_kwargs["socket.keepalive.enable"] is True


# ---------------------------------------------------------------------------
# send_message
# ---------------------------------------------------------------------------
@patch("utils.kafka_transection.build_kafka_client_config", return_value={})
@patch("utils.kafka_transection.Producer")
def test_send_message_empty_topic_raises(mock_producer_cls, _mock_cfg, mock_logger):
    kt = KafkaTransection(bootstrap_server="localhost:9092", logger=mock_logger)

    with pytest.raises(ValueError):
        kt.send_message("", {"key": "value"})

    mock_producer_cls.assert_not_called()


@patch("utils.kafka_transection._otel_propagate")
@patch("utils.kafka_transection._OTEL_AVAILABLE", True)
@patch("utils.kafka_transection.build_kafka_client_config", return_value={})
@patch("utils.kafka_transection.Producer")
def test_send_message_success_with_otel_available(
    mock_producer_cls, _mock_cfg, mock_otel_propagate, mock_logger
):
    producer_instance = MagicMock()
    mock_producer_cls.return_value = producer_instance

    kt = KafkaTransection(bootstrap_server="localhost:9092", logger=mock_logger)
    kt.send_message("test-topic", {"key": "value"})

    mock_otel_propagate.inject.assert_called_once()
    producer_instance.produce.assert_called_once()

    call_kwargs = producer_instance.produce.call_args.kwargs
    assert call_kwargs["topic"] == "test-topic"
    assert json.loads(call_kwargs["value"].decode("utf-8")) == {"key": "value"}
    assert isinstance(call_kwargs["headers"], list)
    assert callable(call_kwargs["callback"])

    producer_instance.flush.assert_called_once()


@patch("utils.kafka_transection._otel_propagate")
@patch("utils.kafka_transection._OTEL_AVAILABLE", False)
@patch("utils.kafka_transection.build_kafka_client_config", return_value={})
@patch("utils.kafka_transection.Producer")
def test_send_message_otel_unavailable_skips_injection(
    mock_producer_cls, _mock_cfg, mock_otel_propagate, mock_logger
):
    producer_instance = MagicMock()
    mock_producer_cls.return_value = producer_instance

    kt = KafkaTransection(bootstrap_server="localhost:9092", logger=mock_logger)
    kt.send_message("test-topic", {"key": "value"})

    mock_otel_propagate.inject.assert_not_called()
    call_kwargs = producer_instance.produce.call_args.kwargs
    assert call_kwargs["headers"] == []


@patch("utils.kafka_transection.build_kafka_client_config", return_value={})
@patch("utils.kafka_transection.Producer")
def test_send_message_delivery_callback_error_logs_error(
    mock_producer_cls, _mock_cfg, mock_logger
):
    producer_instance = MagicMock()
    mock_producer_cls.return_value = producer_instance

    kt = KafkaTransection(
        bootstrap_server="localhost:9092",
        logger=mock_logger,
        component_name="comp-a",
    )
    kt.send_message("test-topic", {"key": "value"})

    callback = producer_instance.produce.call_args.kwargs["callback"]
    fake_err = MagicMock()
    fake_err.__str__.return_value = "boom"
    fake_msg = MagicMock()

    callback(fake_err, fake_msg)

    mock_logger.error.assert_called_once()
    args, kwargs = mock_logger.error.call_args
    assert args[0] == "Kafka delivery failed"
    assert kwargs["extra"]["topic"] == "test-topic"
    assert kwargs["extra"]["component.name"] == "comp-a"


@patch("utils.kafka_transection.build_kafka_client_config", return_value={})
@patch("utils.kafka_transection.Producer")
def test_send_message_delivery_callback_success_logs_info(
    mock_producer_cls, _mock_cfg, mock_logger
):
    producer_instance = MagicMock()
    mock_producer_cls.return_value = producer_instance

    kt = KafkaTransection(bootstrap_server="localhost:9092", logger=mock_logger)
    kt.send_message("test-topic", {"key": "value"})

    callback = producer_instance.produce.call_args.kwargs["callback"]
    fake_msg = MagicMock()
    fake_msg.topic.return_value = "test-topic"
    fake_msg.partition.return_value = 0
    fake_msg.offset.return_value = 42

    callback(None, fake_msg)

    mock_logger.info.assert_called_once()
    args, kwargs = mock_logger.info.call_args
    assert args[0] == "Kafka message delivered"
    assert kwargs["extra"]["partition"] == 0
    assert kwargs["extra"]["offset"] == 42


# ---------------------------------------------------------------------------
# flush
# ---------------------------------------------------------------------------
def test_flush_noop_when_producer_never_created(mock_logger):
    kt = KafkaTransection(bootstrap_server="localhost:9092", logger=mock_logger)
    # Should not raise and should not attempt to build a producer.
    kt.flush(timeout=5.0)
    assert kt._producer is None


@patch("utils.kafka_transection.build_kafka_client_config", return_value={})
@patch("utils.kafka_transection.Producer")
def test_flush_delegates_to_producer_when_created(
    mock_producer_cls, _mock_cfg, mock_logger
):
    producer_instance = MagicMock()
    mock_producer_cls.return_value = producer_instance

    kt = KafkaTransection(bootstrap_server="localhost:9092", logger=mock_logger)
    kt._get_producer()  # force creation

    kt.flush(timeout=7.5)

    producer_instance.flush.assert_called_once_with(timeout=7.5)


# ---------------------------------------------------------------------------
# consume_messages
# ---------------------------------------------------------------------------
def _make_message(value: dict, partition=0, offset=0, headers=None):
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = json.dumps(value).encode("utf-8")
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.headers.return_value = headers or []
    return msg


@patch("utils.kafka_transection.build_kafka_client_config", return_value={})
@patch("utils.kafka_transection.Consumer")
def test_consume_messages_poll_none_then_message_then_stop(
    mock_consumer_cls, _mock_cfg, mock_logger
):
    consumer_instance = MagicMock()
    mock_consumer_cls.return_value = consumer_instance

    msg = _make_message({"hello": "world"})
    consumer_instance.poll.side_effect = [None, msg, StopIteration]

    handler = MagicMock()
    kt = KafkaTransection(bootstrap_server="localhost:9092", logger=mock_logger)

    with pytest.raises(StopIteration):
        kt.consume_messages("source-topic", "group1", handler, poll_timeout=0.1)

    consumer_instance.subscribe.assert_called_once_with(["source-topic"])
    handler.assert_called_once_with({"hello": "world"})
    consumer_instance.close.assert_called_once()


@patch("utils.kafka_transection.build_kafka_client_config", return_value={})
@patch("utils.kafka_transection.Consumer")
def test_consume_messages_partition_eof_continues(
    mock_consumer_cls, _mock_cfg, mock_logger
):
    consumer_instance = MagicMock()
    mock_consumer_cls.return_value = consumer_instance

    eof_error = MagicMock()
    eof_error.code.return_value = KafkaError._PARTITION_EOF
    eof_msg = MagicMock()
    eof_msg.error.return_value = eof_error

    consumer_instance.poll.side_effect = [eof_msg, StopIteration]

    kt = KafkaTransection(bootstrap_server="localhost:9092", logger=mock_logger)

    with pytest.raises(StopIteration):
        kt.consume_messages("source-topic", "group1", MagicMock())

    consumer_instance.close.assert_called_once()


@patch("utils.kafka_transection.build_kafka_client_config", return_value={})
@patch("utils.kafka_transection.Consumer")
def test_consume_messages_non_eof_error_raises_kafka_exception(
    mock_consumer_cls, _mock_cfg, mock_logger
):
    consumer_instance = MagicMock()
    mock_consumer_cls.return_value = consumer_instance

    bad_error = MagicMock()
    bad_error.code.return_value = KafkaError._ALL_BROKERS_DOWN
    bad_msg = MagicMock()
    bad_msg.error.return_value = bad_error

    consumer_instance.poll.side_effect = [bad_msg]

    kt = KafkaTransection(bootstrap_server="localhost:9092", logger=mock_logger)

    with pytest.raises(KafkaException):
        kt.consume_messages("source-topic", "group1", MagicMock())

    # finally-block cleanup must still run even though the exception propagates
    consumer_instance.close.assert_called_once()


@patch("utils.kafka_transection.build_kafka_client_config", return_value={})
@patch("utils.kafka_transection.Consumer")
def test_consume_messages_json_decode_error_logged(
    mock_consumer_cls, _mock_cfg, mock_logger
):
    consumer_instance = MagicMock()
    mock_consumer_cls.return_value = consumer_instance

    bad_json_msg = MagicMock()
    bad_json_msg.error.return_value = None
    bad_json_msg.value.return_value = b"not-json"
    bad_json_msg.headers.return_value = []

    consumer_instance.poll.side_effect = [bad_json_msg, StopIteration]

    handler = MagicMock()
    kt = KafkaTransection(bootstrap_server="localhost:9092", logger=mock_logger)

    with pytest.raises(StopIteration):
        kt.consume_messages("source-topic", "group1", handler)

    handler.assert_not_called()
    error_calls = [c for c in mock_logger.error.call_args_list if c.args[0] == "JSON decode failed"]
    assert len(error_calls) == 1


@patch("utils.kafka_transection.build_kafka_client_config", return_value={})
@patch("utils.kafka_transection.Consumer")
def test_consume_messages_handler_exception_logged(
    mock_consumer_cls, _mock_cfg, mock_logger
):
    consumer_instance = MagicMock()
    mock_consumer_cls.return_value = consumer_instance

    msg = _make_message({"hello": "world"})
    consumer_instance.poll.side_effect = [msg, StopIteration]

    def handler(_payload):
        raise RuntimeError("handler boom")

    kt = KafkaTransection(bootstrap_server="localhost:9092", logger=mock_logger)

    with pytest.raises(StopIteration):
        kt.consume_messages("source-topic", "group1", handler)

    error_calls = [
        c for c in mock_logger.error.call_args_list
        if c.args[0] == "Message handler exception"
    ]
    assert len(error_calls) == 1
    assert error_calls[0].kwargs["exc_info"] is True


@patch("utils.kafka_transection._otel_propagate")
@patch("utils.kafka_transection._OTEL_AVAILABLE", True)
@patch("utils.kafka_transection.build_kafka_client_config", return_value={})
@patch("utils.kafka_transection.Consumer")
def test_consume_messages_extracts_otel_headers(
    mock_consumer_cls, _mock_cfg, mock_otel_propagate, mock_logger
):
    consumer_instance = MagicMock()
    mock_consumer_cls.return_value = consumer_instance

    msg = _make_message(
        {"hello": "world"}, headers=[("traceparent", b"00-abc-def-01")]
    )
    consumer_instance.poll.side_effect = [msg, StopIteration]

    handler = MagicMock()
    kt = KafkaTransection(bootstrap_server="localhost:9092", logger=mock_logger)

    with pytest.raises(StopIteration):
        kt.consume_messages("source-topic", "group1", handler)

    mock_otel_propagate.extract.assert_called_once()
    handler.assert_called_once_with({"hello": "world"})


@patch("utils.kafka_transection.build_kafka_client_config")
@patch("utils.kafka_transection.Consumer")
def test_consume_messages_builds_config_and_logs_subscription(
    mock_consumer_cls, mock_build_config, mock_logger
):
    mock_build_config.return_value = {"bootstrap.servers": "localhost:9092"}
    consumer_instance = MagicMock()
    mock_consumer_cls.return_value = consumer_instance
    consumer_instance.poll.side_effect = [StopIteration]

    kt = KafkaTransection(
        bootstrap_server="localhost:9092",
        logger=mock_logger,
        component_name="comp-x",
    )

    with pytest.raises(StopIteration):
        kt.consume_messages("source-topic", "group42", MagicMock())

    mock_build_config.assert_called_once_with("localhost:9092")
    config_arg = mock_consumer_cls.call_args[0][0]
    assert config_arg["group.id"] == "group42"
    assert config_arg["auto.offset.reset"] == "earliest"
    assert config_arg["enable.auto.commit"] is True

    info_calls = [
        c for c in mock_logger.info.call_args_list
        if c.args[0] == "Kafka consumer subscribed"
    ]
    assert len(info_calls) == 1
    assert info_calls[0].kwargs["extra"]["component.name"] == "comp-x"

    closed_calls = [
        c for c in mock_logger.info.call_args_list
        if c.args[0] == "Kafka consumer closed"
    ]
    assert len(closed_calls) == 1
