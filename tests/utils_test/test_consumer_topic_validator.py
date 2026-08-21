import pytest
import sys
import os

from utils.topic_consumer_config_validator import (
    BaseValidator,
    ExtractorValidator,
    SchemaMapperValidator,
)


# =========================================================
# Helper to prevent actual sys.exit
# =========================================================
@pytest.fixture
def no_exit(monkeypatch):
    def fake_exit(code):
        raise SystemExit(code)

    monkeypatch.setattr(sys, "exit", fake_exit)


# =========================================================
# BaseValidator Coverage
# =========================================================
def test_base_require_missing(monkeypatch):
    v = BaseValidator("test")
    monkeypatch.delenv("MISSING_KEY", raising=False)

    value = v._require("MISSING_KEY")

    assert value == ""
    assert len(v.errors) == 1
    assert "MISSING_KEY" in v.errors[0]


def test_base_no_edge_spaces():
    v = BaseValidator("test")
    v._no_edge_spaces("key", " value ")

    assert len(v.errors) == 1


def test_base_no_internal_spaces():
    v = BaseValidator("test")
    v._no_internal_spaces("key", "bad value")

    assert len(v.errors) == 1


def test_base_validate_kafka_bootstrap_valid():
    v = BaseValidator("test")
    v._validate_kafka_bootstrap("host1:9092,host2:9093")

    assert not v.errors


def test_base_validate_kafka_bootstrap_invalid():
    v = BaseValidator("test")
    v._validate_kafka_bootstrap("host1,host2:9092")

    assert any("invalid bootstrap entry" in e for e in v.errors)


def test_base_validate_kafka_bootstrap_with_spaces():
    v = BaseValidator("test")
    v._validate_kafka_bootstrap("host1:9092, host2:9093")

    assert any("must not contain spaces" in e for e in v.errors)


def test_base_validate_kafka_bootstrap_empty():
    v = BaseValidator("test")
    v._validate_kafka_bootstrap("")

    assert any("required" in e for e in v.errors)


def test_base_validate_triggers_exit(no_exit):
    v = BaseValidator("test")
    v.errors.append("error")

    with pytest.raises(SystemExit):
        v.validate()


def test_base_validate_no_errors(no_exit):
    v = BaseValidator("test")
    # should not raise
    v.validate()


# =========================================================
# ExtractorValidator Coverage
# =========================================================
def test_extractor_success(monkeypatch, no_exit):
    monkeypatch.setenv("bootstrapServer", "host1:9092")
    monkeypatch.setenv("srcTopicName", "topicA")
    monkeypatch.setenv("mapperTopicName", "topicB")

    v = ExtractorValidator()
    v.validate_all()


def test_extractor_missing_required(monkeypatch, no_exit):
    monkeypatch.delenv("bootstrapServer", raising=False)
    monkeypatch.delenv("srcTopicName", raising=False)

    v = ExtractorValidator()

    with pytest.raises(SystemExit):
        v.validate_all()


def test_extractor_invalid_bootstrap(monkeypatch, no_exit):
    monkeypatch.setenv("bootstrapServer", "badformat")
    monkeypatch.setenv("srcTopicName", "topicA")

    v = ExtractorValidator()

    with pytest.raises(SystemExit):
        v.validate_all()


def test_extractor_src_topic_spaces(monkeypatch, no_exit):
    monkeypatch.setenv("bootstrapServer", "host1:9092")
    monkeypatch.setenv("srcTopicName", "bad topic")

    v = ExtractorValidator()

    with pytest.raises(SystemExit):
        v.validate_all()


def test_extractor_mapper_optional(monkeypatch, no_exit):
    monkeypatch.setenv("bootstrapServer", "host1:9092")
    monkeypatch.setenv("srcTopicName", "topicA")
    monkeypatch.delenv("mapperTopicName", raising=False)

    v = ExtractorValidator()
    v.validate_all()


def test_extractor_mapper_invalid(monkeypatch, no_exit):
    monkeypatch.setenv("bootstrapServer", "host1:9092")
    monkeypatch.setenv("srcTopicName", "topicA")
    monkeypatch.setenv("mapperTopicName", "bad topic")

    v = ExtractorValidator()

    with pytest.raises(SystemExit):
        v.validate_all()


# =========================================================
# SchemaMapperValidator Coverage
# =========================================================
def test_schema_mapper_success(monkeypatch, no_exit):
    monkeypatch.setenv("bootstrapServer", "host1:9092")
    monkeypatch.setenv("mapperTopicName", "topicB")

    v = SchemaMapperValidator()
    v.validate_all()


def test_schema_mapper_missing(monkeypatch, no_exit):
    monkeypatch.delenv("bootstrapServer", raising=False)
    monkeypatch.delenv("mapperTopicName", raising=False)

    v = SchemaMapperValidator()

    with pytest.raises(SystemExit):
        v.validate_all()


def test_schema_mapper_invalid_bootstrap(monkeypatch, no_exit):
    monkeypatch.setenv("bootstrapServer", "invalid")
    monkeypatch.setenv("mapperTopicName", "topicB")

    v = SchemaMapperValidator()

    with pytest.raises(SystemExit):
        v.validate_all()


def test_schema_mapper_topic_spaces(monkeypatch, no_exit):
    monkeypatch.setenv("bootstrapServer", "host1:9092")
    monkeypatch.setenv("mapperTopicName", "bad topic")

    v = SchemaMapperValidator()

    with pytest.raises(SystemExit):
        v.validate_all()