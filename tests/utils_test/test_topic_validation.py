import os
import pytest

from utils.topic_config_validator import (
    AdaptorConfigValidator,
    SchemaMapperConfigValidator,
)

# ======================================================
# Helpers
# ======================================================

def set_env(env: dict):
    """Set environment variables for test."""
    for k, v in env.items():
        os.environ[k] = v


def clear_env():
    """Clear all relevant env vars."""
    keys = [
        "orgName",
        "schemaType",
        "productType",
        "SERVICE_NAME",
        "SERVICE_VERSION",
        "bootstrapServer",
        "srcTopicName",
        "mapperTopicName",
        "targetTopicName",
    ]
    for k in keys:
        os.environ.pop(k, None)


# ======================================================
# BASE METADATA TESTS
# ======================================================

def test_missing_metadata_fields():
    clear_env()

    validator = AdaptorConfigValidator(exit_on_failure=False)
    validator.validate_all()

    assert len(validator.errors) >= 5
    assert any("orgName" in e for e in validator.errors)
    assert any("schemaType" in e for e in validator.errors)


def test_invalid_service_version():
    clear_env()

    set_env({
        "orgName": "neso",
        "schemaType": "eqbd",
        "productType": "oil",
        "SERVICE_NAME": "test",
        "SERVICE_VERSION": "invalid",
        "bootstrapServer": "localhost:9092",
        "srcTopicName": "topic",
    })

    validator = AdaptorConfigValidator(exit_on_failure=False)
    validator.validate_all()

    assert any("SERVICE_VERSION" in e for e in validator.errors)


# ======================================================
# KAFKA VALIDATION TESTS
# ======================================================

def test_invalid_bootstrap_missing():
    clear_env()

    set_env({
        "orgName": "a",
        "schemaType": "b",
        "productType": "c",
        "SERVICE_NAME": "svc",
        "SERVICE_VERSION": "1.0.0",
        "srcTopicName": "topic",
    })

    v = AdaptorConfigValidator(exit_on_failure=False)
    v.validate_all()

    assert any("bootstrapServer" in e for e in v.errors)


def test_invalid_bootstrap_format():
    clear_env()

    set_env({
        "orgName": "a",
        "schemaType": "b",
        "productType": "c",
        "SERVICE_NAME": "svc",
        "SERVICE_VERSION": "1.0.0",
        "bootstrapServer": "invalid-format",
        "srcTopicName": "topic",
    })

    v = AdaptorConfigValidator(exit_on_failure=False)
    v.validate_all()

    assert any("expected host:port" in e for e in v.errors)


def test_valid_multiple_bootstrap():
    clear_env()

    set_env({
        "orgName": "a",
        "schemaType": "b",
        "productType": "c",
        "SERVICE_NAME": "svc",
        "SERVICE_VERSION": "1.0.0",
        "bootstrapServer": "host1:9092,host2:9092",
        "srcTopicName": "topic",
    })

    v = AdaptorConfigValidator(exit_on_failure=False)
    v.validate_all()

    assert len(v.errors) == 0


# ======================================================
# ADAPTOR TESTS
# ======================================================

def test_adaptor_valid_config():
    clear_env()

    set_env({
        "orgName": "neso",
        "schemaType": "eqbd",
        "productType": "oil",
        "SERVICE_NAME": "svc",
        "SERVICE_VERSION": "1.0.0",
        "bootstrapServer": "localhost:9092",
        "srcTopicName": "valid-topic",
    })

    v = AdaptorConfigValidator(exit_on_failure=False)
    v.validate_all()

    assert v.errors == []


def test_adaptor_missing_src_topic():
    clear_env()

    set_env({
        "orgName": "neso",
        "schemaType": "eqbd",
        "productType": "oil",
        "SERVICE_NAME": "svc",
        "SERVICE_VERSION": "1.0.0",
        "bootstrapServer": "localhost:9092",
    })

    v = AdaptorConfigValidator(exit_on_failure=False)
    v.validate_all()

    assert any("srcTopicName" in e for e in v.errors)


def test_adaptor_invalid_topic_spacing():
    clear_env()

    set_env({
        "orgName": "neso",
        "schemaType": "eqbd",
        "productType": "oil",
        "SERVICE_NAME": "svc",
        "SERVICE_VERSION": "1.0.0",
        "bootstrapServer": "localhost:9092",
        "srcTopicName": " bad topic ",
    })

    v = AdaptorConfigValidator(exit_on_failure=False)
    v.validate_all()

    assert any("must not contain spaces" in e for e in v.errors)


def test_adaptor_optional_mapper_topic():
    clear_env()

    set_env({
        "orgName": "neso",
        "schemaType": "eqbd",
        "productType": "oil",
        "SERVICE_NAME": "svc",
        "SERVICE_VERSION": "1.0.0",
        "bootstrapServer": "localhost:9092",
        "srcTopicName": "topic",
        "mapperTopicName": " valid ",
    })

    v = AdaptorConfigValidator(exit_on_failure=False)
    v.validate_all()

    assert any("leading/trailing" in e for e in v.errors)


# ======================================================
# SCHEMA MAPPER TESTS
# ======================================================

def test_schema_mapper_valid():
    clear_env()

    set_env({
        "orgName": "neso",
        "schemaType": "eqbd",
        "productType": "oil",
        "SERVICE_NAME": "svc",
        "SERVICE_VERSION": "1.0.0",
        "bootstrapServer": "localhost:9092",
        "mapperTopicName": "mapper-topic",
    })

    v = SchemaMapperConfigValidator(exit_on_failure=False)
    v.validate_all()

    assert v.errors == []


def test_schema_mapper_missing_mapper_topic():
    clear_env()

    set_env({
        "orgName": "neso",
        "schemaType": "eqbd",
        "productType": "oil",
        "SERVICE_NAME": "svc",
        "SERVICE_VERSION": "1.0.0",
        "bootstrapServer": "localhost:9092",
    })

    v = SchemaMapperConfigValidator(exit_on_failure=False)
    v.validate_all()

    assert any("mapperTopicName" in e for e in v.errors)


def test_schema_mapper_optional_target_topic():
    clear_env()

    set_env({
        "orgName": "neso",
        "schemaType": "eqbd",
        "productType": "oil",
        "SERVICE_NAME": "svc",
        "SERVICE_VERSION": "1.0.0",
        "bootstrapServer": "localhost:9092",
        "mapperTopicName": "mapper-topic",
        "targetTopicName": " bad topic ",
    })

    v = SchemaMapperConfigValidator(exit_on_failure=False)
    v.validate_all()

    assert any("targetTopicName" in e for e in v.errors)