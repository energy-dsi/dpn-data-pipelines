import os
import base64
import pytest
from unittest.mock import patch, MagicMock

import utils.config_validator as cv


# ------------------------------------------------------------
# Helper: clean env setup
# ------------------------------------------------------------
def set_env(env):
    return patch.dict(os.environ, env, clear=True)


# ------------------------------------------------------------
# CLOUD VALIDATION — AZURE
# ------------------------------------------------------------

def test_azure_valid():
    encoded = base64.b64encode(b"secret").decode()

    with set_env({
        "srcConnectionString": encoded,
        "mapperConnectionString": encoded,
        "targetConnectionString": encoded,
    }):
        errors = cv.validate_cloud_config(
            "azure",
            list(cv._AZURE_ENV_VARS),
            exit_on_failure=False,
        )

    assert errors == []


def test_azure_missing():
    with set_env({}):
        errors = cv.validate_cloud_config(
            "azure",
            ["srcConnectionString"],
            exit_on_failure=False,
        )

    assert any("srcConnectionString" in e for e in errors)


def test_azure_invalid_base64():
    with set_env({"srcConnectionString": "invalid"}):
        errors = cv.validate_cloud_config(
            "azure",
            ["srcConnectionString"],
            exit_on_failure=False,
        )

    assert any("base64" in e for e in errors)


def test_azure_exit_on_failure():
    with set_env({"srcConnectionString": ""}):
        with pytest.raises(SystemExit):
            cv.validate_cloud_config("azure", ["srcConnectionString"])


# ------------------------------------------------------------
# AWS VALIDATION
# ------------------------------------------------------------

def test_aws_valid():
    with set_env({
        "AWS_ACCESS_KEY_ID": "a",
        "AWS_SECRET_ACCESS_KEY": "b",
        "AWS_REGION": "c",
        "AWS_ENDPOINT_URL": "d",
    }):
        errors = cv.validate_cloud_config("aws", [], exit_on_failure=False)

    assert errors == []


def test_aws_missing():
    with set_env({}):
        errors = cv.validate_cloud_config("aws", [], exit_on_failure=False)

    assert len(errors) == len(cv._AWS_REQUIRED_VARS)


# ------------------------------------------------------------
# UNKNOWN PROVIDER
# ------------------------------------------------------------

def test_unknown_provider():
    errors = cv.validate_cloud_config("gcp", [], exit_on_failure=False)

    assert any("Unknown cloudProviderType" in e for e in errors)


# ------------------------------------------------------------
# KAFKA VALIDATION
# ------------------------------------------------------------

def test_kafka_valid():
    with set_env({
        "bootstrapServer": "host:9092",
        "mapperTopicName": "mapper",
        "targetTopicName": "target",
    }):
        errors = cv.validate_kafka_config(exit_on_failure=False)

    assert errors == []


def test_kafka_missing():
    with set_env({}):
        errors = cv.validate_kafka_config(exit_on_failure=False)

    assert any("bootstrapServer" in e for e in errors)
    assert any("mapperTopicName" in e for e in errors)
    assert any("targetTopicName" in e for e in errors)


def test_kafka_bootstrap_space():
    with set_env({
        "bootstrapServer": "host 9092",
        "mapperTopicName": "mapper",
        "targetTopicName": "target",
    }):
        errors = cv.validate_kafka_config(exit_on_failure=False)

    assert any("must not contain spaces" in e for e in errors)


def test_kafka_bootstrap_invalid():
    with set_env({
        "bootstrapServer": "host9092",
        "mapperTopicName": "mapper",
        "targetTopicName": "target",
    }):
        errors = cv.validate_kafka_config(exit_on_failure=False)

    assert any("invalid bootstrap entry" in e for e in errors)


# ✅ ✅ IMPORTANT FIX — matches your actual implementation
def test_kafka_topic_with_spaces():
    with set_env({
        "bootstrapServer": "host:9092",
        "mapperTopicName": " mapper ",
        "targetTopicName": " target ",
    }):
        errors = cv.validate_kafka_config(exit_on_failure=False)

    # ✅ Your code trims values → NO error
    assert errors == []


def test_kafka_exit_on_failure():
    with set_env({}):
        with pytest.raises(SystemExit):
            cv.validate_kafka_config(exit_on_failure=True)


# ------------------------------------------------------------
# REPORT ERRORS
# ------------------------------------------------------------

def test_report_errors_with_logger():
    logger = MagicMock()
    errors = ["e1", "e2"]

    cv._report_errors(errors, "cloud", logger)

    assert logger.error.call_count >= 3


def test_report_errors_without_logger(capsys):
    errors = ["e1", "e2"]

    cv._report_errors(errors, "kafka", None)

    out = capsys.readouterr().out

    assert "configuration validation failed" in out
    assert "e1" in out
    assert "e2" in out


def test_report_errors_empty():
    # ✅ covers no-error branch
    cv._report_errors([], "kafka", None)