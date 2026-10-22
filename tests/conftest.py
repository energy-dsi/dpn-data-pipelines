# Copyright 2025 Platform Team – Apache 2.0 License
"""
Shared pytest fixtures used across all test modules.

All fixtures that patch environment variables or expensive dependencies
(Azure SDK, boto3, confluent-kafka) live here so each test module imports
only the fixtures it needs without reimplementing patching boilerplate.
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Base environment variable sets
# ---------------------------------------------------------------------------
AZURE_ENV: dict[str, str] = {
    "cloudProviderType":   "azure",
    "srcBootstrapServer":  "localhost:9092",
    "tgtBootstrapServer":  "localhost:9093",
    "mapperTopicName":     "eq-mapper-events",
    "targetTopicName":     "eq-target-events",
    # Empty strings are valid base64 – each decode gives ""
    "srcConnectionString":"QmxvYkVuZHBvaW50PWh0dHBzOi8vbXlzdG9yYWdlYWNjb3VudC5ibG9iLmNvcmUud2luZG93cy5uZXQvO1NoYXJlZEFjY2Vzc1NpZ25hdHVyZT1zdj0yMDIzLTAxLTAzJnNzPWImc3J0PXNjbyZzcD1yd2RsYWMmc2U9MjAzMC0wMS0wMVQwMDowMDowMFomc3Q9MjAyNC0wMS0wMVQwMDowMDowMFomc3ByPWh0dHBzJnNpZz1hYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejEyMzQ1Njc4OTAlM0Q=",
    "mapperConnectionString": "QmxvYkVuZHBvaW50PWh0dHBzOi8vbXlzdG9yYWdlYWNjb3VudC5ibG9iLmNvcmUud2luZG93cy5uZXQvO1NoYXJlZEFjY2Vzc1NpZ25hdHVyZT1zdj0yMDIzLTAxLTAzJnNzPWImc3J0PXNjbyZzcD1yd2RsYWMmc2U9MjAzMC0wMS0wMVQwMDowMDowMFomc3Q9MjAyNC0wMS0wMVQwMDowMDowMFomc3ByPWh0dHBzJnNpZz1hYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejEyMzQ1Njc4OTAlM0Q=",
    "targetConnectionString": "QmxvYkVuZHBvaW50PWh0dHBzOi8vbXlzdG9yYWdlYWNjb3VudC5ibG9iLmNvcmUud2luZG93cy5uZXQvO1NoYXJlZEFjY2Vzc1NpZ25hdHVyZT1zdj0yMDIzLTAxLTAzJnNzPWImc3J0PXNjbyZzcD1yd2RsYWMmc2U9MjAzMC0wMS0wMVQwMDowMDowMFomc3Q9MjAyNC0wMS0wMVQwMDowMDowMFomc3ByPWh0dHBzJnNpZz1hYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejEyMzQ1Njc4OTAlM0Q=",
    "srcContainerName":    "src",
    "mapperContainerName": "mapper",
    "targetContainerName": "target",
    "orgName":             "neso",
    "schemaType":          "eq",
    "scheduleInterval":    "60",
    "SERVICE_NAME":        "test-service",
    "SERVICE_VERSION":     "0.0.1",
}

AWS_ENV: dict[str, str] = {
    **AZURE_ENV,
    "cloudProviderType":   "aws",
    "AWS_ENDPOINT_URL":    "http://localhost:9000",
    "AWS_ACCESS_KEY_ID":   "bWluaW9hZG1pbg==",
    "AWS_SECRET_ACCESS_KEY": "bWluaW9hZG1pbg==",
    "AWS_REGION":          "us-east-1",
}


@pytest.fixture()
def azure_env(monkeypatch):
    """Patch environment for Azure-backed services."""
    for k, v in AZURE_ENV.items():
        monkeypatch.setenv(k, v)
    yield AZURE_ENV


@pytest.fixture()
def aws_env(monkeypatch):
    """Patch environment for AWS/MinIO-backed services."""
    for k, v in AWS_ENV.items():
        monkeypatch.setenv(k, v)
    yield AWS_ENV


# ---------------------------------------------------------------------------
# Reusable mock factories
# ---------------------------------------------------------------------------
@pytest.fixture()
def mock_logger():
    return MagicMock()


@pytest.fixture()
def mock_data_trans():
    return MagicMock()


@pytest.fixture()
def mock_kafka_trans():
    return MagicMock()
