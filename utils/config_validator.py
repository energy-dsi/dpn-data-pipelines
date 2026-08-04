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
#
# +---------+----------------------------------------------------------+---------------+-------------+
# | Version | Description                                              | Change Owner  | Change Date |
# +---------+----------------------------------------------------------+---------------+-------------+
# | 1.0.0   | Initial version                                          | DSI Team      | 2026-05-01  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
Input configuration validator.
"""
from __future__ import annotations

import logging
import os
import sys
import base64


# ── Azure config map ─────────────────────────────────────
_AZURE_ENV_VARS: dict[str, str] = {
    "srcConnectionString": "Source Azure Storage connection string",
    "mapperConnectionString": "Mapper staging Azure Storage connection string",
    "targetConnectionString": "Target Azure Storage connection string",
}

# ── AWS required vars ────────────────────────────────────
_AWS_REQUIRED_VARS: list[tuple[str, str]] = [
    ("AWS_ACCESS_KEY_ID", "AWS / MinIO access key ID"),
    ("AWS_SECRET_ACCESS_KEY", "AWS / MinIO secret access key"),
    ("AWS_REGION", "AWS region"),
    ("AWS_ENDPOINT_URL", "S3-compatible endpoint URL"),
]


# =========================================================
# Cloud validation
# =========================================================
def validate_cloud_config(
    cloud_provider: str,
    azure_fields: list[str],
    logger: logging.Logger | None = None,
    exit_on_failure: bool = True,
) -> list[str]:

    errors: list[str] = []
    provider = (cloud_provider or "").lower().strip()

    if provider == "azure":
        for field in azure_fields:
            value = os.getenv(field, "").strip()

            if not value:
                errors.append(f"[azure] '{field}' is required but empty")
                continue

            try:
                base64.b64decode(value)
            except Exception:
                errors.append(f"[azure] '{field}' is not valid base64")

    elif provider == "aws":
        for env_var, desc in _AWS_REQUIRED_VARS:
            value = os.getenv(env_var, "").strip()
            if not value:
                errors.append(f"[aws] '{env_var}' is required but empty ({desc})")

    else:
        errors.append(
            f"Unknown cloudProviderType '{cloud_provider}'. Expected 'azure' or 'aws'"
        )

    if errors:
        _report_errors(errors, "cloud", logger)
        if exit_on_failure:
            sys.exit(1)

    return errors


# =========================================================
# Kafka validation
# =========================================================
def validate_kafka_config(
    logger: logging.Logger | None = None,
    exit_on_failure: bool = True,
) -> list[str]:

    errors: list[str] = []

    bootstrap = os.getenv("bootstrapServer", "").strip()
    mapper_topic = os.getenv("mapperTopicName", "").strip()
    target_topic = os.getenv("targetTopicName", "").strip()

    # bootstrap validation
    if not bootstrap:
        errors.append("[kafka] 'bootstrapServer' is required but empty")
    else:
        if " " in bootstrap:
            errors.append("[kafka] 'bootstrapServer' must not contain spaces")

        for server in bootstrap.split(","):
            if ":" not in server:
                errors.append(
                    f"[kafka] invalid bootstrap entry '{server}' (expected host:port)"
                )

    # mapper topic
    if not mapper_topic:
        errors.append("[kafka] 'mapperTopicName' is required but empty")
    elif mapper_topic.strip() != mapper_topic:
        errors.append("[kafka] 'mapperTopicName' must not have leading/trailing spaces")

    # target topic
    if not target_topic:
        errors.append("[kafka] 'targetTopicName' is required but empty")
    elif target_topic.strip() != target_topic:
        errors.append("[kafka] 'targetTopicName' must not have leading/trailing spaces")

    if errors:
        _report_errors(errors, "kafka", logger)
        if exit_on_failure:
            sys.exit(1)

    return errors


# =========================================================
# OTEL-aligned error reporting
# =========================================================
def _report_errors(
    errors: list[str],
    config_type: str,
    logger: logging.Logger | None,
):
    summary = f"{config_type.upper()} configuration validation failed"

    if logger:
        logger.error(
            summary,
            extra={
                "event.name": "config.validation.failed",
                "config.type": config_type,
                "error.count": len(errors),
                "errors": errors,
            },
        )

        for err in errors:
            logger.error(
                "Config validation error",
                extra={
                    "event.name": "config.validation.error",
                    "config.type": config_type,
                    "error.message": err,
                },
            )
    else:
        print(summary)
        for err in errors:
            print(err)