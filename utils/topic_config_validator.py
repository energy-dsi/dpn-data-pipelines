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
Configuration Validators for Adaptor and Schema Mapper.

This module provides a structured, class-based validation framework for
runtime environment variables used by:

1. Topic Adaptor
2. Topic Schema Mapper

Key Features:
- Centralized validation logic
- Reusable base validator
- OTEL-aligned logging
- Fail-fast behavior
- Clear and structured error reporting
"""

from __future__ import annotations

import os
import sys
import logging
import re
from typing import List


# =========================================================
# Base Validator
# =========================================================
class BaseConfigValidator:
    """
    Base configuration validator.

    Provides:
    - Error collection
    - Common validation helpers
    - OTEL-style error reporting
    - Fail-fast behavior

    This class should be extended by service-specific validators.
    """

    def __init__(
        self,
        config_type: str,
        logger: logging.Logger | None = None,
        exit_on_failure: bool = True,
    ) -> None:
        self.config_type = config_type
        self.logger = logger
        self.exit_on_failure = exit_on_failure
        self.errors: List[str] = []

    # -----------------------------------------------------
    # Helper Methods
    # -----------------------------------------------------

    def _get_env(self, key: str) -> str:
        """Read environment variable safely."""
        return os.getenv(key, "")

    def _require(self, key: str) -> str:
        """
        Validate that a required environment variable exists.

        Returns value if valid.
        """
        value = self._get_env(key)
        if not value:
            self.errors.append(f"[{self.config_type}] '{key}' is required but empty")
        return value

    def _validate_optional(self, key: str) -> str:
        """
        Validate optional variable (only if present).
        """
        return self._get_env(key)

    def _no_whitespace_edges(self, key: str, value: str) -> None:
        """
        Ensure no leading or trailing whitespace.
        """
        if value and value.strip() != value:
            self.errors.append(
                f"[{self.config_type}] '{key}' must not have leading/trailing spaces"
            )

    def _no_internal_spaces(self, key: str, value: str) -> None:
        """
        Ensure no spaces inside value.
        """
        if value and " " in value:
            self.errors.append(
                f"[{self.config_type}] '{key}' must not contain spaces"
            )

    def _validate_kafka_bootstrap(self, value: str) -> None:
        """
        Validate Kafka bootstrap server format.

        Expected:
        - host:port
        - multiple entries supported (comma-separated)
        """
        if not value:
            self.errors.append(
                f"[{self.config_type}] 'bootstrapServer' is required but empty"
            )
            return

        if " " in value:
            self.errors.append(
                f"[{self.config_type}] 'bootstrapServer' must not contain spaces"
            )

        for server in value.split(","):
            if ":" not in server:
                self.errors.append(
                    f"[{self.config_type}] invalid bootstrap entry '{server}' (expected host:port)"
                )

    def _validate_version(self, value: str) -> None:
        """
        Validate SERVICE_VERSION format (e.g. 1.0.0).
        """
        if value and not re.match(r"^v?\d+\.\d+\.\d+$", value):
            self.errors.append(
                f"[{self.config_type}] 'SERVICE_VERSION' must follow format X.Y.Z"
            )

    # -----------------------------------------------------
    # Error Reporting
    # -----------------------------------------------------

    def _report_errors(self) -> None:
        """
        Report validation errors using OTEL-style logging.
        """
        summary = f"{self.config_type.upper()} configuration validation failed"

        if self.logger:
            self.logger.error(
                summary,
                extra={
                    "event.name": "config.validation.failed",
                    "config.type": self.config_type,
                    "error.count": len(self.errors),
                    "errors": self.errors,
                },
            )

            for err in self.errors:
                self.logger.error(
                    "Config validation error",
                    extra={
                        "event.name": "config.validation.error",
                        "config.type": self.config_type,
                        "error.message": err,
                    },
                )
        else:
            print(summary)
            for err in self.errors:
                print(err)

        if self.exit_on_failure:
            sys.exit(1)


# =========================================================
# Common Metadata Validator
# =========================================================
class CommonMetadataValidator(BaseConfigValidator):
    """
    Validates common metadata required by both Adaptor and Schema Mapper.

    Fields:
    - orgName
    - schemaType
    - productType
    - SERVICE_NAME
    - SERVICE_VERSION
    """

    def validate_metadata(self) -> None:
        """Validate shared metadata fields."""

        # Required metadata fields
        org = self._require("orgName")
        schema = self._require("schemaType")
        product = self._require("productType")
        service = self._require("SERVICE_NAME")
        version = self._require("SERVICE_VERSION")

        # Basic whitespace checks
        for key, value in [
            ("orgName", org),
            ("schemaType", schema),
            ("productType", product),
            ("SERVICE_NAME", service),
        ]:
            self._no_whitespace_edges(key, value)

        # Version format
        self._validate_version(version)


# =========================================================
# Adaptor Validator
# =========================================================
class AdaptorConfigValidator(CommonMetadataValidator):
    """
    Configuration validator for Topic Adaptor.

    Validates:
    - Common metadata
    - Kafka bootstrap server
    - Source topic (required)
    - Mapper topic (optional)
    """

    def __init__(self, logger: logging.Logger | None = None, exit_on_failure: bool = True,):
        super().__init__(config_type="adaptor", logger=logger, exit_on_failure=exit_on_failure,)

    # -----------------------------------------------------
    # Validation Sections
    # -----------------------------------------------------

    def _validate_kafka(self) -> None:
        """Validate Kafka bootstrap configuration."""
        bootstrap = self._require("bootstrapServer")
        self._validate_kafka_bootstrap(bootstrap)

    def _validate_topics(self) -> None:
        """Validate topic configuration."""

        # Required
        src_topic = self._require("srcTopicName")
        self._no_whitespace_edges("srcTopicName", src_topic)
        self._no_internal_spaces("srcTopicName", src_topic)

        # Optional
        mapper_topic = self._validate_optional("mapperTopicName")
        if mapper_topic:
            self._no_whitespace_edges("mapperTopicName", mapper_topic)
            self._no_internal_spaces("mapperTopicName", mapper_topic)

    # -----------------------------------------------------
    # Entry Point
    # -----------------------------------------------------

    def validate_all(self) -> None:
        """
        Execute all adaptor validations.
        """
        self.validate_metadata()
        self._validate_kafka()
        self._validate_topics()

        if self.errors:
            self._report_errors()


# =========================================================
# Schema Mapper Validator
# =========================================================
class SchemaMapperConfigValidator(CommonMetadataValidator):
    """
    Configuration validator for Topic Schema Mapper.

    Validates:
    - Common metadata
    - Kafka bootstrap server
    - Mapper topic (required - input)
    - Target topic (optional)
    """

    def __init__(self, logger: logging.Logger | None = None, exit_on_failure: bool = True,):
        super().__init__(config_type="schema-mapper", logger=logger,  exit_on_failure=exit_on_failure,)

    # -----------------------------------------------------
    # Validation Sections
    # -----------------------------------------------------

    def _validate_kafka(self) -> None:
        """Validate Kafka bootstrap configuration."""
        bootstrap = self._require("bootstrapServer")
        self._validate_kafka_bootstrap(bootstrap)

    def _validate_topics(self) -> None:
        """Validate topic configuration."""

        # REQUIRED (used as source topic internally)
        mapper_topic = self._require("mapperTopicName")
        self._no_whitespace_edges("mapperTopicName", mapper_topic)
        self._no_internal_spaces("mapperTopicName", mapper_topic)

        # OPTIONAL
        target_topic = self._validate_optional("targetTopicName")
        if target_topic:
            self._no_whitespace_edges("targetTopicName", target_topic)
            self._no_internal_spaces("targetTopicName", target_topic)

    # -----------------------------------------------------
    # Entry Point
    # -----------------------------------------------------

    def validate_all(self) -> None:
        """
        Execute all schema mapper validations.
        """
        self.validate_metadata()
        self._validate_kafka()
        self._validate_topics()

        if self.errors:
            self._report_errors()
