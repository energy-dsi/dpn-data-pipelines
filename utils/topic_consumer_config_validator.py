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
Minimal Runtime Validators for Topic Extractor and Schema Mapper

Purpose:
Validate only the environment variables that are REQUIRED at runtime.
No governance/metadata validation included.
"""

import os
import re
import sys
from typing import List


# =========================================================
# Base Validator
# =========================================================
class BaseValidator:
    def __init__(self, service: str) -> None:
        self.service = service
        self.errors: List[str] = []

    def _get(self, key: str) -> str:
        return os.getenv(key, "")

    def _require(self, key: str) -> str:
        value = self._get(key)
        if not value:
            self.errors.append(f"[{self.service}] '{key}' is required")
        return value

    def _no_edge_spaces(self, key: str, value: str) -> None:
        if value and value.strip() != value:
            self.errors.append(f"[{self.service}] '{key}' has leading/trailing spaces")

    def _no_internal_spaces(self, key: str, value: str) -> None:
        if value and " " in value:
            self.errors.append(f"[{self.service}] '{key}' must not contain spaces")

    def _validate_kafka_bootstrap(self, value: str) -> None:
        if not value:
            self.errors.append(f"[{self.service}] 'bootstrapServer' is required")
            return

        if " " in value:
            self.errors.append(f"[{self.service}] 'bootstrapServer' must not contain spaces")

        for entry in value.split(","):
            if ":" not in entry:
                self.errors.append(
                    f"[{self.service}] invalid bootstrap entry '{entry}' (expected host:port)"
                )

    def validate(self) -> None:
        if self.errors:
            print(f"{self.service.upper()} validation failed:")
            for err in self.errors:
                print("-", err)
            sys.exit(1)


# =========================================================
# Extractor Validator
# =========================================================
class ExtractorValidator(BaseValidator):
    """
    Validates:
    - bootstrapServer (required)
    - srcTopicName (required)
    - mapperTopicName (optional)
    """

    def __init__(self) -> None:
        super().__init__("extractor")

    def validate_all(self) -> None:
        # Required
        bootstrap = self._require("bootstrapServer")
        src_topic = self._require("srcTopicName")

        # Optional
        mapper_topic = self._get("mapperTopicName")

        # Validations
        self._validate_kafka_bootstrap(bootstrap)

        self._no_edge_spaces("srcTopicName", src_topic)
        self._no_internal_spaces("srcTopicName", src_topic)

        if mapper_topic:
            self._no_edge_spaces("mapperTopicName", mapper_topic)
            self._no_internal_spaces("mapperTopicName", mapper_topic)

        self.validate()


# =========================================================
# Schema Mapper Validator
# =========================================================
class SchemaMapperValidator(BaseValidator):
    """
    Validates:
    - bootstrapServer (required)
    - mapperTopicName (required)
    """

    def __init__(self) -> None:
        super().__init__("schema-mapper")

    def validate_all(self) -> None:
        bootstrap = self._require("bootstrapServer")
        mapper_topic = self._require("mapperTopicName")

        # Validations
        self._validate_kafka_bootstrap(bootstrap)

        self._no_edge_spaces("mapperTopicName", mapper_topic)
        self._no_internal_spaces("mapperTopicName", mapper_topic)

        self.validate()