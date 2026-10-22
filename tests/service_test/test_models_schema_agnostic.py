# Copyright 2026 DSI Project
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
"""
AC1 — Support Schema Agnostic Provisioning.

DataProductCreate/DataProductDefinition (service/models.py) carry no schema
field at all — these tests pin that down so a future change can't
reintroduce a mandatory "schema type" without a test failing here. FR
references are to the AC1 ticket's Functional Requirements list.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from service.models import DataProductCreate, DataProductDefinition


def test_create_has_no_schema_field():
    """FR2 — schema metadata isn't merely optional, it doesn't exist as a field."""
    fields = DataProductCreate.model_fields.keys()
    assert not any("schema" in name.lower() for name in fields)


def test_definition_has_no_schema_field():
    fields = DataProductDefinition.model_fields.keys()
    assert not any("schema" in name.lower() for name in fields)


def test_file_pipeline_created_without_schema():
    """FR1/FR3 — a file-pathway product is created supplying only the
    documented fields; no schema kwarg is required or accepted."""
    step1 = DataProductCreate(product_type="bp-natural-gas-02", pipeline_type="file")
    assert step1.product_type == "bp-natural-gas-02"
    assert step1.pipeline_type == "file"


def test_topic_pipeline_created_without_schema():
    step1 = DataProductCreate(product_type="bp-natural-gas-02", pipeline_type="topic")
    assert step1.pipeline_type == "topic"


def test_definition_completes_without_schema_file_pathway():
    """FR4 — provisioning (constructing the stored definition) succeeds with
    no schema info supplied, regardless of pipeline_type."""
    definition = DataProductDefinition(
        product_type="bp-natural-gas-02",
        pipeline_type="file",
        org_name="dpn",
        cloud_provider="aws",
        source_container_name="bp-natural-gas-02-src",
        target_container_name="bp-natural-gas-02-target",
    )
    assert definition.is_active is True
    assert definition.pipeline_type == "file"


def test_definition_completes_without_schema_topic_pathway():
    definition = DataProductDefinition(
        product_type="bp-natural-gas-02",
        pipeline_type="topic",
        org_name="dpn",
        cloud_provider="aws",
        src_topic_name="dpn-producer-bp-natural-gas-02-src",
        target_topic_name="dpn-producer-dpn-bp-natural-gas-02-target",
    )
    assert definition.pipeline_type == "topic"


def test_legacy_schema_kwarg_is_ignored_not_required():
    """A caller that still passes a legacy 'schema_type' kwarg is neither
    required to nor blocked from doing so — it's simply not a recognized
    field, proving schema is unrecognized rather than mandatory under a
    different name."""
    step1 = DataProductCreate(
        product_type="bp-natural-gas-02",
        pipeline_type="file",
        schema_type="eq",
    )
    assert not hasattr(step1, "schema_type")


def test_missing_schema_never_raises_validation_error():
    """FR3/FR4 — omitting schema entirely (the normal case) never raises."""
    try:
        DataProductCreate(product_type="bp-natural-gas-02", pipeline_type="topic")
    except ValidationError as exc:
        pytest.fail(f"provisioning without schema unexpectedly failed validation: {exc}")
