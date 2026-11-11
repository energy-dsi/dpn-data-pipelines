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
AC1 — Support Schema Agnostic Provisioning: end-to-end API coverage.

Drives ui/app.py's actual FastAPI routes (preview + confirm) with
FastAPI's TestClient, proving a data product can be fully provisioned
through the real HTTP surface without ever supplying schema information —
no such field exists on the form, so there's nothing to omit "correctly";
this just pins down that the whole request/response cycle completes.

registry's on-disk JSON store is redirected to a tmp_path, and
runtime.launch/registry_backup.backup are stubbed so this never touches a
real podman/Kubernetes backend or a real cloud storage account.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from service import auth, registry, registry_backup, runtime
from ui.app import app


@pytest.fixture(autouse=True)
def _isolated_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Every test gets its own empty registry file and never calls the real
    cloud backup or a real container runtime."""
    monkeypatch.setattr(registry, "_DATA_DIR", tmp_path)
    monkeypatch.setattr(registry, "_REGISTRY_FILE", tmp_path / "data_products.json")
    monkeypatch.setattr(registry_backup, "backup", lambda content: None)
    monkeypatch.setattr(runtime, "launch", lambda dp: None)
    # /create-new-data-product, /data-products/preview, /data-products/confirm all require
    # auth.require_operator — bypass the oauth2-proxy-header dependency
    # entirely so these tests exercise the schema-agnostic provisioning
    # logic itself, not the auth layer (which is unrelated to AC1).
    app.dependency_overrides[auth.require_operator] = lambda: None
    yield
    app.dependency_overrides.clear()


@pytest.fixture()
def client() -> TestClient:
    return TestClient(app)


def test_preview_accepts_request_with_no_schema_field(client: TestClient):
    """FR3 — the preview endpoint's form has no schema parameter at all;
    the request succeeds without one."""
    response = client.post(
        "/data-products/preview",
        data={"product_type": "bp-natural-gas-02", "pipeline_type": "file"},
    )
    assert response.status_code == 200
    assert "schema" not in response.text.lower()


def test_confirm_provisions_file_pipeline_without_schema(client: TestClient):
    """FR1/FR4 — a full create flow (preview + confirm) completes and the
    product lands in the registry, with no schema info ever supplied."""
    response = client.post(
        "/data-products/confirm",
        data={
            "product_type": "bp-natural-gas-02",
            "pipeline_type": "file",
            "source_container_name": "bp-natural-gas-02-src",
            "target_container_name": "bp-natural-gas-02-target",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    product = registry.get_product("bp-natural-gas-02")
    assert product.pipeline_type == "file"
    assert not hasattr(product, "schema_type")


def test_confirm_provisions_topic_pipeline_without_schema(client: TestClient):
    response = client.post(
        "/data-products/confirm",
        data={
            "product_type": "bp-natural-gas-03",
            "pipeline_type": "topic",
            "src_topic_name": "dpn-producer-bp-natural-gas-03-src",
            "target_topic_name": "dpn-producer-dpn-bp-natural-gas-03-target",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    product = registry.get_product("bp-natural-gas-03")
    assert product.pipeline_type == "topic"


def test_confirm_rejects_only_documented_validation_errors_not_missing_schema(
    client: TestClient,
):
    """A request missing the *required* product_type/pipeline_type fields
    still fails validation as expected — but nothing about "schema" ever
    appears in that error, confirming schema was never part of what's
    being validated."""
    response = client.post("/data-products/confirm", data={}, follow_redirects=False)
    assert response.status_code == 400
    assert "schema" not in response.text.lower()
