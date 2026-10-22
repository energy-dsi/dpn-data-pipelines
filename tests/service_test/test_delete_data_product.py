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
Pins down ui/app.py's delete_data_product route (POST
/data-products/{product_type}/delete): deleting a TOPIC-pathway data
product must delete both of its Kafka topics (src_topic_name and
target_topic_name), not just remove the registry entry/Deployment.

Drives the real FastAPI route with TestClient, same pattern as
test_provisioning_api_schema_agnostic.py — registry redirected to a
tmp_path, runtime.launch/stop and precheck.delete_kafka_topic/
delete_storage_container stubbed so this never touches a real
Kubernetes/Podman backend, Kafka broker, or cloud storage account.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from service import auth, precheck, registry, registry_backup, runtime
from ui.app import app


@pytest.fixture(autouse=True)
def _isolated_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(registry, "_DATA_DIR", tmp_path)
    monkeypatch.setattr(registry, "_REGISTRY_FILE", tmp_path / "data_products.json")
    monkeypatch.setattr(registry_backup, "backup", lambda content: None)
    monkeypatch.setattr(runtime, "launch", lambda dp: None)
    monkeypatch.setattr(runtime, "stop", lambda dp: None)
    app.dependency_overrides[auth.require_admin] = lambda: None
    app.dependency_overrides[auth.require_operator] = lambda: None
    yield
    app.dependency_overrides.clear()


@pytest.fixture()
def client() -> TestClient:
    return TestClient(app)


def _create_topic_product(client: TestClient, product_type: str, src_topic: str, target_topic: str) -> None:
    response = client.post(
        "/data-products/confirm",
        data={
            "product_type": product_type,
            "pipeline_type": "topic",
            "src_topic_name": src_topic,
            "target_topic_name": target_topic,
        },
        follow_redirects=False,
    )
    assert response.status_code == 303


def test_delete_topic_product_deletes_both_kafka_topics(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    deleted_topics: list[str] = []
    monkeypatch.setattr(precheck, "delete_kafka_topic", lambda topic_name: deleted_topics.append(topic_name))

    product_type = "bp-natural-gas-04"
    src_topic = "dpn-producer-bp-natural-gas-04-src"
    target_topic = "dpn-producer-dpn-bp-natural-gas-04-target"
    _create_topic_product(client, product_type, src_topic, target_topic)

    response = client.post(f"/data-products/{product_type}/delete", follow_redirects=False)

    assert response.status_code == 303
    assert set(deleted_topics) == {src_topic, target_topic}

    with pytest.raises(registry.ProductNotFoundError):
        registry.get_product(product_type)


def test_delete_topic_product_is_404_for_unknown_product(client: TestClient):
    response = client.post("/data-products/does-not-exist/delete", follow_redirects=False)
    assert response.status_code == 404
