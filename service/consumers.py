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
#
# +---------+----------------------------------------------------------+---------------+-------------+
# | Version | Description                                              | Change Owner  | Change Date |
# +---------+----------------------------------------------------------+---------------+-------------+
# | 1.0.0   | Initial version                                           | DSI Team      | 2026-09-14  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
Fixed registry of the consumer pipelines this UI monitors.

Unlike producer data products (service/registry.py), the consumer side
is NOT something this UI creates — per service/README.md and
compose.yaml, there is exactly one consumer-file pipeline and one
consumer-topic pipeline for the whole platform, each a SINGLE extractor
container — the old schema_mapper stage was absorbed into the extractor
entirely (staging container/mapper topic removed), the same way the
producer side's own adaptor absorbed its schema mapper (see
blueprints/producer/{file,topic}/adaptor/main.py and
blueprints/consumer/{file,topic}/extractor/main.py's changelog entries).
They receive every subscribed data product through the same well-known
dp-consumer-* storage containers/topics, so there is nothing per-product
to configure and no "new consumer" flow — this module is a plain
read-only list of pipelines, not a registry.py-style create/list/get
store.

Execution mode (automatic/manual), unlike everything else here, IS
mutable — same Pause/Resume/Run Now semantics as a producer data product
(see ui/app.py's /consumer-pipelines/{key}/pause|resume|run). CONSUMER_PIPELINES
below only holds each pipeline's *default* mode (matching the
EXECUTION_MODE each is deployed with); the live value is layered on top
from _STATE_FILE, a tiny local override store — there's no
DataProductDefinition/registry.py row for a consumer pipeline to live on,
so this is the smallest thing that gets the job done. Toggling it doesn't
require owning the consumer chart's Deployment spec (image, volumes,
Secrets, ...): runtime.set_container_execution_mode() patches just the
EXECUTION_MODE env var on the container that's already running (see
service/k8s_ops.py / service/podman_ops.py), the same way runtime.run_once()
already execs into it for Run Now without needing that spec either.

Container/Deployment names match both runtime backends exactly:
  - podman/docker (local demo): service/compose.yaml's container_name:
    fields
  - kubernetes: consumer/{file,topic}/extractor/charts'
    values.yaml `name:` field, which .pipelines/azure-pipelines/cd-pipelines/
    dpn-self-service-cd.yaml deploys into the same $(PRODUCER_NAMESPACE) this
    UI's own producer Deployments live in — so the existing
    charts/templates/rbac.yaml Role (scoped to producerNamespace, get/list on
    deployments + pods + pods/log) already covers reading these with no
    chart changes.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

CONSUMER_PIPELINES: list[dict[str, str]] = [
    {
        "key": "file",
        "label": "File Consumer",
        "pathway": "file",
        "extractor_container": "dpn-consumer-file-extractor",
        # Default only — see module docstring. The live value is whatever
        # _STATE_FILE last recorded via set_execution_mode(), falling back
        # to this when there's no override yet (a brand-new deployment).
        "execution_mode": "automatic",
    },
    {
        "key": "topic",
        "label": "Topic Consumer",
        "pathway": "topic",
        "extractor_container": "dpn-consumer-topic-extractor",
        "execution_mode": "automatic",
    },
]

_BY_KEY = {p["key"]: p for p in CONSUMER_PIPELINES}

_STATE_FILE = Path(__file__).parent / "data" / "consumer_execution_modes.json"
_lock = threading.Lock()


class ConsumerNotFoundError(Exception):
    pass


def _read_overrides() -> dict[str, str]:
    if not _STATE_FILE.exists():
        return {}
    with _STATE_FILE.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _write_overrides(overrides: dict[str, str]) -> None:
    _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with _STATE_FILE.open("w", encoding="utf-8") as fh:
        json.dump(overrides, fh, indent=2)


def list_pipelines() -> list[dict[str, str]]:
    """CONSUMER_PIPELINES with each entry's execution_mode overridden by
    whatever was last set via set_execution_mode(), if anything."""
    with _lock:
        overrides = _read_overrides()
    return [{**p, "execution_mode": overrides.get(p["key"], p["execution_mode"])} for p in CONSUMER_PIPELINES]


def get_pipeline(key: str) -> dict[str, str]:
    if key not in _BY_KEY:
        raise ConsumerNotFoundError(key)
    for pipeline in list_pipelines():
        if pipeline["key"] == key:
            return pipeline
    raise ConsumerNotFoundError(key)  # pragma: no cover - unreachable, _BY_KEY already checked


def set_execution_mode(key: str, execution_mode: str) -> dict[str, str]:
    if key not in _BY_KEY:
        raise ConsumerNotFoundError(key)
    with _lock:
        overrides = _read_overrides()
        overrides[key] = execution_mode
        _write_overrides(overrides)
    return get_pipeline(key)
