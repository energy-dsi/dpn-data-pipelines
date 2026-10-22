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
Data product registry.

A flat JSON file stands in for the Postgres registry table described in
the target architecture (see service/README.md) — enough to prove
the self-service concept end-to-end without standing up a database
container. Swapping this module for a Postgres-backed one is the only
change needed to move this from prototype to production; the FastAPI
routes and podman_ops module do not need to change.

Every write is also mirrored to cloud storage (see registry_backup.py)
so a lost/rebuilt PVC — a fresh cluster, a deleted namespace — doesn't
mean losing the record of every data product this deployment ever
created; see ui/app.py's startup hook for the matching restore.
"""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path

from service import registry_backup
from service.models import DataProductDefinition

_DATA_DIR = Path(__file__).parent / "data"
_REGISTRY_FILE = _DATA_DIR / "data_products.json"
_lock = threading.Lock()
_logger = logging.getLogger(__name__)


class DuplicateProductError(Exception):
    pass


class ProductNotFoundError(Exception):
    pass


def _read_all() -> list[dict]:
    if not _REGISTRY_FILE.exists():
        return []
    with _REGISTRY_FILE.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _write_all(rows: list[dict]) -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    content = json.dumps(rows, indent=2, default=str)
    with _REGISTRY_FILE.open("w", encoding="utf-8") as fh:
        fh.write(content)
    # Local write already succeeded by this point — see registry_backup's
    # module docstring for why the cloud mirror is best-effort and comes
    # strictly after, never in place of, the local write above. This runs
    # inside every caller's `with _lock:` block, so a slow cloud call adds
    # to this request's latency (and briefly delays any other in-flight
    # registry call) — acceptable for this single-admin-at-a-time UI, but
    # worth knowing if this registry ever gets busier.
    registry_backup.backup(content.encode("utf-8"))


def list_products() -> list[DataProductDefinition]:
    with _lock:
        return [DataProductDefinition(**row) for row in _read_all()]


def get_product(product_type: str) -> DataProductDefinition:
    for product in list_products():
        if product.product_type == product_type:
            return product
    raise ProductNotFoundError(product_type)


def create_product(definition: DataProductDefinition) -> DataProductDefinition:
    with _lock:
        rows = _read_all()
        if any(row["product_type"] == definition.product_type for row in rows):
            raise DuplicateProductError(definition.product_type)
        rows.append(json.loads(definition.model_dump_json()))
        _write_all(rows)
    # AC1 (schema-agnostic provisioning): DataProductDefinition carries no
    # schema field at all — this is the one place every create path (UI
    # confirm, future API clients) funnels through, so log it here rather
    # than at each caller, confirming no predefined schema type was needed.
    _logger.info(
        "Provisioned data product '%s' (pipeline_type=%s) without a predefined schema type.",
        definition.product_type,
        definition.pipeline_type,
    )
    return definition


def set_active(product_type: str, is_active: bool) -> DataProductDefinition:
    with _lock:
        rows = _read_all()
        for row in rows:
            if row["product_type"] == product_type:
                row["is_active"] = is_active
                _write_all(rows)
                return DataProductDefinition(**row)
    raise ProductNotFoundError(product_type)


def set_execution_mode(product_type: str, execution_mode: str) -> DataProductDefinition:
    with _lock:
        rows = _read_all()
        for row in rows:
            if row["product_type"] == product_type:
                row["execution_mode"] = execution_mode
                _write_all(rows)
                return DataProductDefinition(**row)
    raise ProductNotFoundError(product_type)


def delete_product(product_type: str) -> None:
    with _lock:
        rows = _read_all()
        remaining = [row for row in rows if row["product_type"] != product_type]
        if len(remaining) == len(rows):
            raise ProductNotFoundError(product_type)
        _write_all(remaining)


def restore_from_backup_if_missing() -> None:
    """
    Disaster recovery: called once at FastAPI startup (see ui/app.py). A
    brand-new PVC (a rebuilt cluster, a deleted namespace) has no
    data_products.json at all — in that specific case, and only that
    case, pull the last cloud backup down before this pod starts serving
    requests. A normal restart with an intact PVC returns at the first
    `if` and never calls registry_backup at all.
    """
    with _lock:
        if _REGISTRY_FILE.exists():
            return
        content = registry_backup.restore_if_missing()
        if content is None:
            _logger.info("No registry backup found in cloud storage — starting with an empty registry.")
            return
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        _REGISTRY_FILE.write_bytes(content)
        _logger.warning(
            "Registry restored from cloud backup after finding no local data_products.json "
            "— this pod's PVC appears to be new/empty (disaster recovery path)."
        )
