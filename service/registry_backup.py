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
Disaster-recovery backup/restore for the data-product registry.

service/registry.py's own docstring already flags its JSON file as a
stand-in for a Postgres table, held on a single-writer PVC (see
service/charts/values.yaml's persistence block). A PVC survives pod
restarts, but not a deleted/lost volume, a rebuilt cluster, or a fresh
namespace — every data product this deployment ever created would
otherwise be unrecoverable, even though the actual Kafka topics/storage
containers and running adaptor/schema_mapper Deployments those products
describe are untouched and still perfectly usable; only the registry's
record of them (and therefore this UI's ability to manage them) is gone.

This module closes that gap by mirroring the same JSON file to the
platform's own cloud storage account — the one already configured for
data products themselves via DPN_CLOUD_PROVIDER / platformStorageSecretName
(see service/env_builder.py) — on every write, and restoring from it once,
at startup, if the local file isn't there yet.

Design:
  * backup() is called from registry._write_all() AFTER the local write
    already succeeded — never before, so a cloud storage outage can
    never block a business user's create/pause/resume/delete action.
    Failures are logged and swallowed: this is a safety net, not a
    critical-path dependency, and the local PVC remains the source of
    truth for as long as it exists.
  * restore_if_missing() runs once at FastAPI startup (see ui/app.py),
    before any request is served, and ONLY when the local file doesn't
    exist at all — a normal pod restart with an intact PVC never touches
    cloud storage. Unlike backup(), a restore failure IS raised: an
    empty registry after a real disaster must never look identical to
    "this deployment has always been empty" (registry.list_products()
    can't tell the difference on its own), so a broken restore should
    fail this pod's startup loudly rather than silently discard every
    product's history.
  * One well-known object per deployment - container/bucket name is
    "dpconfig" (overridable via DPN_REGISTRY_BACKUP_CONTAINER for the rare
    case that default collides with something else in a
    globally-namespaced AWS/GCP bucket space — Azure containers are only
    namespaced per storage account, so a collision there would mean two
    data products sharing one Azure Storage account, already the
    platform-level setup this deployment assumes), key
    "data_products.json". The whole file is overwritten on every backup,
    mirroring registry.py's own rewrite-the-whole-file model rather than
    inventing an incremental or versioned scheme.
"""
from __future__ import annotations

import base64
import logging
import os

from service import env_builder

_BACKUP_BLOB_NAME = "data_products.json"
_DEFAULT_BACKUP_CONTAINER = "dpconfig"
_logger = logging.getLogger(__name__)


def _backup_container_name() -> str:
    return os.getenv("DPN_REGISTRY_BACKUP_CONTAINER", "").strip() or _DEFAULT_BACKUP_CONTAINER


# --------------------------------------------------------------------------
# Azure
# --------------------------------------------------------------------------
# producer-file-dp-secret's SRC_CONNECTION_STRING is base64-encoded at rest
# (same convention blueprints/producer/file/adaptor/main.py's own
# base64.b64decode() on that env var name already assumes) —
# BlobServiceClient.from_connection_string() wants the real, decoded
# connection string, not the base64 text.
def _azure_conn_str() -> str:
    return base64.b64decode(env_builder.PLATFORM_AZURE_SRC_CONNECTION_STRING).decode("utf-8")


def _azure_backup(content: bytes) -> None:
    from azure.core.exceptions import ResourceExistsError
    from azure.storage.blob import BlobServiceClient

    # SRC, not TARGET: this backs up the self-service app's own internal
    # registry.json, not product data, so it belongs in this org's own
    # storage account (SRC) rather than the shared Federator-side account
    # (TARGET) — see env_builder.PLATFORM_AZURE_SRC_CONNECTION_STRING's
    # comment for why the two differ.
    client = BlobServiceClient.from_connection_string(_azure_conn_str())
    container = client.get_container_client(_backup_container_name())
    try:
        container.create_container()
    except ResourceExistsError:
        pass
    container.upload_blob(_BACKUP_BLOB_NAME, content, overwrite=True)


def _azure_restore() -> bytes | None:
    from azure.core.exceptions import ResourceNotFoundError
    from azure.storage.blob import BlobServiceClient

    client = BlobServiceClient.from_connection_string(_azure_conn_str())
    container = client.get_container_client(_backup_container_name())
    try:
        return container.download_blob(_BACKUP_BLOB_NAME).readall()
    except ResourceNotFoundError:
        return None


# --------------------------------------------------------------------------
# AWS S3
# --------------------------------------------------------------------------
def _s3_client():
    import boto3
    from botocore.client import Config

    kwargs: dict = {
        "region_name": env_builder.AWS_REGION,
        "config": Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    }
    if env_builder.AWS_ENDPOINT_URL:
        kwargs["endpoint_url"] = env_builder.AWS_ENDPOINT_URL
    if env_builder.AWS_ACCESS_KEY_ID:
        kwargs["aws_access_key_id"] = env_builder.AWS_ACCESS_KEY_ID
    if env_builder.AWS_SECRET_ACCESS_KEY:
        kwargs["aws_secret_access_key"] = env_builder.AWS_SECRET_ACCESS_KEY
    return boto3.client("s3", **kwargs)


def _s3_backup(content: bytes) -> None:
    from botocore.exceptions import ClientError

    s3 = _s3_client()
    bucket = _backup_container_name()
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in ("404", "NoSuchBucket"):
            raise
        create_kwargs: dict = {"Bucket": bucket}
        if env_builder.AWS_REGION and env_builder.AWS_REGION != "us-east-1":
            create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": env_builder.AWS_REGION}
        try:
            s3.create_bucket(**create_kwargs)
        except ClientError as create_exc:
            if create_exc.response.get("Error", {}).get("Code", "") != "BucketAlreadyOwnedByYou":
                raise
    s3.put_object(Bucket=bucket, Key=_BACKUP_BLOB_NAME, Body=content)


def _s3_restore() -> bytes | None:
    from botocore.exceptions import ClientError

    s3 = _s3_client()
    try:
        return s3.get_object(Bucket=_backup_container_name(), Key=_BACKUP_BLOB_NAME)["Body"].read()
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "NoSuchBucket", "404"):
            return None
        raise


# --------------------------------------------------------------------------
# GCP GCS
# --------------------------------------------------------------------------
def _gcs_client():
    import json

    from google.cloud import storage as gcs_storage
    from google.oauth2 import service_account

    credentials = None
    if env_builder.PLATFORM_GCP_SERVICE_ACCOUNT_KEY:
        info = json.loads(env_builder.PLATFORM_GCP_SERVICE_ACCOUNT_KEY)
        credentials = service_account.Credentials.from_service_account_info(info)
    return gcs_storage.Client(project=env_builder.PLATFORM_GCP_PROJECT_ID or None, credentials=credentials)


def _gcs_backup(content: bytes) -> None:
    from google.api_core.exceptions import Conflict

    client = _gcs_client()
    bucket_name = _backup_container_name()
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        try:
            client.create_bucket(bucket_name, project=env_builder.PLATFORM_GCP_PROJECT_ID or None)
        except Conflict:
            pass
    bucket.blob(_BACKUP_BLOB_NAME).upload_from_string(content)


def _gcs_restore() -> bytes | None:
    client = _gcs_client()
    blob = client.bucket(_backup_container_name()).blob(_BACKUP_BLOB_NAME)
    if not blob.exists():
        return None
    return blob.download_as_bytes()


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------
_BACKENDS = {
    "azure": (_azure_backup, _azure_restore),
    "aws": (_s3_backup, _s3_restore),
    "gcp": (_gcs_backup, _gcs_restore),
}

# Azure's BlobServiceClient.from_connection_string() has no ambient-credential
# fallback the way boto3/GCS clients do (instance profile / workload identity
# can leave AWS_ACCESS_KEY_ID/GCP_SERVICE_ACCOUNT_KEY blank and still work) —
# a blank connection string always raises before ever reaching a real call.
# Gated only here, not in _BACKENDS' AWS/GCP entries, since those two can
# still plausibly succeed with no explicit credentials configured.
_CONFIGURED_CHECKS = {
    "azure": lambda: bool(env_builder.PLATFORM_AZURE_SRC_CONNECTION_STRING),
}

# None of the three SDKs above are given an explicit request timeout, and
# their default retry/connect behaviour on a genuinely unreachable endpoint
# (not just an error response — an address that never answers) can hang
# far longer than any caller here should ever wait; confirmed directly by
# hanging for 2+ minutes against a deliberately black-holed test endpoint
# while building this module. A hard wall-clock bound via a worker thread
# is the one mechanism that behaves the same way regardless of which of
# the three wildly-different SDKs is in play, so it wraps every call
# instead of trying to tune each library's own timeout knobs individually.
_BACKUP_TIMEOUT_SECONDS = 10
_RESTORE_TIMEOUT_SECONDS = 30


def _run_with_timeout(fn, timeout_seconds: float):
    # Deliberately a raw daemon Thread, NOT concurrent.futures.ThreadPoolExecutor
    # — that class's worker threads are tracked by a module-level atexit hook
    # (concurrent.futures.thread._python_exit) which unconditionally JOINS
    # every outstanding worker at interpreter shutdown, no matter what
    # shutdown(wait=...) was called with. Confirmed by hand: even with
    # shutdown(wait=False), a call against a black-holed endpoint still hung
    # the whole process past 90s at exit. A plain daemon thread has no such
    # hook — the interpreter never waits on it, so a stuck call here can
    # never delay this pod's own shutdown, only (bounded by timeout_seconds)
    # the one registry call that started it.
    import threading

    result: dict = {}

    def _target() -> None:
        try:
            result["value"] = fn()
        except BaseException as exc:  # noqa: BLE001 - re-raised on the caller's thread below
            result["error"] = exc

    thread = threading.Thread(target=_target, daemon=True)
    thread.start()
    thread.join(timeout=timeout_seconds)
    if thread.is_alive():
        raise TimeoutError(f"cloud call did not finish within {timeout_seconds}s")
    if "error" in result:
        raise result["error"]
    return result.get("value")


def _is_configured() -> bool:
    check = _CONFIGURED_CHECKS.get(env_builder.PLATFORM_CLOUD_PROVIDER)
    return check() if check is not None else True


def backup(content: bytes) -> None:
    """Best-effort mirror of the registry's full JSON content to cloud
    storage. Never raises and never blocks longer than
    _BACKUP_TIMEOUT_SECONDS — see module docstring for why a backup
    failure (or a slow one) must not block the request that triggered it."""
    if not _is_configured():
        return
    backup_fn, _ = _BACKENDS.get(env_builder.PLATFORM_CLOUD_PROVIDER, _BACKENDS["aws"])
    try:
        _run_with_timeout(lambda: backup_fn(content), _BACKUP_TIMEOUT_SECONDS)
    except Exception:
        _logger.warning(
            "Registry cloud backup failed or timed out — local PVC copy is unaffected, "
            "but a disaster right now would lose every data product recorded since the "
            "last successful backup.",
            exc_info=True,
        )


def restore_if_missing() -> bytes | None:
    """Return the last backed-up registry JSON as bytes, or None if no
    backup exists yet (a genuinely fresh deployment, or one where platform
    storage credentials for the configured cloud provider simply haven't
    been set up yet — the two are indistinguishable from here, and neither
    should block startup). Raises on any other failure, including a timeout
    past _RESTORE_TIMEOUT_SECONDS — see module docstring for why silence is
    dangerous once credentials ARE configured."""
    if not _is_configured():
        _logger.info(
            "Platform storage credentials for %r aren't configured yet — "
            "starting with an empty registry instead of attempting a restore.",
            env_builder.PLATFORM_CLOUD_PROVIDER,
        )
        return None
    _, restore_fn = _BACKENDS.get(env_builder.PLATFORM_CLOUD_PROVIDER, _BACKENDS["aws"])
    return _run_with_timeout(restore_fn, _RESTORE_TIMEOUT_SECONDS)
