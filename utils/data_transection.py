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
# | 1.0.0   | Initial version                                          | DSI Team      | 2026-05-01  |
# | 1.1.0   | Add delete_container() for self-service product teardown | DSI Team      | 2026-10-01  |
# | 1.2.0   | Added debug_logging opt-in (Azure logging_enable + DEBUG  | DSI Team      | 2026-10-03  |
# |         | level on the azure/boto3/botocore/google loggers) to      |               |             |
# |         | diagnose slow single-file storage operations              |               |             |
# | 1.3.0   | Added list_files_with_timestamps() — one listing call per | DSI Team      | 2026-10-03  |
# |         | side instead of a per-file get_blob_properties()/         |               |             |
# |         | head_object()/get_blob() round-trip for every file, every |               |             |
# |         | cycle, regardless of whether it needed copying             |               |             |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
DataTransection - multi-cloud storage utility.

Operations
----------
file_copy  (Adaptor, Producer Mapper)
    Copy source -> target WITHOUT deleting source.
    * Target absent  -> copy unconditionally.
    * Target present -> copy only when source is strictly newer.

file_move  (Extractor, Consumer Mapper)
    Copy source -> target THEN delete source. No timestamp check.

source_file_info
    List objects in the source container / bucket.

data_read
    Download object content as UTF-8 string.
"""

from __future__ import annotations

import base64
import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import boto3
from azure.core.exceptions import AzureError, ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from boto3 import Session  # noqa: F401
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError
from google.api_core.exceptions import Conflict, GoogleAPIError, NotFound
from google.cloud import storage as gcs_storage
from google.oauth2 import service_account

from utils.otel_logger import Logging

# Object-key charset, deliberately aligned with what the two cloud
# providers we actually write these keys to consider valid, rather than
# an arbitrary guess:
#
#   * S3 (AWS's own "Object key naming guidelines") splits key characters
#     into three tiers:
#       - "Safe" (use freely):       A-Z a-z 0-9  ! - _ . * ' ( )
#       - "Special handling" (valid, just URL-encoded on the wire — the
#         SDK already does that for us, so there's nothing extra to
#         handle here): & $ @ = ; : + , ? # and space
#       - "Avoid": backslash, { } ^ % ` [ ] " < > ~ | and non-printable
#         (0x00-0x1F, 0x7F) characters — these genuinely break S3 clients
#         or are ambiguous with URL-encoding/console rendering. '#' was
#         previously excluded here too even though every real filename
#         this module is ever handed passes it as a plain SDK parameter
#         (never assembled into a URL/query string in this repo — see
#         file_copy()/file_move() below), so the console/URL-fragment
#         ambiguity AWS warns about never actually applies here; keeping
#         it excluded only broke ordinary business filenames like
#         "Order #12, Q3 & Q4.xlsx" or "my_file #1.txt" for no benefit.
#   * Azure Blob names accept effectively any UTF-8 character (reserved
#     URL characters are escaped by the SDK, same as S3 above), with one
#     extra rule this repo does not otherwise get for free: a blob name
#     must not END in a dot or a forward slash.
#
# So: allow S3's "safe" + "special handling" tiers (covers real business
# filenames like "test sample 1.xml", "attribution (3).json" — a
# browser's own default naming for a repeat download — "Order #12, Q3 &
# Q4.xlsx", or "my_file #1.txt"), explicitly exclude everything still on
# S3's "avoid" list, and separately reject a trailing '.' or '/' for
# Azure. ".." segments are blocked explicitly below to keep path
# traversal out even though '.' itself remains allowed for ordinary file
# extensions.
_SAFE_OBJECT_KEY_RE: re.Pattern[str] = re.compile(r"^[\w !$&'()*+,.:;=@#/-]+$")


def _validate_object_key(key: str) -> str:
    if (
        not key
        or not _SAFE_OBJECT_KEY_RE.match(key)
        or ".." in key
        or key.endswith(".")
        or key.endswith("/")
    ):
        raise ValueError(
            f"Object key contains unsafe characters: {key!r}. Allowed: "
            "alphanumeric, space, and ! $ & ' ( ) * + , - . / : ; = @ # _ "
            "(no '..' segments, and it may not end in '.' or '/')."
        )
    return key


class CopyResult:
    """
    Outcome of a file_copy call.

    Attributes
    ----------
    copied  : True when bytes were written to the target.
    skipped : True when target is already up-to-date (no write needed).
    reason  : Human-readable explanation for structured log output.
    """

    __slots__ = ("copied", "skipped", "reason")

    def __init__(self, *, copied: bool, skipped: bool, reason: str) -> None:
        self.copied = copied
        self.skipped = skipped
        self.reason = reason

    def __bool__(self) -> bool:
        return self.copied or self.skipped

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CopyResult(copied={self.copied}, "
            f"skipped={self.skipped}, reason={self.reason!r})"
        )


class DataTransection:
    """
    Multi-cloud storage transaction helper.

    Existing parameter names are ALL unchanged.
    New AWS/MinIO parameters are purely additive.
    """

    def __init__(
        self,
        source_azure_conn_str: str,
        source_container_name: str,
        target_container_name: str,
        source_blob_name: Optional[str],
        target_blob_name: Optional[str],
        target_azure_conn_str: str,
        aws_endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_region: str = "us-east-1",
        aws_connect_timeout: Optional[float] = None,
        aws_read_timeout: Optional[float] = None,
        gcp_service_account_info: Optional[dict] = None,
        gcp_project_id: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        component_name: Optional[str] = None,
        debug_logging: bool = False,
    ) -> None:
        self.component_name: Optional[str] = component_name
        # Opt-in, off by default: turns on the cloud SDKs' own verbose
        # HTTP-level request/response logging. Exists to diagnose cases
        # where a trivial single-blob list/copy takes tens of seconds —
        # confirmed on a live consumer-file-extractor run (list_files ~30s,
        # process_file ~45s on a 1-file container) — which is the signature
        # of the SDK's default retry policy quietly backing off and retrying
        # a transient failure (DNS, network blip, throttling) several times
        # before it succeeds, rather than any application-level slowness.
        # Caller (main.py) wires this to an env var so it can be flipped on
        # for one diagnostic run without a code change.
        self._debug_logging: bool = debug_logging
        if debug_logging:
            for name in ("azure", "boto3", "botocore", "google"):
                logging.getLogger(name).setLevel(logging.DEBUG)
        self.source_azure_conn_str: str = source_azure_conn_str
        self.source_container_name: str = source_container_name
        self.target_container_name: str = target_container_name
        self.source_blob_name: Optional[str] = source_blob_name
        self.target_blob_name: Optional[str] = target_blob_name
        self.target_azure_conn_str: str = target_azure_conn_str
        self.aws_endpoint_url: Optional[str] = aws_endpoint_url
        self.aws_access_key_id: Optional[str] = aws_access_key_id
        self.aws_secret_access_key: Optional[str] = aws_secret_access_key
        self.aws_region: str = aws_region
        # None (the default) leaves botocore's own defaults (60s/60s) in
        # place for every real pipeline run — only service/precheck.py's
        # short-lived existence checks pass these, to keep a self-service
        # preview request from hanging on an unreachable/misconfigured
        # endpoint until something upstream (e.g. oauth2-proxy) times out.
        self.aws_connect_timeout: Optional[float] = aws_connect_timeout
        self.aws_read_timeout: Optional[float] = aws_read_timeout
        # Parsed service-account JSON (not the raw string) — see
        # gcp_credentials() below, which decodes/parses the base64'd
        # GCP_SERVICE_ACCOUNT_KEY env var once at main.py startup.
        self.gcp_service_account_info: Optional[dict] = gcp_service_account_info
        self.gcp_project_id: Optional[str] = gcp_project_id
        self._logger: logging.Logger = logger or Logging().create_logger()
        self._azure_src_client: Optional[BlobServiceClient] = None
        self._azure_tgt_client: Optional[BlobServiceClient] = None
        self._s3_client = None
        self._gcs_client: Optional["gcs_storage.Client"] = None

    def _get_azure_src_client(self) -> BlobServiceClient:
        if self._azure_src_client is None:
            self._azure_src_client = BlobServiceClient.from_connection_string(
                self.source_azure_conn_str,
                # logging_enable alone (not just the "azure" logger's level)
                # is what makes azure-core's HTTP logging policy actually
                # emit per-request method/URL/headers and per-response
                # status/elapsed-time at DEBUG — off by default since it can
                # include request IDs/headers best kept out of normal logs.
                logging_enable=self._debug_logging,
            )
        return self._azure_src_client

    def _get_azure_tgt_client(self) -> BlobServiceClient:
        if self._azure_tgt_client is None:
            self._azure_tgt_client = BlobServiceClient.from_connection_string(
                self.target_azure_conn_str,
                logging_enable=self._debug_logging,
            )
        return self._azure_tgt_client

    def _get_s3_client(self):
        if self._s3_client is None:
            client_kwargs: dict = {
                "region_name": self.aws_region,
                "config": Config(
                    signature_version="s3v4",
                    s3={"addressing_style": "path"},
                    connect_timeout=self.aws_connect_timeout,
                    read_timeout=self.aws_read_timeout,
                ),
            }
            if self.aws_endpoint_url:
                client_kwargs["endpoint_url"] = self.aws_endpoint_url
            if self.aws_access_key_id:
                client_kwargs["aws_access_key_id"] = self.aws_access_key_id
            if self.aws_secret_access_key:
                client_kwargs["aws_secret_access_key"] = self.aws_secret_access_key
            self._s3_client = boto3.client("s3", **client_kwargs)
        return self._s3_client

    def _get_gcs_client(self) -> "gcs_storage.Client":
        if self._gcs_client is None:
            credentials = None
            if self.gcp_service_account_info:
                credentials = service_account.Credentials.from_service_account_info(
                    self.gcp_service_account_info
                )
            self._gcs_client = gcs_storage.Client(
                project=self.gcp_project_id, credentials=credentials
            )
        return self._gcs_client

    # -----------------------------------------------------------------------
    # Public interface
    # -----------------------------------------------------------------------

    def ensure_container_exists(
        self, cloud_provider: str, container_name: str, side: str = "source"
    ) -> bool:
        """
        Idempotently ensure a storage container/bucket exists, creating it
        if absent — the storage-side counterpart to
        utils.topic_utils.KafkaTopicManager.ensure_exists() for Kafka
        topics. Lets a data product be provisioned with a brand-new
        container/bucket name (FR-D-016m-10) instead of requiring one to
        already exist.

        side selects which Azure connection string identifies the storage
        account to create in ("source" or "target") — meaningless for
        AWS/GCP, where one set of credentials owns every bucket.

        Returns True if the container already existed or was created,
        False if the provider is unsupported (logged, not raised — the
        caller's later read/write call surfaces the real error).
        """
        provider = (cloud_provider or "").lower().strip()
        if provider == "azure":
            return self._azure_ensure_container(container_name, side)
        if provider == "aws":
            return self._s3_ensure_bucket(container_name)
        if provider == "gcp":
            return self._gcs_ensure_bucket(container_name)
        self._logger.warning(
            "Unsupported cloud provider for container creation",
            extra={"cloud_provider": cloud_provider, "component.name": self.component_name},
        )
        return False

    def container_exists(
        self, cloud_provider: str, container_name: str, side: str = "source"
    ) -> bool:
        """
        Pure existence check — unlike ensure_container_exists(), never
        creates anything. Used by the self-service precheck
        (service/precheck.py) to block product creation on a name
        collision before anything is actually provisioned.
        """
        provider = (cloud_provider or "").lower().strip()
        if provider == "azure":
            return self._azure_container_exists(container_name, side)
        if provider == "aws":
            return self._s3_bucket_exists(container_name)
        if provider == "gcp":
            return self._gcs_bucket_exists(container_name)
        self._logger.warning(
            "Unsupported cloud provider for container existence check",
            extra={"cloud_provider": cloud_provider, "component.name": self.component_name},
        )
        return False

    def delete_container(
        self, cloud_provider: str, container_name: str, side: str = "source"
    ) -> bool:
        """
        Idempotently delete a storage container/bucket and everything in
        it — the teardown counterpart to ensure_container_exists(), used
        when a self-service data product is permanently deleted (see
        ui/app.py's delete_data_product). No-op (logged) if the
        container/bucket is already absent.

        side selects which Azure connection string identifies the storage
        account to delete from ("source" or "target") — meaningless for
        AWS/GCP, where one set of credentials owns every bucket.

        Returns True if the container/bucket was deleted or already
        absent, False if the provider is unsupported (logged, not raised).
        """
        provider = (cloud_provider or "").lower().strip()
        if provider == "azure":
            self._azure_delete_container(container_name, side)
            return True
        if provider == "aws":
            self._s3_delete_bucket(container_name)
            return True
        if provider == "gcp":
            self._gcs_delete_bucket(container_name)
            return True
        self._logger.warning(
            "Unsupported cloud provider for container deletion",
            extra={"cloud_provider": cloud_provider, "component.name": self.component_name},
        )
        return False

    def source_file_info(self, cloud_provider: str) -> list[str]:
        """List objects in the source container / bucket."""
        provider = cloud_provider.lower().strip()
        if provider == "azure":
            return self._azure_list_files()
        if provider == "aws":
            return self._s3_list_files()
        if provider == "gcp":
            return self._gcs_list_files()
        self._logger.warning(
            "Unsupported cloud provider for list operation",
            extra={"cloud_provider": cloud_provider, "component.name": self.component_name},
        )
        return []

    def list_files_with_timestamps(
        self, cloud_provider: str, side: str = "source"
    ) -> dict[str, Optional[datetime]]:
        """
        Return {name: last_modified} for every object in the source or
        target container, in ONE listing call per provider.

        Every provider's own listing response (Azure's list_blobs(), S3's
        list_objects_v2, GCS's list_blobs()) already carries each object's
        last-modified timestamp — callers deciding what to copy used to
        throw that away (source_file_info() keeps only names) and then
        re-fetch it per file via a separate get_blob_properties()/
        head_object()/get_blob() call for BOTH source and target, costing
        2 extra round-trips per file regardless of whether that file turned
        out to need copying at all. Confirmed live against a container
        holding just 1-4 files: each of those per-file lookups took
        25-68 seconds (see blueprints/consumer/file/extractor/main.py's
        v2.2.0 changelog and DEBUG_STORAGE_SDK), so a cycle with N files
        paid that cost N times for files that never change. Calling this
        once for each side instead turns an O(N) round-trip count into O(1),
        and lets the caller decide what actually needs copying before
        touching the network again for the copy itself.
        """
        provider = cloud_provider.lower().strip()
        if provider == "azure":
            return self._azure_list_files_with_timestamps(side)
        if provider == "aws":
            return self._s3_list_files_with_timestamps(side)
        if provider == "gcp":
            return self._gcs_list_files_with_timestamps(side)
        self._logger.warning(
            "Unsupported cloud provider for list operation",
            extra={"cloud_provider": cloud_provider, "component.name": self.component_name},
        )
        return {}

    def file_copy(
        self,
        cloud_vendor: str,
        file_name: str,
        dest_file_name: Optional[str] = None,
    ) -> CopyResult:
        """
        Copy source -> target WITHOUT deleting the source.

        Used by Adaptor and Producer Mapper.

        Decision logic
        --------------
        1. Source not found           -> CopyResult(copied=False, skipped=False)
        2. Target absent              -> copy; CopyResult(copied=True)
        3. Source newer than target   -> overwrite; CopyResult(copied=True)
        4. Source same age or older   -> skip; CopyResult(skipped=True)
        """
        _validate_object_key(file_name)
        effective_dest: str = dest_file_name or file_name
        if dest_file_name:
            _validate_object_key(dest_file_name)

        vendor = cloud_vendor.lower().strip()
        if vendor == "azure":
            return self._azure_copy_file(file_name, effective_dest)
        if vendor == "aws":
            return self._s3_copy_file(file_name, effective_dest)
        if vendor == "gcp":
            return self._gcs_copy_file(file_name, effective_dest)

        self._logger.warning(
            "Unsupported cloud vendor for copy operation",
            extra={"cloud_vendor": cloud_vendor, "component.name": self.component_name},
        )
        return CopyResult(copied=False, skipped=False, reason="unsupported vendor")

    def file_move(
        self,
        cloud_vendor: str,
        file_name: str,
        dest_file_name: Optional[str] = None,
    ) -> bool:
        """
        Copy source -> target THEN delete source.

        Used by Extractor and Consumer Mapper.
        No timestamp comparison – the file is consumed exactly once.
        """
        _validate_object_key(file_name)
        effective_dest: str = dest_file_name or file_name
        if dest_file_name:
            _validate_object_key(dest_file_name)

        vendor = cloud_vendor.lower().strip()
        if vendor == "azure":
            return self._azure_move_file(file_name, effective_dest)
        if vendor == "aws":
            return self._s3_move_file(file_name, effective_dest)
        if vendor == "gcp":
            return self._gcs_move_file(file_name, effective_dest)

        self._logger.warning(
            "Unsupported cloud vendor for move operation",
            extra={"cloud_vendor": cloud_vendor, "component.name": self.component_name},
        )
        return False

    def data_read(self, cloud_vendor: str) -> str:
        """Download source_blob_name and return contents as UTF-8."""
        if not self.source_blob_name:
            self._logger.error(
                "data_read called with no source_blob_name set",
                extra={"component.name": self.component_name},
            )
            return ""
        _validate_object_key(self.source_blob_name)
        vendor = cloud_vendor.lower().strip()
        if vendor == "azure":
            return self._azure_read_file(self.source_blob_name)
        if vendor == "aws":
            return self._s3_read_file(self.source_blob_name)
        if vendor == "gcp":
            return self._gcs_read_file(self.source_blob_name)
        self._logger.warning(
            "Unsupported cloud vendor for read operation",
            extra={"cloud_vendor": cloud_vendor, "component.name": self.component_name},
        )
        return ""

    # -----------------------------------------------------------------------
    # Azure helpers
    # -----------------------------------------------------------------------

    def _azure_list_files(self) -> list[str]:
        try:
            container = self._get_azure_src_client().get_container_client(
                self.source_container_name
            )
            return [b.name for b in container.list_blobs()]
        except AzureError as exc:
            self._logger.error(
                "Azure list blobs failed",
                extra={
                    "container": self.source_container_name,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _azure_list_files_with_timestamps(self, side: str) -> dict[str, Optional[datetime]]:
        """list_blobs()'s own BlobProperties already carries last_modified —
        no separate get_blob_properties() call needed per blob."""
        client = self._get_azure_tgt_client() if side == "target" else self._get_azure_src_client()
        container_name = self.target_container_name if side == "target" else self.source_container_name
        try:
            container = client.get_container_client(container_name)
            result: dict[str, Optional[datetime]] = {}
            for b in container.list_blobs():
                result[b.name] = b.last_modified.astimezone(timezone.utc) if b.last_modified else None
            return result
        except AzureError as exc:
            self._logger.error(
                "Azure list blobs (with timestamps) failed",
                extra={
                    "container": container_name,
                    "side": side,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _azure_get_last_modified(
        self,
        client: BlobServiceClient,
        container: str,
        blob_name: str,
    ) -> Optional[datetime]:
        """
        Return UTC last-modified for blob_name, or None if absent.
        Single get_blob_properties() call - no extra exists() round-trip.
        """
        try:
            props = client.get_blob_client(
                container=container, blob=blob_name
            ).get_blob_properties()
            lm = props.get("last_modified")
            return lm.astimezone(timezone.utc) if lm else None
        except AzureError:
            return None  # 404 or any transient error -> treat as absent

    def _azure_copy_file(self, src: str, dst: str) -> CopyResult:
        """Copy src->dst (Azure) without deleting source. Timestamp-gated."""
        src_client = self._get_azure_src_client()
        tgt_client = self._get_azure_tgt_client()
        try:
            # 1. Source timestamp
            src_modified = self._azure_get_last_modified(
                src_client, self.source_container_name, src
            )
            if src_modified is None:
                self._logger.error(
                    "Azure copy aborted – source blob not found",
                    extra={
                        "container": self.source_container_name,
                        "file": src,
                        "component.name": self.component_name,
                    },
                )
                return CopyResult(
                    copied=False, skipped=False, reason="source blob not found"
                )

            # 2. Target timestamp (None = does not exist)
            tgt_modified = self._azure_get_last_modified(
                tgt_client, self.target_container_name, dst
            )

            # 3. Skip if target is already up-to-date
            if tgt_modified is not None and src_modified <= tgt_modified:
                self._logger.info(
                    "Azure copy skipped – target is up-to-date",
                    extra={
                        "file": src,
                        "src_modified": src_modified.isoformat(),
                        "tgt_modified": tgt_modified.isoformat(),
                        "component.name": self.component_name,
                    },
                )
                return CopyResult(
                    copied=False,
                    skipped=True,
                    reason=(
                        f"target up-to-date "
                        f"(src={src_modified.isoformat()}, "
                        f"tgt={tgt_modified.isoformat()})"
                    ),
                )

            # 4. Perform the copy
            src_blob = src_client.get_blob_client(
                container=self.source_container_name, blob=src
            )
            tgt_blob = tgt_client.get_blob_client(
                container=self.target_container_name, blob=dst
            )
            tgt_blob.start_copy_from_url(src_blob.url)

            action = "overwritten" if tgt_modified else "created"
            self._logger.info(
                f"Azure blob copied ({action})",
                extra={
                    "source_container": self.source_container_name,
                    "target_container": self.target_container_name,
                    "source_file": src,
                    "dest_file": dst,
                    "src_modified": src_modified.isoformat(),
                    "component.name": self.component_name,
                },
            )
            return CopyResult(copied=True, skipped=False, reason=action)

        except AzureError as exc:
            self._logger.error(
                "Azure blob copy failed",
                extra={
                    "source_file": src,
                    "dest_file": dst,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _azure_move_file(self, src: str, dst: str) -> bool:
        """Copy src->dst (Azure) then delete source blob."""
        try:
            src_blob = self._get_azure_src_client().get_blob_client(
                container=self.source_container_name, blob=src
            )
            tgt_blob = self._get_azure_tgt_client().get_blob_client(
                container=self.target_container_name, blob=dst
            )
            tgt_blob.start_copy_from_url(src_blob.url)
            src_blob.delete_blob()
            self._logger.info(
                "Azure blob moved",
                extra={
                    "source_container": self.source_container_name,
                    "target_container": self.target_container_name,
                    "source_file": src,
                    "dest_file": dst,
                    "component.name": self.component_name,
                },
            )
            return True
        except AzureError as exc:
            self._logger.error(
                "Azure blob move failed",
                extra={
                    "source_file": src,
                    "dest_file": dst,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _azure_read_file(self, blob_name: str) -> str:
        try:
            blob = self._get_azure_src_client().get_blob_client(
                container=self.source_container_name, blob=blob_name
            )
            return blob.download_blob().readall()
        except AzureError as exc:
            self._logger.error(
                "Azure blob read failed",
                extra={
                    "blob": blob_name,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _azure_ensure_container(self, container_name: str, side: str) -> bool:
        client = self._get_azure_tgt_client() if side == "target" else self._get_azure_src_client()
        try:
            client.create_container(container_name)
            self._logger.info(
                "Azure container created",
                extra={"container": container_name, "component.name": self.component_name},
            )
        except ResourceExistsError:
            pass  # already there — the common, expected case after the first product
        except AzureError as exc:
            self._logger.error(
                "Azure container creation failed",
                extra={
                    "container": container_name,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise
        return True

    def _azure_container_exists(self, container_name: str, side: str) -> bool:
        client = self._get_azure_tgt_client() if side == "target" else self._get_azure_src_client()
        try:
            return client.get_container_client(container_name).exists()
        except AzureError as exc:
            self._logger.error(
                "Azure container existence check failed",
                extra={
                    "container": container_name,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _azure_delete_container(self, container_name: str, side: str) -> None:
        client = self._get_azure_tgt_client() if side == "target" else self._get_azure_src_client()
        try:
            # Azure deletes a container and every blob inside it in one call
            # — no separate blob-purge pass needed, unlike S3/GCS below.
            client.delete_container(container_name)
            self._logger.info(
                "Azure container deleted",
                extra={"container": container_name, "component.name": self.component_name},
            )
        except ResourceNotFoundError:
            pass  # already gone — the common, expected case on a retried delete
        except AzureError as exc:
            self._logger.error(
                "Azure container deletion failed",
                extra={
                    "container": container_name,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    # -----------------------------------------------------------------------
    # S3 / MinIO helpers
    # -----------------------------------------------------------------------

    def _s3_list_files(self) -> list[str]:
        try:
            s3 = self._get_s3_client()
            paginator = s3.get_paginator("list_objects_v2")
            return [
                obj["Key"]
                for page in paginator.paginate(Bucket=self.source_container_name)
                for obj in page.get("Contents", [])
            ]
        except (BotoCoreError, ClientError) as exc:
            self._logger.error(
                "S3/MinIO list objects failed",
                extra={
                    "bucket": self.source_container_name,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _s3_list_files_with_timestamps(self, side: str) -> dict[str, Optional[datetime]]:
        """list_objects_v2's own response already carries LastModified per
        object — no separate head_object() call needed per key."""
        bucket = self.target_container_name if side == "target" else self.source_container_name
        try:
            s3 = self._get_s3_client()
            paginator = s3.get_paginator("list_objects_v2")
            result: dict[str, Optional[datetime]] = {}
            for page in paginator.paginate(Bucket=bucket):
                for obj in page.get("Contents", []):
                    lm = obj.get("LastModified")
                    result[obj["Key"]] = lm.astimezone(timezone.utc) if lm else None
            return result
        except (BotoCoreError, ClientError) as exc:
            self._logger.error(
                "S3/MinIO list objects (with timestamps) failed",
                extra={
                    "bucket": bucket,
                    "side": side,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _s3_get_last_modified(
        self, bucket: str, key: str
    ) -> Optional[datetime]:
        """
        Return UTC last-modified for key in bucket, or None if absent.
        Uses head_object - lightweight, no data transfer.
        """
        try:
            response = self._get_s3_client().head_object(Bucket=bucket, Key=key)
            return response["LastModified"].astimezone(timezone.utc)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return None
            raise

    def _s3_copy_file(self, src: str, dst: str) -> CopyResult:
        """Copy src->dst (S3/MinIO) without deleting source. Timestamp-gated."""
        s3 = self._get_s3_client()
        try:
            # 1. Source timestamp
            src_modified = self._s3_get_last_modified(
                self.source_container_name, src
            )
            if src_modified is None:
                self._logger.error(
                    "S3/MinIO copy aborted – source object not found",
                    extra={
                        "bucket": self.source_container_name,
                        "key": src,
                        "component.name": self.component_name,
                    },
                )
                return CopyResult(
                    copied=False, skipped=False, reason="source object not found"
                )

            # 2. Target timestamp (None = does not exist)
            tgt_modified = self._s3_get_last_modified(
                self.target_container_name, dst
            )

            # 3. Skip if target is already up-to-date
            if tgt_modified is not None and src_modified <= tgt_modified:
                self._logger.info(
                    "S3/MinIO copy skipped – target is up-to-date",
                    extra={
                        "key": src,
                        "src_modified": src_modified.isoformat(),
                        "tgt_modified": tgt_modified.isoformat(),
                        "component.name": self.component_name,
                    },
                )
                return CopyResult(
                    copied=False,
                    skipped=True,
                    reason=(
                        f"target up-to-date "
                        f"(src={src_modified.isoformat()}, "
                        f"tgt={tgt_modified.isoformat()})"
                    ),
                )

            # 4. Perform the copy
            s3.copy_object(
                CopySource={"Bucket": self.source_container_name, "Key": src},
                Bucket=self.target_container_name,
                Key=dst,
            )
            action = "overwritten" if tgt_modified else "created"
            self._logger.info(
                f"S3/MinIO object copied ({action})",
                extra={
                    "source_bucket": self.source_container_name,
                    "target_bucket": self.target_container_name,
                    "source_key": src,
                    "dest_key": dst,
                    "src_modified": src_modified.isoformat(),
                    "component.name": self.component_name,
                },
            )
            return CopyResult(copied=True, skipped=False, reason=action)

        except (BotoCoreError, ClientError) as exc:
            self._logger.error(
                "S3/MinIO object copy failed",
                extra={
                    "source_key": src,
                    "dest_key": dst,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _s3_move_file(self, src: str, dst: str) -> bool:
        """Copy src->dst (S3/MinIO) then delete source object."""
        try:
            s3 = self._get_s3_client()
            s3.copy_object(
                CopySource={"Bucket": self.source_container_name, "Key": src},
                Bucket=self.target_container_name,
                Key=dst,
            )
            s3.delete_object(Bucket=self.source_container_name, Key=src)
            self._logger.info(
                "S3/MinIO object moved",
                extra={
                    "source_bucket": self.source_container_name,
                    "target_bucket": self.target_container_name,
                    "source_key": src,
                    "dest_key": dst,
                    "component.name": self.component_name,
                },
            )
            return True
        except (BotoCoreError, ClientError) as exc:
            self._logger.error(
                "S3/MinIO object move failed",
                extra={
                    "source_key": src,
                    "dest_key": dst,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _s3_read_file(self, key: str) -> str:
        try:
            response = self._get_s3_client().get_object(
                Bucket=self.source_container_name, Key=key
            )
            return response["Body"].read()
        except (BotoCoreError, ClientError) as exc:
            self._logger.error(
                "S3/MinIO object read failed",
                extra={
                    "key": key,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _s3_ensure_bucket(self, bucket_name: str) -> bool:
        s3 = self._get_s3_client()
        try:
            s3.head_bucket(Bucket=bucket_name)
            return True  # already exists and we can see it — nothing to do
        except ClientError as exc:
            status = exc.response.get("Error", {}).get("Code", "")
            if status not in ("404", "NoSuchBucket"):
                # Anything other than "doesn't exist" (e.g. 403 on a bucket
                # someone else owns) is a real problem, not ours to paper over.
                self._logger.error(
                    "S3/MinIO bucket check failed",
                    extra={
                        "bucket": bucket_name,
                        "error": str(exc),
                        "component.name": self.component_name,
                    },
                )
                raise

        try:
            create_kwargs: dict = {"Bucket": bucket_name}
            if self.aws_region and self.aws_region != "us-east-1":
                create_kwargs["CreateBucketConfiguration"] = {
                    "LocationConstraint": self.aws_region
                }
            s3.create_bucket(**create_kwargs)
            self._logger.info(
                "S3/MinIO bucket created",
                extra={"bucket": bucket_name, "component.name": self.component_name},
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "BucketAlreadyOwnedByYou":
                pass  # created concurrently by another stage — fine
            else:
                self._logger.error(
                    "S3/MinIO bucket creation failed",
                    extra={
                        "bucket": bucket_name,
                        "error": str(exc),
                        "component.name": self.component_name,
                    },
                )
                raise
        return True

    def _s3_bucket_exists(self, bucket_name: str) -> bool:
        s3 = self._get_s3_client()
        try:
            s3.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as exc:
            status = exc.response.get("Error", {}).get("Code", "")
            if status in ("404", "NoSuchBucket"):
                return False
            self._logger.error(
                "S3/MinIO bucket existence check failed",
                extra={
                    "bucket": bucket_name,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _s3_delete_bucket(self, bucket_name: str) -> None:
        s3 = self._get_s3_client()
        try:
            # Unlike Azure, S3 refuses to delete a non-empty bucket — every
            # object has to be purged first.
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name):
                objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
                if objects:
                    s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
            s3.delete_bucket(Bucket=bucket_name)
            self._logger.info(
                "S3/MinIO bucket deleted",
                extra={"bucket": bucket_name, "component.name": self.component_name},
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchBucket"):
                pass  # already gone — the common, expected case on a retried delete
            else:
                self._logger.error(
                    "S3/MinIO bucket deletion failed",
                    extra={
                        "bucket": bucket_name,
                        "error": str(exc),
                        "component.name": self.component_name,
                    },
                )
                raise

    # -----------------------------------------------------------------------
    # GCP / GCS helpers
    # -----------------------------------------------------------------------

    def _gcs_list_files(self) -> list[str]:
        try:
            bucket = self._get_gcs_client().bucket(self.source_container_name)
            return [blob.name for blob in bucket.list_blobs()]
        except GoogleAPIError as exc:
            self._logger.error(
                "GCS list objects failed",
                extra={
                    "bucket": self.source_container_name,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _gcs_list_files_with_timestamps(self, side: str) -> dict[str, Optional[datetime]]:
        """list_blobs()'s own Blob objects already carry .updated — no
        separate get_blob() call needed per object."""
        bucket_name = self.target_container_name if side == "target" else self.source_container_name
        try:
            bucket = self._get_gcs_client().bucket(bucket_name)
            result: dict[str, Optional[datetime]] = {}
            for blob in bucket.list_blobs():
                result[blob.name] = blob.updated.astimezone(timezone.utc) if blob.updated else None
            return result
        except GoogleAPIError as exc:
            self._logger.error(
                "GCS list objects (with timestamps) failed",
                extra={
                    "bucket": bucket_name,
                    "side": side,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _gcs_get_last_modified(self, bucket_name: str, blob_name: str) -> Optional[datetime]:
        """Return UTC last-modified for blob_name in bucket_name, or None if absent."""
        blob = self._get_gcs_client().bucket(bucket_name).get_blob(blob_name)
        if blob is None or blob.updated is None:
            return None
        return blob.updated.astimezone(timezone.utc)

    def _gcs_copy_file(self, src: str, dst: str) -> CopyResult:
        """Copy src->dst (GCS) without deleting source. Timestamp-gated."""
        client = self._get_gcs_client()
        try:
            src_modified = self._gcs_get_last_modified(self.source_container_name, src)
            if src_modified is None:
                self._logger.error(
                    "GCS copy aborted – source object not found",
                    extra={
                        "bucket": self.source_container_name,
                        "object": src,
                        "component.name": self.component_name,
                    },
                )
                return CopyResult(
                    copied=False, skipped=False, reason="source object not found"
                )

            tgt_modified = self._gcs_get_last_modified(self.target_container_name, dst)

            if tgt_modified is not None and src_modified <= tgt_modified:
                self._logger.info(
                    "GCS copy skipped – target is up-to-date",
                    extra={
                        "file": src,
                        "src_modified": src_modified.isoformat(),
                        "tgt_modified": tgt_modified.isoformat(),
                        "component.name": self.component_name,
                    },
                )
                return CopyResult(
                    copied=False,
                    skipped=True,
                    reason=(
                        f"target up-to-date "
                        f"(src={src_modified.isoformat()}, "
                        f"tgt={tgt_modified.isoformat()})"
                    ),
                )

            src_bucket = client.bucket(self.source_container_name)
            tgt_bucket = client.bucket(self.target_container_name)
            src_blob = src_bucket.blob(src)
            src_bucket.copy_blob(src_blob, tgt_bucket, new_name=dst)

            action = "overwritten" if tgt_modified else "created"
            self._logger.info(
                f"GCS object copied ({action})",
                extra={
                    "source_bucket": self.source_container_name,
                    "target_bucket": self.target_container_name,
                    "source_object": src,
                    "dest_object": dst,
                    "src_modified": src_modified.isoformat(),
                    "component.name": self.component_name,
                },
            )
            return CopyResult(copied=True, skipped=False, reason=action)

        except GoogleAPIError as exc:
            self._logger.error(
                "GCS object copy failed",
                extra={
                    "source_object": src,
                    "dest_object": dst,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _gcs_move_file(self, src: str, dst: str) -> bool:
        """Copy src->dst (GCS) then delete source object."""
        try:
            client = self._get_gcs_client()
            src_bucket = client.bucket(self.source_container_name)
            tgt_bucket = client.bucket(self.target_container_name)
            src_blob = src_bucket.blob(src)
            src_bucket.copy_blob(src_blob, tgt_bucket, new_name=dst)
            src_bucket.delete_blob(src)
            self._logger.info(
                "GCS object moved",
                extra={
                    "source_bucket": self.source_container_name,
                    "target_bucket": self.target_container_name,
                    "source_object": src,
                    "dest_object": dst,
                    "component.name": self.component_name,
                },
            )
            return True
        except GoogleAPIError as exc:
            self._logger.error(
                "GCS object move failed",
                extra={
                    "source_object": src,
                    "dest_object": dst,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _gcs_read_file(self, blob_name: str) -> str:
        try:
            blob = self._get_gcs_client().bucket(self.source_container_name).blob(blob_name)
            return blob.download_as_bytes()
        except GoogleAPIError as exc:
            self._logger.error(
                "GCS object read failed",
                extra={
                    "object": blob_name,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise

    def _gcs_ensure_bucket(self, bucket_name: str) -> bool:
        client = self._get_gcs_client()
        try:
            client.create_bucket(bucket_name)
            self._logger.info(
                "GCS bucket created",
                extra={"bucket": bucket_name, "component.name": self.component_name},
            )
        except Conflict:
            pass  # already exists — the common, expected case after the first product
        except GoogleAPIError as exc:
            self._logger.error(
                "GCS bucket creation failed",
                extra={
                    "bucket": bucket_name,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise
        return True

    def _gcs_bucket_exists(self, bucket_name: str) -> bool:
        try:
            return self._get_gcs_client().bucket(bucket_name).exists()
        except GoogleAPIError as exc:
            self._logger.error(
                "GCS bucket existence check failed",
                extra={
                    "bucket": bucket_name,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise


    def _gcs_delete_bucket(self, bucket_name: str) -> None:
        try:
            # force=True purges the bucket's blobs first — GCS, like S3,
            # refuses to delete a non-empty bucket otherwise.
            self._get_gcs_client().bucket(bucket_name).delete(force=True)
            self._logger.info(
                "GCS bucket deleted",
                extra={"bucket": bucket_name, "component.name": self.component_name},
            )
        except NotFound:
            pass  # already gone — the common, expected case on a retried delete
        except GoogleAPIError as exc:
            self._logger.error(
                "GCS bucket deletion failed",
                extra={
                    "bucket": bucket_name,
                    "error": str(exc),
                    "component.name": self.component_name,
                },
            )
            raise


def gcp_credentials_from_env(b64_service_account_key: str) -> Optional[dict]:
    """
    Decode+parse a base64'd GCP service-account key JSON (the
    GCP_SERVICE_ACCOUNT_KEY env var main.py reads), the same way the
    Azure/AWS credential fields are already base64-encoded end to end.
    Returns None for an unset/empty value — main.py's validate_cloud_config()
    is what enforces this is actually present when cloudProviderType=gcp.
    """
    if not b64_service_account_key:
        return None
    return json.loads(base64.b64decode(b64_service_account_key).decode("utf-8"))
