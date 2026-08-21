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

import logging
import re
from datetime import datetime, timezone
from typing import Optional

import boto3
from azure.core.exceptions import AzureError
from azure.storage.blob import BlobServiceClient
from boto3 import Session  # noqa: F401
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

from utils.otel_logger import Logging

_SAFE_OBJECT_KEY_RE: re.Pattern[str] = re.compile(r"^[\w.\-/]+$")


def _validate_object_key(key: str) -> str:
    if not key or not _SAFE_OBJECT_KEY_RE.match(key):
        raise ValueError(
            f"Object key contains unsafe characters: {key!r}. "
            "Only alphanumeric, '.', '-', '_', and '/' are allowed."
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
        logger: Optional[logging.Logger] = None,
        component_name: Optional[str] = None,
    ) -> None:
        self.component_name: Optional[str] = component_name
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
        self._logger: logging.Logger = logger or Logging().create_logger()
        self._azure_src_client: Optional[BlobServiceClient] = None
        self._azure_tgt_client: Optional[BlobServiceClient] = None
        self._s3_client = None

    def _get_azure_src_client(self) -> BlobServiceClient:
        if self._azure_src_client is None:
            self._azure_src_client = BlobServiceClient.from_connection_string(
                self.source_azure_conn_str
            )
        return self._azure_src_client

    def _get_azure_tgt_client(self) -> BlobServiceClient:
        if self._azure_tgt_client is None:
            self._azure_tgt_client = BlobServiceClient.from_connection_string(
                self.target_azure_conn_str
            )
        return self._azure_tgt_client

    def _get_s3_client(self):
        if self._s3_client is None:
            client_kwargs: dict = {
                "region_name": self.aws_region,
                "config": Config(
                    signature_version="s3v4",
                    s3={"addressing_style": "path"},
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

    # -----------------------------------------------------------------------
    # Public interface
    # -----------------------------------------------------------------------

    def source_file_info(self, cloud_provider: str) -> list[str]:
        """List objects in the source container / bucket."""
        provider = cloud_provider.lower().strip()
        if provider == "azure":
            return self._azure_list_files()
        if provider == "aws":
            return self._s3_list_files()
        self._logger.warning(
            "Unsupported cloud provider for list operation",
            extra={"cloud_provider": cloud_provider, "component.name": self.component_name},
        )
        return []

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
