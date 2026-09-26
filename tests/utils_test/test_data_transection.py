import importlib.util
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest


SOURCE_PATH = Path("utils/data_transection.py")


class FakeAzureError(Exception):
    pass


class FakeBotoCoreError(Exception):
    pass


class FakeClientError(Exception):
    def __init__(self, code="500", message="client error"):
        super().__init__(message)
        self.response = {"Error": {"Code": code}}


class FakeLogger:
    def __init__(self):
        self.info_calls = []
        self.warning_calls = []
        self.error_calls = []

    def info(self, message, *args, extra=None, **kwargs):
        self.info_calls.append((message, args, extra, kwargs))

    def warning(self, message, *args, extra=None, **kwargs):
        self.warning_calls.append((message, args, extra, kwargs))

    def error(self, message, *args, extra=None, **kwargs):
        self.error_calls.append((message, args, extra, kwargs))


class FakeBlobProperties:
    def __init__(self, last_modified):
        self._last_modified = last_modified

    def get(self, key):
        if key == "last_modified":
            return self._last_modified
        return None


class FakeDownload:
    def __init__(self, data):
        self.data = data

    def readall(self):
        return self.data


class FakeBlobClient:
    def __init__(self, name="blob", last_modified=None, props_error=None, read_data=b"data"):
        self.name = name
        self.url = f"https://example.blob/{name}"
        self.last_modified = last_modified
        self.props_error = props_error
        self.read_data = read_data
        self.copy_urls = []
        self.deleted = False
        self.download_error = None
        self.copy_error = None
        self.delete_error = None

    def get_blob_properties(self):
        if self.props_error:
            raise self.props_error
        return FakeBlobProperties(self.last_modified)

    def start_copy_from_url(self, url):
        if self.copy_error:
            raise self.copy_error
        self.copy_urls.append(url)

    def delete_blob(self):
        if self.delete_error:
            raise self.delete_error
        self.deleted = True

    def download_blob(self):
        if self.download_error:
            raise self.download_error
        return FakeDownload(self.read_data)


class FakeContainerClient:
    def __init__(self, blobs=None, list_error=None):
        self.blobs = blobs or []
        self.list_error = list_error

    def list_blobs(self):
        if self.list_error:
            raise self.list_error
        return [types.SimpleNamespace(name=name) for name in self.blobs]


class FakeBlobServiceClient:
    created_from = []
    next_instance = None

    def __init__(self):
        self.container_clients = {}
        self.blob_clients = {}

    @classmethod
    def from_connection_string(cls, conn_str):
        cls.created_from.append(conn_str)
        if cls.next_instance is not None:
            inst = cls.next_instance
            cls.next_instance = None
            return inst
        return cls()

    def get_container_client(self, container):
        value = self.container_clients.get(container)
        if value is None:
            value = FakeContainerClient()
            self.container_clients[container] = value
        return value

    def get_blob_client(self, container, blob):
        key = (container, blob)
        value = self.blob_clients.get(key)
        if value is None:
            value = FakeBlobClient(blob)
            self.blob_clients[key] = value
        return value


class FakePaginator:
    def __init__(self, pages=None, error=None):
        self.pages = pages or []
        self.error = error
        self.paginate_calls = []

    def paginate(self, **kwargs):
        self.paginate_calls.append(kwargs)
        if self.error:
            raise self.error
        return self.pages


class FakeBody:
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data


class FakeS3Client:
    def __init__(self):
        self.paginator = FakePaginator()
        self.head_responses = {}
        self.head_errors = {}
        self.copy_calls = []
        self.delete_calls = []
        self.get_responses = {}
        self.get_errors = {}
        self.copy_error = None
        self.delete_error = None

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self.paginator

    def head_object(self, Bucket, Key):
        if (Bucket, Key) in self.head_errors:
            raise self.head_errors[(Bucket, Key)]
        return {"LastModified": self.head_responses[(Bucket, Key)]}

    def copy_object(self, **kwargs):
        if self.copy_error:
            raise self.copy_error
        self.copy_calls.append(kwargs)

    def delete_object(self, **kwargs):
        if self.delete_error:
            raise self.delete_error
        self.delete_calls.append(kwargs)

    def get_object(self, Bucket, Key):
        if (Bucket, Key) in self.get_errors:
            raise self.get_errors[(Bucket, Key)]
        return {"Body": FakeBody(self.get_responses[(Bucket, Key)])}


@pytest.fixture(autouse=True)
def fake_external_modules(monkeypatch):
    FakeBlobServiceClient.created_from.clear()
    FakeBlobServiceClient.next_instance = None

    azure_exceptions = types.ModuleType("azure.core.exceptions")
    azure_exceptions.AzureError = FakeAzureError

    azure_storage_blob = types.ModuleType("azure.storage.blob")
    azure_storage_blob.BlobServiceClient = FakeBlobServiceClient

    monkeypatch.setitem(sys.modules, "azure", types.ModuleType("azure"))
    monkeypatch.setitem(sys.modules, "azure.core", types.ModuleType("azure.core"))
    monkeypatch.setitem(sys.modules, "azure.core.exceptions", azure_exceptions)
    monkeypatch.setitem(sys.modules, "azure.storage", types.ModuleType("azure.storage"))
    monkeypatch.setitem(sys.modules, "azure.storage.blob", azure_storage_blob)

    botocore_client = types.ModuleType("botocore.client")
    botocore_client.Config = Mock(side_effect=lambda **kwargs: {"Config": kwargs})

    botocore_exceptions = types.ModuleType("botocore.exceptions")
    botocore_exceptions.BotoCoreError = FakeBotoCoreError
    botocore_exceptions.ClientError = FakeClientError

    monkeypatch.setitem(sys.modules, "botocore", types.ModuleType("botocore"))
    monkeypatch.setitem(sys.modules, "botocore.client", botocore_client)
    monkeypatch.setitem(sys.modules, "botocore.exceptions", botocore_exceptions)

    boto3_mod = types.ModuleType("boto3")
    boto3_mod.Session = Mock()
    boto3_mod.client = Mock(return_value=FakeS3Client())
    monkeypatch.setitem(sys.modules, "boto3", boto3_mod)

    utils_pkg = types.ModuleType("utils")
    utils_otel_logger = types.ModuleType("utils.otel_logger")
    utils_otel_logger.Logging = Mock(
        return_value=types.SimpleNamespace(
            create_logger=Mock(return_value=FakeLogger())
        )
    )

    monkeypatch.setitem(sys.modules, "utils", utils_pkg)
    monkeypatch.setitem(sys.modules, "utils.otel_logger", utils_otel_logger)


@pytest.fixture
def module():
    if not SOURCE_PATH.exists():
        pytest.skip(f"Source file not found: {SOURCE_PATH}")

    module_name = "data_transection_under_test"
    sys.modules.pop(module_name, None)

    spec = importlib.util.spec_from_file_location(module_name, SOURCE_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def logger():
    return FakeLogger()


@pytest.fixture
def trans(module, logger):
    return module.DataTransection(
        source_azure_conn_str="src-conn",
        source_container_name="src-container",
        target_container_name="tgt-container",
        source_blob_name=None,
        target_blob_name=None,
        target_azure_conn_str="tgt-conn",
        aws_endpoint_url="http://localhost:4566",
        aws_access_key_id="key",
        aws_secret_access_key="secret",
        aws_region="eu-west-2",
        logger=logger,
    )


def test_validate_object_key_and_copy_result(module):
    assert module._validate_object_key("safe/path-file_1.csv") == "safe/path-file_1.csv"

    for bad_key in ["", "bad key", "bad;key", "bad$key"]:
        with pytest.raises(ValueError):
            module._validate_object_key(bad_key)

    copied = module.CopyResult(copied=True, skipped=False, reason="created")
    skipped = module.CopyResult(copied=False, skipped=True, reason="same")
    failed = module.CopyResult(copied=False, skipped=False, reason="missing")

    assert bool(copied) is True
    assert bool(skipped) is True
    assert bool(failed) is False


def test_client_factories_cache_clients(trans):
    src_client_1 = trans._get_azure_src_client()
    src_client_2 = trans._get_azure_src_client()
    tgt_client_1 = trans._get_azure_tgt_client()
    tgt_client_2 = trans._get_azure_tgt_client()

    assert src_client_1 is src_client_2
    assert tgt_client_1 is tgt_client_2
    assert FakeBlobServiceClient.created_from == ["src-conn", "tgt-conn"]

    s3_1 = trans._get_s3_client()
    s3_2 = trans._get_s3_client()

    assert s3_1 is s3_2

    _, kwargs = sys.modules["boto3"].client.call_args

    assert kwargs["endpoint_url"] == "http://localhost:4566"
    assert kwargs["aws_access_key_id"] == "key"
    assert kwargs["aws_secret_access_key"] == "secret"
    assert kwargs["region_name"] == "eu-west-2"


def test_s3_client_without_optional_credentials(module, logger):
    transaction = module.DataTransection(
        "src",
        "src-bucket",
        "tgt-bucket",
        None,
        None,
        "tgt",
        aws_region="us-east-1",
        logger=logger,
    )

    transaction._get_s3_client()

    _, kwargs = sys.modules["boto3"].client.call_args

    assert "endpoint_url" not in kwargs
    assert "aws_access_key_id" not in kwargs
    assert "aws_secret_access_key" not in kwargs


def test_source_file_info_routes_and_unsupported(trans, logger):
    trans._azure_list_files = Mock(return_value=["a.csv"])
    trans._s3_list_files = Mock(return_value=["b.csv"])

    assert trans.source_file_info(" azure ") == ["a.csv"]
    assert trans.source_file_info("AWS") == ["b.csv"]
    assert trans.source_file_info("gcp") == []

    assert logger.warning_calls[-1][0] == "Unsupported cloud provider for list operation"


def test_file_copy_routes_validates_and_unsupported(module, trans, logger):
    azure_result = module.CopyResult(copied=True, skipped=False, reason="created")
    s3_result = module.CopyResult(copied=True, skipped=False, reason="created")

    trans._azure_copy_file = Mock(return_value=azure_result)
    trans._s3_copy_file = Mock(return_value=s3_result)

    assert trans.file_copy(" azure ", "a.csv") is azure_result
    trans._azure_copy_file.assert_called_once_with("a.csv", "a.csv")

    assert trans.file_copy("AWS", "a.csv", dest_file_name="b.csv") is s3_result
    trans._s3_copy_file.assert_called_once_with("a.csv", "b.csv")

    with pytest.raises(ValueError):
        trans.file_copy("azure", "bad key")

    with pytest.raises(ValueError):
        trans.file_copy("azure", "a.csv", dest_file_name="bad key")

    result = trans.file_copy("gcp", "a.csv")

    assert result.copied is False
    assert result.skipped is False
    assert result.reason == "unsupported vendor"
    assert logger.warning_calls[-1][0] == "Unsupported cloud vendor for copy operation"


def test_file_move_routes_validates_and_unsupported(trans, logger):
    trans._azure_move_file = Mock(return_value=True)
    trans._s3_move_file = Mock(return_value=True)

    assert trans.file_move("azure", "a.csv") is True
    trans._azure_move_file.assert_called_once_with("a.csv", "a.csv")

    assert trans.file_move("aws", "a.csv", dest_file_name="b.csv") is True
    trans._s3_move_file.assert_called_once_with("a.csv", "b.csv")

    with pytest.raises(ValueError):
        trans.file_move("azure", "bad key")

    with pytest.raises(ValueError):
        trans.file_move("azure", "a.csv", dest_file_name="bad key")

    assert trans.file_move("gcp", "a.csv") is False
    assert logger.warning_calls[-1][0] == "Unsupported cloud vendor for move operation"


def test_data_read_routes_no_source_invalid_and_unsupported(trans, logger):
    assert trans.data_read("azure") == ""
    assert logger.error_calls[-1][0] == "data_read called with no source_blob_name set"

    trans.source_blob_name = "bad key"

    with pytest.raises(ValueError):
        trans.data_read("azure")

    trans.source_blob_name = "a.csv"
    trans._azure_read_file = Mock(return_value="azure-data")
    trans._s3_read_file = Mock(return_value="s3-data")

    assert trans.data_read(" azure ") == "azure-data"
    assert trans.data_read("AWS") == "s3-data"
    assert trans.data_read("gcp") == ""

    assert logger.warning_calls[-1][0] == "Unsupported cloud vendor for read operation"


def test_azure_list_files_success_and_error(trans, logger):
    src_client = FakeBlobServiceClient()
    src_client.container_clients["src-container"] = FakeContainerClient(
        ["a.csv", "b.csv"]
    )
    trans._azure_src_client = src_client

    assert trans._azure_list_files() == ["a.csv", "b.csv"]

    src_client.container_clients["src-container"] = FakeContainerClient(
        list_error=FakeAzureError("boom")
    )

    with pytest.raises(FakeAzureError):
        trans._azure_list_files()

    assert logger.error_calls[-1][0] == "Azure list blobs failed"


def test_azure_get_last_modified_success_missing_and_none(trans):
    aware = datetime(2026, 1, 1, tzinfo=timezone.utc)

    client = FakeBlobServiceClient()
    client.blob_clients[("container", "exists.csv")] = FakeBlobClient(
        "exists.csv",
        last_modified=aware,
    )
    client.blob_clients[("container", "none.csv")] = FakeBlobClient(
        "none.csv",
        last_modified=None,
    )
    client.blob_clients[("container", "missing.csv")] = FakeBlobClient(
        "missing.csv",
        props_error=FakeAzureError("404"),
    )

    assert trans._azure_get_last_modified(client, "container", "exists.csv") == aware
    assert trans._azure_get_last_modified(client, "container", "none.csv") is None
    assert trans._azure_get_last_modified(client, "container", "missing.csv") is None


def test_azure_copy_source_missing_skip_created_overwritten_and_error(trans, logger):
    older = datetime(2026, 1, 1, tzinfo=timezone.utc)
    newer = datetime(2026, 1, 2, tzinfo=timezone.utc)

    src_client = FakeBlobServiceClient()
    tgt_client = FakeBlobServiceClient()
    trans._azure_src_client = src_client
    trans._azure_tgt_client = tgt_client

    result = trans._azure_copy_file("missing.csv", "missing.csv")

    assert result.copied is False
    assert result.skipped is False
    assert result.reason == "source blob not found"
    assert logger.error_calls[-1][0] == "Azure copy aborted – source blob not found"

    src_blob = FakeBlobClient("a.csv", last_modified=older)
    tgt_blob = FakeBlobClient("a.csv", last_modified=newer)
    src_client.blob_clients[("src-container", "a.csv")] = src_blob
    tgt_client.blob_clients[("tgt-container", "a.csv")] = tgt_blob

    result = trans._azure_copy_file("a.csv", "a.csv")

    assert result.copied is False
    assert result.skipped is True
    assert result.reason.startswith("target up-to-date")

    src_blob = FakeBlobClient("b.csv", last_modified=newer)
    tgt_blob = FakeBlobClient("b.csv", last_modified=None)
    src_client.blob_clients[("src-container", "b.csv")] = src_blob
    tgt_client.blob_clients[("tgt-container", "b.csv")] = tgt_blob

    result = trans._azure_copy_file("b.csv", "b.csv")

    assert result.copied is True
    assert result.reason == "created"
    assert tgt_blob.copy_urls == [src_blob.url]

    src_blob = FakeBlobClient("c.csv", last_modified=newer)
    tgt_blob = FakeBlobClient("c.csv", last_modified=older)
    src_client.blob_clients[("src-container", "c.csv")] = src_blob
    tgt_client.blob_clients[("tgt-container", "c.csv")] = tgt_blob

    result = trans._azure_copy_file("c.csv", "c.csv")

    assert result.copied is True
    assert result.reason == "overwritten"

    src_blob = FakeBlobClient("d.csv", last_modified=newer)
    tgt_blob = FakeBlobClient("d.csv", last_modified=None)
    tgt_blob.copy_error = FakeAzureError("copy failed")
    src_client.blob_clients[("src-container", "d.csv")] = src_blob
    tgt_client.blob_clients[("tgt-container", "d.csv")] = tgt_blob

    with pytest.raises(FakeAzureError):
        trans._azure_copy_file("d.csv", "d.csv")

    assert logger.error_calls[-1][0] == "Azure blob copy failed"


def test_azure_move_success_and_error(trans, logger):
    src_client = FakeBlobServiceClient()
    tgt_client = FakeBlobServiceClient()
    trans._azure_src_client = src_client
    trans._azure_tgt_client = tgt_client

    src_blob = FakeBlobClient("a.csv")
    tgt_blob = FakeBlobClient("a.csv")
    src_client.blob_clients[("src-container", "a.csv")] = src_blob
    tgt_client.blob_clients[("tgt-container", "a.csv")] = tgt_blob

    assert trans._azure_move_file("a.csv", "a.csv") is True
    assert tgt_blob.copy_urls == [src_blob.url]
    assert src_blob.deleted is True

    src_blob.delete_error = FakeAzureError("delete failed")

    with pytest.raises(FakeAzureError):
        trans._azure_move_file("a.csv", "a.csv")

    assert logger.error_calls[-1][0] == "Azure blob move failed"


def test_azure_read_success_and_error(trans, logger):
    src_client = FakeBlobServiceClient()
    trans._azure_src_client = src_client

    blob = FakeBlobClient("a.csv", read_data=b"hello")
    src_client.blob_clients[("src-container", "a.csv")] = blob

    assert trans._azure_read_file("a.csv") == b"hello"

    blob.download_error = FakeAzureError("read failed")

    with pytest.raises(FakeAzureError):
        trans._azure_read_file("a.csv")

    assert logger.error_calls[-1][0] == "Azure blob read failed"


def test_s3_list_success_and_error(trans, logger):
    s3 = FakeS3Client()
    s3.paginator = FakePaginator(
        [
            {"Contents": [{"Key": "a.csv"}]},
            {"Contents": [{"Key": "b.csv"}]},
            {},
        ]
    )
    trans._s3_client = s3

    assert trans._s3_list_files() == ["a.csv", "b.csv"]

    s3.paginator = FakePaginator(error=FakeBotoCoreError("list failed"))

    with pytest.raises(FakeBotoCoreError):
        trans._s3_list_files()

    assert logger.error_calls[-1][0] == "S3/MinIO list objects failed"


def test_s3_get_last_modified_success_not_found_and_other_error(trans):
    aware = datetime(2026, 1, 1, tzinfo=timezone.utc)

    s3 = FakeS3Client()
    s3.head_responses[("bucket", "exists.csv")] = aware
    s3.head_errors[("bucket", "missing.csv")] = FakeClientError("404")
    s3.head_errors[("bucket", "nosuch.csv")] = FakeClientError("NoSuchKey")
    s3.head_errors[("bucket", "boom.csv")] = FakeClientError("500")
    trans._s3_client = s3

    assert trans._s3_get_last_modified("bucket", "exists.csv") == aware
    assert trans._s3_get_last_modified("bucket", "missing.csv") is None
    assert trans._s3_get_last_modified("bucket", "nosuch.csv") is None

    with pytest.raises(FakeClientError):
        trans._s3_get_last_modified("bucket", "boom.csv")


def test_s3_copy_source_missing_skip_created_overwritten_and_error(trans, logger):
    older = datetime(2026, 1, 1, tzinfo=timezone.utc)
    newer = datetime(2026, 1, 2, tzinfo=timezone.utc)

    s3 = FakeS3Client()
    trans._s3_client = s3

    s3.head_errors[("src-container", "missing.csv")] = FakeClientError("404")

    result = trans._s3_copy_file("missing.csv", "missing.csv")

    assert result.copied is False
    assert result.skipped is False
    assert result.reason == "source object not found"
    assert logger.error_calls[-1][0] == "S3/MinIO copy aborted – source object not found"

    s3.head_responses[("src-container", "a.csv")] = older
    s3.head_responses[("tgt-container", "a.csv")] = newer

    result = trans._s3_copy_file("a.csv", "a.csv")

    assert result.skipped is True
    assert result.reason.startswith("target up-to-date")

    s3.head_responses[("src-container", "b.csv")] = newer
    s3.head_errors[("tgt-container", "b.csv")] = FakeClientError("404")

    result = trans._s3_copy_file("b.csv", "b.csv")

    assert result.copied is True
    assert result.reason == "created"
    assert s3.copy_calls[-1]["CopySource"] == {
        "Bucket": "src-container",
        "Key": "b.csv",
    }

    s3.head_responses[("src-container", "c.csv")] = newer
    s3.head_responses[("tgt-container", "c.csv")] = older

    result = trans._s3_copy_file("c.csv", "c.csv")

    assert result.copied is True
    assert result.reason == "overwritten"

    s3.head_responses[("src-container", "d.csv")] = newer
    s3.head_errors[("tgt-container", "d.csv")] = FakeClientError("404")
    s3.copy_error = FakeClientError("500")

    with pytest.raises(FakeClientError):
        trans._s3_copy_file("d.csv", "d.csv")

    assert logger.error_calls[-1][0] == "S3/MinIO object copy failed"


def test_s3_move_success_and_error(trans, logger):
    s3 = FakeS3Client()
    trans._s3_client = s3

    assert trans._s3_move_file("a.csv", "b.csv") is True
    assert s3.copy_calls[-1]["CopySource"] == {
        "Bucket": "src-container",
        "Key": "a.csv",
    }
    assert s3.delete_calls[-1] == {
        "Bucket": "src-container",
        "Key": "a.csv",
    }

    s3.copy_error = FakeClientError("500")

    with pytest.raises(FakeClientError):
        trans._s3_move_file("x.csv", "y.csv")

    assert logger.error_calls[-1][0] == "S3/MinIO object move failed"


def test_s3_read_success_and_error(trans, logger):
    s3 = FakeS3Client()
    s3.get_responses[("src-container", "a.csv")] = b"hello"
    trans._s3_client = s3

    assert trans._s3_read_file("a.csv") == b"hello"

    s3.get_errors[("src-container", "bad.csv")] = FakeClientError("500")

    with pytest.raises(FakeClientError):
        trans._s3_read_file("bad.csv")

    assert logger.error_calls[-1][0] == "S3/MinIO object read failed"