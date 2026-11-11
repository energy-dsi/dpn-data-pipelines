import importlib.util
import logging
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

# google-cloud-storage / google-api-core are real installed dependencies (unlike
# azure/boto3 above, which fake_external_modules() fakes via sys.modules) — the
# module under test imports the real Conflict/GoogleAPIError/NotFound classes, so
# tests raise/catch those same real classes rather than faking them too.
from google.api_core.exceptions import Conflict, GoogleAPIError, NotFound


SOURCE_PATH = Path("utils/data_transection.py")


class FakeAzureError(Exception):
    pass


class FakeResourceExistsError(FakeAzureError):
    pass


class FakeResourceNotFoundError(FakeAzureError):
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
    def __init__(self, blobs=None, list_error=None, exists_value=True, exists_error=None):
        self.blobs = blobs or []
        self.list_error = list_error
        self.exists_value = exists_value
        self.exists_error = exists_error

    def list_blobs(self):
        if self.list_error:
            raise self.list_error
        items = []
        for entry in self.blobs:
            name, last_modified = entry if isinstance(entry, tuple) else (entry, None)
            items.append(types.SimpleNamespace(name=name, last_modified=last_modified))
        return items

    def exists(self):
        if self.exists_error:
            raise self.exists_error
        return self.exists_value


class FakeBlobServiceClient:
    created_from = []
    next_instance = None

    def __init__(self):
        self.container_clients = {}
        self.blob_clients = {}
        self.create_container_calls = []
        self.create_container_errors = {}
        self.delete_container_calls = []
        self.delete_container_errors = {}

    @classmethod
    def from_connection_string(cls, conn_str, logging_enable=False):
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

    def create_container(self, name):
        self.create_container_calls.append(name)
        if name in self.create_container_errors:
            raise self.create_container_errors[name]

    def delete_container(self, name):
        self.delete_container_calls.append(name)
        if name in self.delete_container_errors:
            raise self.delete_container_errors[name]


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
        self.head_bucket_error = None
        self.head_bucket_calls = []
        self.create_bucket_calls = []
        self.create_bucket_error = None
        self.delete_bucket_calls = []
        self.delete_bucket_error = None
        self.delete_objects_calls = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self.paginator

    def head_object(self, Bucket, Key):
        if (Bucket, Key) in self.head_errors:
            raise self.head_errors[(Bucket, Key)]
        return {"LastModified": self.head_responses[(Bucket, Key)]}

    def head_bucket(self, Bucket):
        self.head_bucket_calls.append(Bucket)
        if self.head_bucket_error:
            raise self.head_bucket_error

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

    def create_bucket(self, **kwargs):
        if self.create_bucket_error:
            raise self.create_bucket_error
        self.create_bucket_calls.append(kwargs)

    def delete_bucket(self, Bucket):
        if self.delete_bucket_error:
            raise self.delete_bucket_error
        self.delete_bucket_calls.append(Bucket)

    def delete_objects(self, **kwargs):
        self.delete_objects_calls.append(kwargs)


class FakeGCSBlobInfo:
    def __init__(self, name, updated=None):
        self.name = name
        self.updated = updated


class FakeGCSBucket:
    def __init__(self, name):
        self.name = name
        self.existing_blobs = {}
        self.list_error = None
        self.copy_error = None
        self.delete_blob_error = None
        self.exists_value = True
        self.exists_error = None
        self.delete_error = None
        self.copy_calls = []
        self.deleted_blobs = []
        self.delete_force_calls = []

    def list_blobs(self):
        if self.list_error:
            raise self.list_error
        return list(self.existing_blobs.values())

    def get_blob(self, name):
        return self.existing_blobs.get(name)

    def blob(self, name):
        return types.SimpleNamespace(name=name)

    def copy_blob(self, blob, dest_bucket, new_name):
        if self.copy_error:
            raise self.copy_error
        self.copy_calls.append((blob.name, dest_bucket.name, new_name))

    def delete_blob(self, name):
        if self.delete_blob_error:
            raise self.delete_blob_error
        self.deleted_blobs.append(name)

    def exists(self):
        if self.exists_error:
            raise self.exists_error
        return self.exists_value

    def delete(self, force=False):
        if self.delete_error:
            raise self.delete_error
        self.delete_force_calls.append(force)


class FakeGCSClient:
    def __init__(self):
        self.buckets = {}
        self.create_bucket_error = None
        self.created_buckets = []

    def bucket(self, name):
        b = self.buckets.get(name)
        if b is None:
            b = FakeGCSBucket(name)
            self.buckets[name] = b
        return b

    def create_bucket(self, name):
        if self.create_bucket_error:
            raise self.create_bucket_error
        self.created_buckets.append(name)


@pytest.fixture(autouse=True)
def fake_external_modules(monkeypatch):
    FakeBlobServiceClient.created_from.clear()
    FakeBlobServiceClient.next_instance = None

    azure_exceptions = types.ModuleType("azure.core.exceptions")
    azure_exceptions.AzureError = FakeAzureError
    azure_exceptions.ResourceExistsError = FakeResourceExistsError
    azure_exceptions.ResourceNotFoundError = FakeResourceNotFoundError

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

    # Spaces and S3's own "safe" + "special handling" punctuation tiers are
    # explicitly allowed — business files are routinely named things like
    # "test sample 1.xml", a browser's own repeat-download naming
    # "attribution (3).json", or "Order #12, Q3 & Q4.xlsx"-style names (see
    # that module's docstring on _SAFE_OBJECT_KEY_RE for the exact AWS/Azure
    # rules this is aligned with). '#' is allowed too — every key here is
    # passed as a plain SDK parameter, never assembled into a URL, so AWS's
    # console/URL-fragment ambiguity warning about '#' never applies.
    assert module._validate_object_key("test sample 1.xml") == "test sample 1.xml"
    assert module._validate_object_key("attribution (3).json") == "attribution (3).json"
    assert (
        module._validate_object_key("John's report, v2 @ Q3 & Q4; final!.csv")
        == "John's report, v2 @ Q3 & Q4; final!.csv"
    )
    assert module._validate_object_key("Order #12, Q3 & Q4.xlsx") == "Order #12, Q3 & Q4.xlsx"
    assert module._validate_object_key("my_file #1.txt") == "my_file #1.txt"

    # On S3's own "avoid" list (backslash, {}, ^, %, `, [], ", <>, ~, |)
    # even though several of these are technically tolerated by S3 itself —
    # excluded here for the same ambiguity reasons AWS itself documents.
    for bad_key in [
        "", "bad\\key", "bad[key]", "bad{key}", "bad%key",
        "bad^key", "bad`key", 'bad"key', "bad<key>", "bad~key", "bad|key",
        "../escape.csv", "a/../b.csv",
        "trailing-dot.",  # Azure disallows a blob name ending in '.'
        "trailing-slash/",  # ...or '/'
    ]:
        with pytest.raises(ValueError):
            module._validate_object_key(bad_key)

    copied = module.CopyResult(copied=True, skipped=False, reason="created")
    skipped = module.CopyResult(copied=False, skipped=True, reason="same")
    failed = module.CopyResult(copied=False, skipped=False, reason="missing")

    assert bool(copied) is True
    assert bool(skipped) is True
    assert bool(failed) is False


def test_validate_object_key_ac2_examples(module):
    """AC2 — Support File Names Containing Spaces and Special Characters.
    The requirement's own example filenames must be accepted and returned
    completely unchanged — no trimming of the leading space in
    " logs.txt", no alteration of the embedded space in "application
    logs.txt" or the '@' in "logs@2024.txt"."""
    for name in ["application logs.txt", " logs.txt", "logs@2024.txt"]:
        assert module._validate_object_key(name) == name


def test_validate_object_key_path_prefixed_with_various_extensions(module):
    """Path-prefixed keys ('/' as a directory separator) combined with
    special characters, across formats beyond the plain .txt/.csv/.xml
    cases above — pdf/docx/xlsx/png/svg go through the same charset-only
    check, so extension never affects the outcome."""
    for name in [
        "logs/new.txt",
        "logs/report (final).pdf",
        "reports/Q3 & Q4 summary.docx",
        "exports/Order #12, Q3 & Q4.xlsx",
        "images/logo (v2).png",
        "diagrams/flow chart #1.svg",
        "config/attribution (3).json",
        "exports/logs@2024, v2 (final) #1.json",
    ]:
        assert module._validate_object_key(name) == name

    for bad_key in [
        "logs/../escape.txt",  # path traversal via a nested '..' segment
        "logs/",  # Azure disallows a blob name ending in '/'
        "logs\\new.txt",  # backslash is on S3's "avoid" list
    ]:
        with pytest.raises(ValueError):
            module._validate_object_key(bad_key)


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


def test_debug_logging_off_by_default_does_not_enable_azure_http_logging(module, logger):
    FakeBlobServiceClient.created_from = []
    trans = module.DataTransection(
        source_azure_conn_str="src-conn",
        source_container_name="src-container",
        target_container_name="tgt-container",
        source_blob_name=None,
        target_blob_name=None,
        target_azure_conn_str="tgt-conn",
        logger=logger,
    )

    captured = {}
    orig = FakeBlobServiceClient.from_connection_string.__func__

    def spy(cls, conn_str, logging_enable=False):
        captured["logging_enable"] = logging_enable
        return orig(cls, conn_str, logging_enable=logging_enable)

    FakeBlobServiceClient.from_connection_string = classmethod(spy)
    try:
        trans._get_azure_src_client()
    finally:
        FakeBlobServiceClient.from_connection_string = classmethod(orig)

    assert captured["logging_enable"] is False


def test_debug_logging_true_enables_azure_http_logging_and_sdk_loggers(module, logger, monkeypatch):
    FakeBlobServiceClient.created_from = []
    sdk_loggers = ["azure", "boto3", "botocore", "google"]
    for name in sdk_loggers:
        logging.getLogger(name).setLevel(logging.WARNING)

    trans = module.DataTransection(
        source_azure_conn_str="src-conn",
        source_container_name="src-container",
        target_container_name="tgt-container",
        source_blob_name=None,
        target_blob_name=None,
        target_azure_conn_str="tgt-conn",
        logger=logger,
        debug_logging=True,
    )

    for name in sdk_loggers:
        assert logging.getLogger(name).level == logging.DEBUG

    captured = {}
    orig = FakeBlobServiceClient.from_connection_string.__func__

    def spy(cls, conn_str, logging_enable=False):
        captured["logging_enable"] = logging_enable
        return orig(cls, conn_str, logging_enable=logging_enable)

    FakeBlobServiceClient.from_connection_string = classmethod(spy)
    try:
        trans._get_azure_src_client()
    finally:
        FakeBlobServiceClient.from_connection_string = classmethod(orig)

    assert captured["logging_enable"] is True


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


def test_ac2_filenames_survive_listing_to_copy_unaltered(module, trans):
    """Regression guard for AC2: the exact string returned by
    source_file_info() must be the exact string handed to the underlying
    per-vendor copy call by file_copy() — catches any future accidental
    .strip()/.lower()/re.sub reintroduction between listing and copy."""
    ac2_filenames = ["application logs.txt", " logs.txt", "logs@2024.txt"]

    trans._s3_list_files = Mock(return_value=list(ac2_filenames))
    trans._s3_copy_file = Mock(
        return_value=module.CopyResult(copied=True, skipped=False, reason="copied")
    )

    listed = trans.source_file_info("aws")
    assert listed == ac2_filenames

    for name in listed:
        trans.file_copy("aws", name)

    assert trans._s3_copy_file.call_args_list == [((name, name),) for name in ac2_filenames]


def test_source_file_info_routes_and_unsupported(trans, logger):
    trans._azure_list_files = Mock(return_value=["a.csv"])
    trans._s3_list_files = Mock(return_value=["b.csv"])

    assert trans.source_file_info(" azure ") == ["a.csv"]
    assert trans.source_file_info("AWS") == ["b.csv"]
    assert trans.source_file_info("oracle") == []

    assert logger.warning_calls[-1][0] == "Unsupported cloud provider for list operation"


def test_list_files_with_timestamps_routes_and_unsupported(trans, logger):
    trans._azure_list_files_with_timestamps = Mock(return_value={"a.csv": None})
    trans._s3_list_files_with_timestamps = Mock(return_value={"b.csv": None})
    trans._gcs_list_files_with_timestamps = Mock(return_value={"c.csv": None})

    assert trans.list_files_with_timestamps(" azure ", side="source") == {"a.csv": None}
    assert trans.list_files_with_timestamps("AWS", side="target") == {"b.csv": None}
    assert trans.list_files_with_timestamps("gcp") == {"c.csv": None}
    assert trans.list_files_with_timestamps("oracle") == {}

    trans._azure_list_files_with_timestamps.assert_called_once_with("source")
    trans._s3_list_files_with_timestamps.assert_called_once_with("target")
    trans._gcs_list_files_with_timestamps.assert_called_once_with("source")  # default side
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
        trans.file_copy("azure", r"bad\key")

    with pytest.raises(ValueError):
        trans.file_copy("azure", "a.csv", dest_file_name=r"bad\key")

    result = trans.file_copy("oracle", "a.csv")

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
        trans.file_move("azure", r"bad\key")

    with pytest.raises(ValueError):
        trans.file_move("azure", "a.csv", dest_file_name=r"bad\key")

    assert trans.file_move("oracle", "a.csv") is False
    assert logger.warning_calls[-1][0] == "Unsupported cloud vendor for move operation"


def test_data_read_routes_no_source_invalid_and_unsupported(trans, logger):
    assert trans.data_read("azure") == ""
    assert logger.error_calls[-1][0] == "data_read called with no source_blob_name set"

    trans.source_blob_name = r"bad\key"

    with pytest.raises(ValueError):
        trans.data_read("azure")

    trans.source_blob_name = "a.csv"
    trans._azure_read_file = Mock(return_value="azure-data")
    trans._s3_read_file = Mock(return_value="s3-data")

    assert trans.data_read(" azure ") == "azure-data"
    assert trans.data_read("AWS") == "s3-data"
    assert trans.data_read("oracle") == ""

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


def test_azure_list_files_with_timestamps_source_and_target(trans, logger):
    src_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    tgt_ts = datetime(2026, 1, 2, tzinfo=timezone.utc)

    src_client = FakeBlobServiceClient()
    src_client.container_clients["src-container"] = FakeContainerClient(
        [("a.csv", src_ts), ("no-timestamp.csv", None)]
    )
    trans._azure_src_client = src_client

    tgt_client = FakeBlobServiceClient()
    tgt_client.container_clients["tgt-container"] = FakeContainerClient([("a.csv", tgt_ts)])
    trans._azure_tgt_client = tgt_client

    assert trans._azure_list_files_with_timestamps("source") == {
        "a.csv": src_ts,
        "no-timestamp.csv": None,
    }
    assert trans._azure_list_files_with_timestamps("target") == {"a.csv": tgt_ts}


def test_azure_list_files_with_timestamps_error(trans, logger):
    src_client = FakeBlobServiceClient()
    src_client.container_clients["src-container"] = FakeContainerClient(
        list_error=FakeAzureError("boom")
    )
    trans._azure_src_client = src_client

    with pytest.raises(FakeAzureError):
        trans._azure_list_files_with_timestamps("source")
    assert logger.error_calls[-1][0] == "Azure list blobs (with timestamps) failed"


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


def test_s3_list_files_with_timestamps_source_and_target(trans, logger):
    src_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    tgt_ts = datetime(2026, 1, 2, tzinfo=timezone.utc)

    s3 = FakeS3Client()
    s3.paginator = FakePaginator(
        [{"Contents": [{"Key": "a.csv", "LastModified": src_ts}, {"Key": "no-ts.csv"}]}]
    )
    trans._s3_client = s3
    assert trans._s3_list_files_with_timestamps("source") == {"a.csv": src_ts, "no-ts.csv": None}

    s3.paginator = FakePaginator([{"Contents": [{"Key": "a.csv", "LastModified": tgt_ts}]}])
    assert trans._s3_list_files_with_timestamps("target") == {"a.csv": tgt_ts}


def test_s3_list_files_with_timestamps_error(trans, logger):
    s3 = FakeS3Client()
    s3.paginator = FakePaginator(error=FakeBotoCoreError("list failed"))
    trans._s3_client = s3

    with pytest.raises(FakeBotoCoreError):
        trans._s3_list_files_with_timestamps("source")
    assert logger.error_calls[-1][0] == "S3/MinIO list objects (with timestamps) failed"


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


def test_container_exists_azure_true_false_and_error(trans, logger):
    src_client = FakeBlobServiceClient()
    trans._azure_src_client = src_client

    src_client.container_clients["present"] = FakeContainerClient(exists_value=True)
    assert trans.container_exists("azure", "present", side="source") is True

    src_client.container_clients["absent"] = FakeContainerClient(exists_value=False)
    assert trans.container_exists("azure", "absent", side="source") is False

    src_client.container_clients["boom"] = FakeContainerClient(exists_error=FakeAzureError("boom"))
    with pytest.raises(FakeAzureError):
        trans.container_exists("azure", "boom", side="source")
    assert logger.error_calls[-1][0] == "Azure container existence check failed"


def test_container_exists_s3_true_false_and_error(trans, logger):
    s3 = FakeS3Client()
    trans._s3_client = s3

    assert trans.container_exists("aws", "present-bucket") is True
    assert s3.head_bucket_calls == ["present-bucket"]

    s3.head_bucket_error = FakeClientError("404")
    assert trans.container_exists("aws", "absent-bucket") is False

    s3.head_bucket_error = FakeClientError("500")
    with pytest.raises(FakeClientError):
        trans.container_exists("aws", "boom-bucket")
    assert logger.error_calls[-1][0] == "S3/MinIO bucket existence check failed"


def test_container_exists_unsupported_provider(trans, logger):
    assert trans.container_exists("unknown", "x") is False
    assert logger.warning_calls[-1][0] == "Unsupported cloud provider for container existence check"


def test_container_exists_gcp_routes(trans):
    trans._gcs_bucket_exists = Mock(return_value=True)

    assert trans.container_exists(" gcp ", "bucket") is True
    trans._gcs_bucket_exists.assert_called_once_with("bucket")


def test_ensure_container_exists_routes_and_unsupported(trans, logger):
    trans._azure_ensure_container = Mock(return_value=True)
    trans._s3_ensure_bucket = Mock(return_value=True)
    trans._gcs_ensure_bucket = Mock(return_value=True)

    assert trans.ensure_container_exists(" azure ", "c", side="target") is True
    trans._azure_ensure_container.assert_called_once_with("c", "target")

    assert trans.ensure_container_exists("AWS", "b") is True
    trans._s3_ensure_bucket.assert_called_once_with("b")

    assert trans.ensure_container_exists("gcp", "g") is True
    trans._gcs_ensure_bucket.assert_called_once_with("g")

    assert trans.ensure_container_exists("oracle", "x") is False
    assert logger.warning_calls[-1][0] == "Unsupported cloud provider for container creation"


def test_delete_container_routes_and_unsupported(trans, logger):
    trans._azure_delete_container = Mock(return_value=None)
    trans._s3_delete_bucket = Mock(return_value=None)
    trans._gcs_delete_bucket = Mock(return_value=None)

    assert trans.delete_container(" azure ", "c", side="target") is True
    trans._azure_delete_container.assert_called_once_with("c", "target")

    assert trans.delete_container("AWS", "b") is True
    trans._s3_delete_bucket.assert_called_once_with("b")

    assert trans.delete_container("gcp", "g") is True
    trans._gcs_delete_bucket.assert_called_once_with("g")

    assert trans.delete_container("oracle", "x") is False
    assert logger.warning_calls[-1][0] == "Unsupported cloud provider for container deletion"


def test_source_file_info_gcp_route(trans):
    trans._gcs_list_files = Mock(return_value=["g.csv"])

    assert trans.source_file_info(" gcp ") == ["g.csv"]


def test_file_copy_gcp_route(module, trans):
    gcp_result = module.CopyResult(copied=True, skipped=False, reason="created")
    trans._gcs_copy_file = Mock(return_value=gcp_result)

    assert trans.file_copy(" gcp ", "a.csv") is gcp_result
    trans._gcs_copy_file.assert_called_once_with("a.csv", "a.csv")


def test_file_move_gcp_route(trans):
    trans._gcs_move_file = Mock(return_value=True)

    assert trans.file_move(" gcp ", "a.csv", dest_file_name="b.csv") is True
    trans._gcs_move_file.assert_called_once_with("a.csv", "b.csv")


def test_data_read_gcp_route(trans):
    trans.source_blob_name = "a.csv"
    trans._gcs_read_file = Mock(return_value="gcp-data")

    assert trans.data_read(" gcp ") == "gcp-data"
    trans._gcs_read_file.assert_called_once_with("a.csv")


def test_azure_ensure_container_created_exists_and_error(trans, logger):
    src_client = FakeBlobServiceClient()
    tgt_client = FakeBlobServiceClient()
    trans._azure_src_client = src_client
    trans._azure_tgt_client = tgt_client

    assert trans._azure_ensure_container("new-container", "source") is True
    assert src_client.create_container_calls == ["new-container"]
    assert logger.info_calls[-1][0] == "Azure container created"

    # side="target" routes to the target client instead of the source one.
    assert trans._azure_ensure_container("tgt-new", "target") is True
    assert tgt_client.create_container_calls == ["tgt-new"]

    # Already exists -> ResourceExistsError swallowed, still returns True.
    src_client.create_container_errors["present"] = FakeResourceExistsError("exists")
    assert trans._azure_ensure_container("present", "source") is True

    # Any other AzureError -> logged and re-raised.
    src_client.create_container_errors["boom"] = FakeAzureError("boom")
    with pytest.raises(FakeAzureError):
        trans._azure_ensure_container("boom", "source")
    assert logger.error_calls[-1][0] == "Azure container creation failed"


def test_azure_delete_container_deleted_missing_and_error(trans, logger):
    src_client = FakeBlobServiceClient()
    tgt_client = FakeBlobServiceClient()
    trans._azure_src_client = src_client
    trans._azure_tgt_client = tgt_client

    trans._azure_delete_container("gone-container", "source")
    assert src_client.delete_container_calls == ["gone-container"]
    assert logger.info_calls[-1][0] == "Azure container deleted"

    # side="target" routes to the target client instead of the source one.
    trans._azure_delete_container("tgt-gone", "target")
    assert tgt_client.delete_container_calls == ["tgt-gone"]

    # Already gone -> ResourceNotFoundError swallowed silently.
    src_client.delete_container_errors["already-gone"] = FakeResourceNotFoundError("missing")
    trans._azure_delete_container("already-gone", "source")  # no raise

    # Any other AzureError -> logged and re-raised.
    src_client.delete_container_errors["boom"] = FakeAzureError("boom")
    with pytest.raises(FakeAzureError):
        trans._azure_delete_container("boom", "source")
    assert logger.error_calls[-1][0] == "Azure container deletion failed"


def test_s3_ensure_bucket_already_exists(trans):
    s3 = FakeS3Client()
    trans._s3_client = s3

    assert trans._s3_ensure_bucket("present-bucket") is True
    assert s3.head_bucket_calls == ["present-bucket"]
    assert s3.create_bucket_calls == []


def test_s3_ensure_bucket_head_bucket_other_error_raises(trans, logger):
    s3 = FakeS3Client()
    s3.head_bucket_error = FakeClientError("403")
    trans._s3_client = s3

    with pytest.raises(FakeClientError):
        trans._s3_ensure_bucket("forbidden-bucket")
    assert logger.error_calls[-1][0] == "S3/MinIO bucket check failed"
    assert s3.create_bucket_calls == []


def test_s3_ensure_bucket_creates_with_location_constraint(trans):
    # trans fixture uses aws_region="eu-west-2" (!= "us-east-1"), so the
    # LocationConstraint branch is exercised here.
    s3 = FakeS3Client()
    s3.head_bucket_error = FakeClientError("404")
    trans._s3_client = s3

    assert trans._s3_ensure_bucket("new-bucket") is True
    assert s3.create_bucket_calls == [
        {
            "Bucket": "new-bucket",
            "CreateBucketConfiguration": {"LocationConstraint": "eu-west-2"},
        }
    ]


def test_s3_ensure_bucket_creates_without_location_constraint_for_us_east_1(module, logger):
    transaction = module.DataTransection(
        "src", "src-bucket", "tgt-bucket", None, None, "tgt",
        aws_region="us-east-1",
        logger=logger,
    )
    s3 = FakeS3Client()
    s3.head_bucket_error = FakeClientError("NoSuchBucket")
    transaction._s3_client = s3

    assert transaction._s3_ensure_bucket("new-bucket") is True
    assert s3.create_bucket_calls == [{"Bucket": "new-bucket"}]


def test_s3_ensure_bucket_already_owned_by_you(trans):
    s3 = FakeS3Client()
    s3.head_bucket_error = FakeClientError("404")
    s3.create_bucket_error = FakeClientError("BucketAlreadyOwnedByYou")
    trans._s3_client = s3

    assert trans._s3_ensure_bucket("race-bucket") is True


def test_s3_ensure_bucket_create_bucket_other_error_raises(trans, logger):
    s3 = FakeS3Client()
    s3.head_bucket_error = FakeClientError("404")
    s3.create_bucket_error = FakeClientError("500")
    trans._s3_client = s3

    with pytest.raises(FakeClientError):
        trans._s3_ensure_bucket("boom-bucket")
    assert logger.error_calls[-1][0] == "S3/MinIO bucket creation failed"


def test_s3_delete_bucket_purges_objects_and_deletes(trans):
    s3 = FakeS3Client()
    s3.paginator = FakePaginator(
        [
            {"Contents": [{"Key": "a.csv"}, {"Key": "b.csv"}]},
            {},  # empty page -> no delete_objects call for it
        ]
    )
    trans._s3_client = s3

    trans._s3_delete_bucket("doomed-bucket")

    assert s3.delete_objects_calls == [
        {
            "Bucket": "doomed-bucket",
            "Delete": {"Objects": [{"Key": "a.csv"}, {"Key": "b.csv"}]},
        }
    ]
    assert s3.delete_bucket_calls == ["doomed-bucket"]


def test_s3_delete_bucket_already_gone_and_other_error(trans, logger):
    s3 = FakeS3Client()
    s3.delete_bucket_error = FakeClientError("NoSuchBucket")
    trans._s3_client = s3

    trans._s3_delete_bucket("already-gone")  # no raise

    s3.delete_bucket_error = FakeClientError("500")
    with pytest.raises(FakeClientError):
        trans._s3_delete_bucket("boom-bucket")
    assert logger.error_calls[-1][0] == "S3/MinIO bucket deletion failed"


def test_get_gcs_client_caches_and_builds_credentials_from_service_account(
    module, logger, monkeypatch
):
    import google.cloud.storage as real_gcs_storage
    from google.oauth2 import service_account as real_service_account

    created_clients = []

    class FakeClientCtor:
        def __init__(self, project=None, credentials=None):
            self.project = project
            self.credentials = credentials
            created_clients.append(self)

    monkeypatch.setattr(real_gcs_storage, "Client", FakeClientCtor)

    fake_credentials = object()
    from_info_mock = Mock(return_value=fake_credentials)
    monkeypatch.setattr(
        real_service_account.Credentials, "from_service_account_info", from_info_mock
    )

    no_sa_trans = module.DataTransection(
        "src", "src-bucket", "tgt-bucket", None, None, "tgt",
        gcp_service_account_info=None,
        gcp_project_id="proj-1",
        logger=logger,
    )
    client1 = no_sa_trans._get_gcs_client()
    client1_again = no_sa_trans._get_gcs_client()

    assert client1 is client1_again  # cached, only built once
    assert client1.project == "proj-1"
    assert client1.credentials is None
    from_info_mock.assert_not_called()

    sa_info = {"type": "service_account", "client_email": "x@y.iam"}
    sa_trans = module.DataTransection(
        "src", "src-bucket", "tgt-bucket", None, None, "tgt",
        gcp_service_account_info=sa_info,
        gcp_project_id="proj-2",
        logger=logger,
    )
    client2 = sa_trans._get_gcs_client()

    from_info_mock.assert_called_once_with(sa_info)
    assert client2.credentials is fake_credentials
    assert client2.project == "proj-2"
    assert len(created_clients) == 2


def test_gcs_list_files_success_and_error(trans, logger):
    client = FakeGCSClient()
    bucket = client.bucket("src-container")
    bucket.existing_blobs = {
        "a.csv": FakeGCSBlobInfo("a.csv"),
        "b.csv": FakeGCSBlobInfo("b.csv"),
    }
    trans._gcs_client = client

    assert sorted(trans._gcs_list_files()) == ["a.csv", "b.csv"]

    bucket.list_error = GoogleAPIError("list failed")
    with pytest.raises(GoogleAPIError):
        trans._gcs_list_files()
    assert logger.error_calls[-1][0] == "GCS list objects failed"


def test_gcs_list_files_with_timestamps_source_and_target(trans, logger):
    src_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    tgt_ts = datetime(2026, 1, 2, tzinfo=timezone.utc)

    client = FakeGCSClient()
    src_bucket = client.bucket("src-container")
    src_bucket.existing_blobs = {
        "a.csv": FakeGCSBlobInfo("a.csv", updated=src_ts),
        "no-ts.csv": FakeGCSBlobInfo("no-ts.csv"),
    }
    trans._gcs_client = client

    assert trans._gcs_list_files_with_timestamps("source") == {
        "a.csv": src_ts,
        "no-ts.csv": None,
    }

    tgt_bucket = client.bucket("tgt-container")
    tgt_bucket.existing_blobs = {"a.csv": FakeGCSBlobInfo("a.csv", updated=tgt_ts)}
    assert trans._gcs_list_files_with_timestamps("target") == {"a.csv": tgt_ts}


def test_gcs_list_files_with_timestamps_error(trans, logger):
    client = FakeGCSClient()
    bucket = client.bucket("src-container")
    bucket.list_error = GoogleAPIError("list failed")
    trans._gcs_client = client

    with pytest.raises(GoogleAPIError):
        trans._gcs_list_files_with_timestamps("source")
    assert logger.error_calls[-1][0] == "GCS list objects (with timestamps) failed"


def test_gcs_get_last_modified_success_missing_and_no_timestamp(trans):
    aware = datetime(2026, 1, 1, tzinfo=timezone.utc)

    client = FakeGCSClient()
    bucket = client.bucket("bucket")
    bucket.existing_blobs["exists.csv"] = FakeGCSBlobInfo("exists.csv", updated=aware)
    bucket.existing_blobs["no-timestamp.csv"] = FakeGCSBlobInfo("no-timestamp.csv", updated=None)
    trans._gcs_client = client

    assert trans._gcs_get_last_modified("bucket", "exists.csv") == aware
    assert trans._gcs_get_last_modified("bucket", "no-timestamp.csv") is None
    assert trans._gcs_get_last_modified("bucket", "missing.csv") is None


def test_gcs_copy_source_missing_skip_created_overwritten_and_error(trans, logger):
    older = datetime(2026, 1, 1, tzinfo=timezone.utc)
    newer = datetime(2026, 1, 2, tzinfo=timezone.utc)

    client = FakeGCSClient()
    trans._gcs_client = client

    result = trans._gcs_copy_file("missing.csv", "missing.csv")
    assert result.copied is False
    assert result.skipped is False
    assert result.reason == "source object not found"
    assert logger.error_calls[-1][0] == "GCS copy aborted – source object not found"

    src_bucket = client.bucket("src-container")
    tgt_bucket = client.bucket("tgt-container")

    src_bucket.existing_blobs["a.csv"] = FakeGCSBlobInfo("a.csv", updated=older)
    tgt_bucket.existing_blobs["a.csv"] = FakeGCSBlobInfo("a.csv", updated=newer)
    result = trans._gcs_copy_file("a.csv", "a.csv")
    assert result.skipped is True
    assert result.reason.startswith("target up-to-date")

    src_bucket.existing_blobs["b.csv"] = FakeGCSBlobInfo("b.csv", updated=newer)
    result = trans._gcs_copy_file("b.csv", "b.csv")
    assert result.copied is True
    assert result.reason == "created"
    assert src_bucket.copy_calls[-1] == ("b.csv", "tgt-container", "b.csv")

    src_bucket.existing_blobs["c.csv"] = FakeGCSBlobInfo("c.csv", updated=newer)
    tgt_bucket.existing_blobs["c.csv"] = FakeGCSBlobInfo("c.csv", updated=older)
    result = trans._gcs_copy_file("c.csv", "c.csv")
    assert result.copied is True
    assert result.reason == "overwritten"

    src_bucket.existing_blobs["d.csv"] = FakeGCSBlobInfo("d.csv", updated=newer)
    src_bucket.copy_error = GoogleAPIError("copy failed")
    with pytest.raises(GoogleAPIError):
        trans._gcs_copy_file("d.csv", "d.csv")
    assert logger.error_calls[-1][0] == "GCS object copy failed"


def test_gcs_move_success_and_error(trans, logger):
    client = FakeGCSClient()
    trans._gcs_client = client

    assert trans._gcs_move_file("a.csv", "b.csv") is True
    src_bucket = client.bucket("src-container")
    assert src_bucket.copy_calls[-1] == ("a.csv", "tgt-container", "b.csv")
    assert src_bucket.deleted_blobs == ["a.csv"]

    src_bucket.copy_error = GoogleAPIError("move failed")
    with pytest.raises(GoogleAPIError):
        trans._gcs_move_file("x.csv", "y.csv")
    assert logger.error_calls[-1][0] == "GCS object move failed"


def test_gcs_read_success_and_error(trans, logger, monkeypatch):
    client = FakeGCSClient()
    trans._gcs_client = client
    bucket = client.bucket("src-container")

    read_blob = types.SimpleNamespace(
        download_as_bytes=Mock(return_value=b"hello")
    )
    bucket.blob = Mock(return_value=read_blob)

    assert trans._gcs_read_file("a.csv") == b"hello"

    error_blob = types.SimpleNamespace(
        download_as_bytes=Mock(side_effect=GoogleAPIError("read failed"))
    )
    bucket.blob = Mock(return_value=error_blob)

    with pytest.raises(GoogleAPIError):
        trans._gcs_read_file("bad.csv")
    assert logger.error_calls[-1][0] == "GCS object read failed"


def test_gcs_ensure_bucket_created_exists_and_error(trans, logger):
    client = FakeGCSClient()
    trans._gcs_client = client

    assert trans._gcs_ensure_bucket("new-bucket") is True
    assert client.created_buckets == ["new-bucket"]
    assert logger.info_calls[-1][0] == "GCS bucket created"

    client.create_bucket_error = Conflict("already exists")
    assert trans._gcs_ensure_bucket("present-bucket") is True  # swallowed

    client.create_bucket_error = GoogleAPIError("boom")
    with pytest.raises(GoogleAPIError):
        trans._gcs_ensure_bucket("boom-bucket")
    assert logger.error_calls[-1][0] == "GCS bucket creation failed"


def test_gcs_bucket_exists_true_false_and_error(trans, logger):
    client = FakeGCSClient()
    trans._gcs_client = client

    client.bucket("present").exists_value = True
    assert trans._gcs_bucket_exists("present") is True

    client.bucket("absent").exists_value = False
    assert trans._gcs_bucket_exists("absent") is False

    client.bucket("boom").exists_error = GoogleAPIError("boom")
    with pytest.raises(GoogleAPIError):
        trans._gcs_bucket_exists("boom")
    assert logger.error_calls[-1][0] == "GCS bucket existence check failed"


def test_gcs_delete_bucket_deleted_missing_and_error(trans, logger):
    client = FakeGCSClient()
    trans._gcs_client = client

    trans._gcs_delete_bucket("doomed-bucket")
    assert client.bucket("doomed-bucket").delete_force_calls == [True]
    assert logger.info_calls[-1][0] == "GCS bucket deleted"

    client.bucket("already-gone").delete_error = NotFound("missing")
    trans._gcs_delete_bucket("already-gone")  # no raise

    client.bucket("boom").delete_error = GoogleAPIError("boom")
    with pytest.raises(GoogleAPIError):
        trans._gcs_delete_bucket("boom")
    assert logger.error_calls[-1][0] == "GCS bucket deletion failed"


def test_gcp_credentials_from_env_empty_and_decoded(module):
    assert module.gcp_credentials_from_env("") is None
    assert module.gcp_credentials_from_env(None) is None

    import base64
    import json

    raw = {"type": "service_account", "project_id": "proj-1"}
    encoded = base64.b64encode(json.dumps(raw).encode("utf-8")).decode("utf-8")

    assert module.gcp_credentials_from_env(encoded) == raw