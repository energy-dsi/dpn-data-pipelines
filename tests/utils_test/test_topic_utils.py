import pytest
from unittest.mock import MagicMock

from utils.topic_utils import TopicResolver, KafkaTopicManager


# ----------------------------------------------------------------------
# TopicResolver
# ----------------------------------------------------------------------

def test_resolve_with_provided():
    result = TopicResolver.resolve("custom-topic", "src", "trfm")
    assert result == "custom-topic"


def test_resolve_with_empty_string():
    result = TopicResolver.resolve("", "src-topic", "trfm")
    assert result == "src-topic-trfm"


def test_resolve_with_none():
    result = TopicResolver.resolve(None, "src-topic", "trfm")
    assert result == "src-topic-trfm"


def test_resolve_with_spaces():
    result = TopicResolver.resolve("   ", "src-topic", "trfm")
    assert result == "src-topic-trfm"


# ----------------------------------------------------------------------
# KafkaTopicManager
# ----------------------------------------------------------------------

@pytest.fixture
def mock_logger():
    return MagicMock()


@pytest.fixture
def manager(monkeypatch, mock_logger):
    mock_admin = MagicMock()

    monkeypatch.setattr(
        "utils.topic_utils.AdminClient",
        lambda *a, **k: mock_admin,
    )

    mgr = KafkaTopicManager("localhost:9092", mock_logger)
    mgr._admin = mock_admin

    return mgr


# ✅ Topic already exists
def test_topic_already_exists(manager, mock_logger):
    manager._admin.list_topics.return_value.topics = {"my-topic": None}

    manager.ensure_exists("my-topic")

    mock_logger.info.assert_called()


# ✅ Topic creation success
def test_topic_create_success(manager, mock_logger):
    manager._admin.list_topics.return_value.topics = {}

    mock_future = MagicMock()
    mock_future.result.return_value = None

    manager._admin.create_topics.return_value = {"new-topic": mock_future}

    manager.ensure_exists("new-topic")

    mock_future.result.assert_called()
    mock_logger.info.assert_called()


# ✅ Topic creation exception (IMPORTANT branch)
def test_topic_create_exception(manager, mock_logger):
    manager._admin.list_topics.return_value.topics = {}

    mock_future = MagicMock()
    mock_future.result.side_effect = Exception("already exists")

    manager._admin.create_topics.return_value = {"new-topic": mock_future}

    manager.ensure_exists("new-topic")

    # ✅ THIS LINE COVERS exception branch
    mock_logger.warning.assert_called()


# ----------------------------------------------------------------------
# KafkaTopicManager.exists — pure check, never creates
# ----------------------------------------------------------------------

def test_exists_true(manager):
    manager._admin.list_topics.return_value.topics = {"my-topic": None}

    assert manager.exists("my-topic") is True
    manager._admin.create_topics.assert_not_called()


def test_exists_false(manager):
    manager._admin.list_topics.return_value.topics = {"other-topic": None}

    assert manager.exists("my-topic") is False
    manager._admin.create_topics.assert_not_called()


# ----------------------------------------------------------------------
# KafkaTopicManager.delete_topic — teardown counterpart to ensure_exists
# ----------------------------------------------------------------------

def test_delete_topic_already_absent_is_a_noop(manager, mock_logger):
    manager._admin.list_topics.return_value.topics = {"other-topic": None}

    manager.delete_topic("my-topic")

    manager._admin.delete_topics.assert_not_called()
    mock_logger.info.assert_called()


def test_delete_topic_success(manager, mock_logger):
    manager._admin.list_topics.return_value.topics = {"my-topic": None}

    mock_future = MagicMock()
    mock_future.result.return_value = None
    manager._admin.delete_topics.return_value = {"my-topic": mock_future}

    manager.delete_topic("my-topic")

    manager._admin.delete_topics.assert_called_once_with(["my-topic"])
    mock_future.result.assert_called()
    mock_logger.info.assert_called()


def test_delete_topic_exception_is_logged_not_raised(manager, mock_logger):
    manager._admin.list_topics.return_value.topics = {"my-topic": None}

    mock_future = MagicMock()
    mock_future.result.side_effect = Exception("UNKNOWN_TOPIC_OR_PARTITION")
    manager._admin.delete_topics.return_value = {"my-topic": mock_future}

    manager.delete_topic("my-topic")  # must not raise

    mock_logger.warning.assert_called()
