import pytest
from utils.pipeline_context import PipelineContext


# ----------------------------------------------------------------------
# Default initialization
# ----------------------------------------------------------------------

def test_pipeline_context_defaults():
    ctx = PipelineContext()

    assert ctx.run_id is not None
    assert ctx.mode == "automatic"
    assert ctx.triggered_by == "standalone"
    assert ctx.pipeline_stage == "unknown"
    assert ctx.pipeline_type == "file"
    assert ctx.pipeline_role == "producer"
    assert ctx.started_at is not None


# ----------------------------------------------------------------------
# as_log_extra
# ----------------------------------------------------------------------

def test_as_log_extra():
    ctx = PipelineContext(
        run_id="123",
        mode="manual",
        triggered_by="airflow",
        pipeline_stage="adaptor",
        pipeline_type="topic",
        pipeline_role="consumer",
        dag_run_id="run1",
        dag_id="dag1",
    )

    data = ctx.as_log_extra()

    assert data["pipeline.run_id"] == "123"
    assert data["pipeline.mode"] == "manual"
    assert data["pipeline.triggered_by"] == "airflow"
    assert data["pipeline.stage"] == "adaptor"
    assert data["pipeline.type"] == "topic"
    assert data["pipeline.role"] == "consumer"
    assert data["pipeline.dag_run_id"] == "run1"
    assert data["pipeline.dag_id"] == "dag1"
    assert "pipeline.started_at" in data


# ----------------------------------------------------------------------
# from_trigger_message
# ----------------------------------------------------------------------

def test_from_trigger_message_full_payload():
    payload = {
        "run_id": "run123",
        "dag_run_id": "dagrun123",
        "dag_id": "dag123",
        "execution_mode": "manual",
    }

    ctx = PipelineContext.from_trigger_message(
        payload,
        pipeline_stage="adaptor",
        pipeline_type="topic",
        pipeline_role="producer",
    )

    assert ctx.run_id == "run123"
    assert ctx.mode == "manual"
    assert ctx.triggered_by == "kafka-trigger"
    assert ctx.pipeline_stage == "adaptor"
    assert ctx.pipeline_type == "topic"
    assert ctx.pipeline_role == "producer"
    assert ctx.dag_run_id == "dagrun123"
    assert ctx.dag_id == "dag123"


def test_from_trigger_message_defaults():
    payload = {}

    ctx = PipelineContext.from_trigger_message(
        payload,
        pipeline_stage="extractor",
        pipeline_type="file",
        pipeline_role="consumer",
    )

    # ✅ fallback run_id generation
    assert ctx.run_id is not None
    assert ctx.mode == "automatic"
    assert ctx.dag_run_id == ""
    assert ctx.dag_id == ""
    assert ctx.triggered_by == "kafka-trigger"


def test_from_trigger_message_mode_normalization():
    payload = {"execution_mode": "  MANUAL  "}

    ctx = PipelineContext.from_trigger_message(
        payload,
        pipeline_stage="schema_mapper",
        pipeline_type="topic",
        pipeline_role="producer",
    )

    # ✅ normalization branch
    assert ctx.mode == "manual"


def test_from_trigger_message_non_manual_mode():
    payload = {"execution_mode": "random-value"}

    ctx = PipelineContext.from_trigger_message(
        payload,
        pipeline_stage="adaptor",
        pipeline_type="file",
        pipeline_role="producer",
    )

    # ✅ fallback branch
    assert ctx.mode == "automatic"