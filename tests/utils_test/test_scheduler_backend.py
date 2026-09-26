import pytest
from unittest.mock import MagicMock, patch

from utils.scheduler_backend import (
    _resolve_execution_mode,
    _build_context,
    AirflowBackend,
    IntervalBackend,
    StandaloneBackend,
    get_backend,
)


# ----------------------------------------------------------------------
# _resolve_execution_mode
# ----------------------------------------------------------------------

def test_execution_mode_manual(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "manual")
    assert _resolve_execution_mode() == "manual"


def test_execution_mode_default(monkeypatch):
    monkeypatch.delenv("EXECUTION_MODE", raising=False)
    assert _resolve_execution_mode() == "automatic"


def test_execution_mode_invalid(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "random")
    assert _resolve_execution_mode() == "automatic"


# ----------------------------------------------------------------------
# _build_context
# ----------------------------------------------------------------------

def test_build_context_defaults(monkeypatch):
    monkeypatch.delenv("EXECUTION_MODE", raising=False)

    ctx = _build_context(
        triggered_by="standalone",
        pipeline_stage="stage",
        pipeline_type="type",
        pipeline_role="role",
    )

    assert ctx.run_id is not None
    assert ctx.mode == "automatic"
    assert ctx.triggered_by == "standalone"


def test_build_context_manual(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "manual")

    ctx = _build_context(
        triggered_by="interval",
        pipeline_stage="stage",
        pipeline_type="type",
        pipeline_role="role",
        run_id="123",
        dag_id="dag",
        dag_run_id="run",
    )

    assert ctx.mode == "manual"
    assert ctx.run_id == "123"
    assert ctx.dag_id == "dag"


# ----------------------------------------------------------------------
# AirflowBackend
# ----------------------------------------------------------------------

def test_airflow_execute_runs(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "automatic")

    mock_run = MagicMock()

    dag_run = MagicMock(run_id="run1")
    dag = MagicMock(dag_id="dag1")

    backend = AirflowBackend({"dag_run": dag_run, "dag": dag})

    backend.execute(
        mock_run,
        pipeline_stage="stage",
        pipeline_type="type",
        pipeline_role="role",
    )

    assert mock_run.called


def test_airflow_execute_manual_skip(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "manual")

    mock_run = MagicMock()
    backend = AirflowBackend({})

    backend.execute(
        mock_run,
        pipeline_stage="stage",
        pipeline_type="type",
        pipeline_role="role",
    )

    mock_run.assert_not_called()


def test_airflow_execute_missing_context(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "automatic")

    mock_run = MagicMock()
    backend = AirflowBackend({})

    backend.execute(
        mock_run,
        pipeline_stage="stage",
        pipeline_type="type",
        pipeline_role="role",
    )

    assert mock_run.called


# ----------------------------------------------------------------------
# StandaloneBackend
# ----------------------------------------------------------------------

def test_standalone_execute_runs(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "automatic")

    mock_run = MagicMock()
    backend = StandaloneBackend()

    backend.execute(
        mock_run,
        pipeline_stage="stage",
        pipeline_type="type",
        pipeline_role="role",
    )

    assert mock_run.called


def test_standalone_execute_manual_skip(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "manual")

    mock_run = MagicMock()
    backend = StandaloneBackend()

    backend.execute(
        mock_run,
        pipeline_stage="stage",
        pipeline_type="type",
        pipeline_role="role",
    )

    mock_run.assert_not_called()


# ----------------------------------------------------------------------
# IntervalBackend
# ----------------------------------------------------------------------

def test_interval_execute_runs_once(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "automatic")

    mock_run = MagicMock()

    backend = IntervalBackend(interval_seconds=1)

    # Patch infinite loop + scheduler
    monkeypatch.setattr("utils.scheduler_backend.schedule.every", lambda x: MagicMock(seconds=MagicMock(return_value=MagicMock(do=lambda fn: None))))
    monkeypatch.setattr("utils.scheduler_backend.schedule.run_pending", lambda: None)

    # stop loop after 1 iteration
    def stop_loop(x):
        raise KeyboardInterrupt()

    monkeypatch.setattr("utils.scheduler_backend.time.sleep", stop_loop)

    try:
        backend.execute(
            mock_run,
            pipeline_stage="stage",
            pipeline_type="type",
            pipeline_role="role",
        )
    except KeyboardInterrupt:
        pass

    assert mock_run.called


def test_interval_execute_manual_skip(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "manual")

    mock_run = MagicMock()
    backend = IntervalBackend(interval_seconds=1)

    monkeypatch.setattr("utils.scheduler_backend.schedule.every", lambda x: MagicMock(seconds=MagicMock(return_value=MagicMock(do=lambda fn: None))))
    monkeypatch.setattr("utils.scheduler_backend.schedule.run_pending", lambda: None)

    def stop_loop(x):
        raise KeyboardInterrupt()

    monkeypatch.setattr("utils.scheduler_backend.time.sleep", stop_loop)

    try:
        backend.execute(
            mock_run,
            pipeline_stage="stage",
            pipeline_type="type",
            pipeline_role="role",
        )
    except KeyboardInterrupt:
        pass

    mock_run.assert_not_called()


# ----------------------------------------------------------------------
# get_backend (Factory)
# ----------------------------------------------------------------------

def test_get_backend_default(monkeypatch):
    monkeypatch.delenv("SCHEDULER_BACKEND", raising=False)

    backend = get_backend()
    assert isinstance(backend, StandaloneBackend)


def test_get_backend_airflow(monkeypatch):
    monkeypatch.setenv("SCHEDULER_BACKEND", "airflow")

    backend = get_backend(airflow_context={})
    assert isinstance(backend, AirflowBackend)


def test_get_backend_airflow_missing_context(monkeypatch):
    monkeypatch.setenv("SCHEDULER_BACKEND", "airflow")

    with pytest.raises(ValueError):
        get_backend()


def test_get_backend_interval(monkeypatch):
    monkeypatch.setenv("SCHEDULER_BACKEND", "interval")

    backend = get_backend(interval_seconds=5)
    assert isinstance(backend, IntervalBackend)


def test_get_backend_kafka(monkeypatch):
    monkeypatch.setenv("SCHEDULER_BACKEND", "kafka-trigger")

    with patch("utils.kafka_trigger.KafkaTriggerBackend") as mock:
        backend = get_backend()
        assert mock.called


def test_get_backend_fallback(monkeypatch):
    monkeypatch.setenv("SCHEDULER_BACKEND", "unknown")

    backend = get_backend()
    assert isinstance(backend, StandaloneBackend)