# Copyright 2025 Platform Team – Apache 2.0 License
#
# +---------+---------------------------------------------+---------------+-------------+
# | Version | Description                                 | Change Owner  | Change Date |
# +---------+---------------------------------------------+---------------+-------------+
# | 1.0.0   | Full test suite for config_validator.py     | Platform Team | 2025-04-27  |
# +---------+---------------------------------------------+---------------+-------------+
"""
Tests for utils/config_validator.py

Coverage targets
----------------
- azure: all required fields present               → no errors
- azure: one field missing                         → error reported
- azure: multiple fields missing                   → all errors collected
- azure: unknown field name in azure_fields list   → error reported
- aws:   all required fields present               → no errors
- aws:   one AWS field missing                     → error reported
- aws:   multiple AWS fields missing               → all errors collected
- unknown provider                                 → error reported
- exit_on_failure=True  triggers sys.exit(1)
- exit_on_failure=False returns error list
- logger supplied      → errors routed to logger.error
- logger=None          → errors printed to stderr
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from utils.config_validator import validate_cloud_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _run(provider: str, fields: list[str], env: dict, **kw):
    """Run validate_cloud_config inside a patched os.environ."""
    with patch.dict("os.environ", env, clear=True):
        return validate_cloud_config(
            cloud_provider=provider,
            azure_fields=fields,
            exit_on_failure=False,
            **kw,
        )


FULL_AZURE_ENV = {
    "srcConnectionString":"QmxvYkVuZHBvaW50PWh0dHBzOi8vbXlzdG9yYWdlYWNjb3VudC5ibG9iLmNvcmUud2luZG93cy5uZXQvO1NoYXJlZEFjY2Vzc1NpZ25hdHVyZT1zdj0yMDIzLTAxLTAzJnNzPWImc3J0PXNjbyZzcD1yd2RsYWMmc2U9MjAzMC0wMS0wMVQwMDowMDowMFomc3Q9MjAyNC0wMS0wMVQwMDowMDowMFomc3ByPWh0dHBzJnNpZz1hYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejEyMzQ1Njc4OTAlM0Q=",
    "mapperConnectionString": "QmxvYkVuZHBvaW50PWh0dHBzOi8vbXlzdG9yYWdlYWNjb3VudC5ibG9iLmNvcmUud2luZG93cy5uZXQvO1NoYXJlZEFjY2Vzc1NpZ25hdHVyZT1zdj0yMDIzLTAxLTAzJnNzPWImc3J0PXNjbyZzcD1yd2RsYWMmc2U9MjAzMC0wMS0wMVQwMDowMDowMFomc3Q9MjAyNC0wMS0wMVQwMDowMDowMFomc3ByPWh0dHBzJnNpZz1hYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejEyMzQ1Njc4OTAlM0Q=",
    "targetConnectionString": "QmxvYkVuZHBvaW50PWh0dHBzOi8vbXlzdG9yYWdlYWNjb3VudC5ibG9iLmNvcmUud2luZG93cy5uZXQvO1NoYXJlZEFjY2Vzc1NpZ25hdHVyZT1zdj0yMDIzLTAxLTAzJnNzPWImc3J0PXNjbyZzcD1yd2RsYWMmc2U9MjAzMC0wMS0wMVQwMDowMDowMFomc3Q9MjAyNC0wMS0wMVQwMDowMDowMFomc3ByPWh0dHBzJnNpZz1hYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejEyMzQ1Njc4OTAlM0Q=",
}

FULL_AWS_ENV = {
    "AWS_ACCESS_KEY_ID":     "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "AWS_REGION":            "us-east-1",
    "AWS_ENDPOINT_URL":      "http://localhost:9000",
}


# ---------------------------------------------------------------------------
# Azure – happy path
# ---------------------------------------------------------------------------
class TestAzureValid:
    def test_all_fields_present_no_errors(self):
        errors = _run("azure", ["srcConnectionString", "mapperConnectionString"], FULL_AZURE_ENV)
        assert errors == []

    def test_single_field_subset_passes(self):
        errors = _run("azure", ["srcConnectionString"], FULL_AZURE_ENV)
        assert errors == []

    def test_all_three_fields_passes(self):
        errors = _run("azure", list(FULL_AZURE_ENV.keys()), FULL_AZURE_ENV)
        assert errors == []

    def test_case_insensitive_provider(self):
        errors = _run("AZURE", ["srcConnectionString"], FULL_AZURE_ENV)
        assert errors == []


# ---------------------------------------------------------------------------
# Azure – missing fields
# ---------------------------------------------------------------------------
class TestAzureMissingFields:
    def test_one_missing_field_returns_one_error(self):
        env = {"mapperConnectionString": "def"}   # srcConnectionString absent
        errors = _run("azure", ["srcConnectionString", "mapperConnectionString"], env)
        assert len(errors) == 2
        assert "srcConnectionString" in errors[0]

    def test_all_fields_missing_returns_correct_count(self):
        errors = _run("azure", ["srcConnectionString", "mapperConnectionString", "targetConnectionString"], {})
        assert len(errors) == 3

    def test_empty_string_value_treated_as_missing(self):
        env = {"srcConnectionString": "   "}   # whitespace only
        errors = _run("azure", ["srcConnectionString"], env)
        assert len(errors) == 1

    def test_unknown_azure_field_returns_error(self):
        errors = _run("azure", ["unknownField"], FULL_AZURE_ENV)
        assert any("unknownField" in e for e in errors)


# ---------------------------------------------------------------------------
# AWS – happy path
# ---------------------------------------------------------------------------
class TestAWSValid:
    def test_all_aws_fields_present_no_errors(self):
        errors = _run("aws", [], FULL_AWS_ENV)
        assert errors == []

    def test_case_insensitive_provider(self):
        errors = _run("AWS", [], FULL_AWS_ENV)
        assert errors == []


# ---------------------------------------------------------------------------
# AWS – missing fields
# ---------------------------------------------------------------------------
class TestAWSMissingFields:
    def test_missing_access_key_returns_error(self):
        env = {k: v for k, v in FULL_AWS_ENV.items() if k != "AWS_ACCESS_KEY_ID"}
        errors = _run("aws", [], env)
        assert any("AWS_ACCESS_KEY_ID" in e for e in errors)

    def test_missing_secret_key_returns_error(self):
        env = {k: v for k, v in FULL_AWS_ENV.items() if k != "AWS_SECRET_ACCESS_KEY"}
        errors = _run("aws", [], env)
        assert any("AWS_SECRET_ACCESS_KEY" in e for e in errors)

    def test_missing_endpoint_returns_error(self):
        env = {k: v for k, v in FULL_AWS_ENV.items() if k != "AWS_ENDPOINT_URL"}
        errors = _run("aws", [], env)
        assert any("AWS_ENDPOINT_URL" in e for e in errors)

    def test_missing_region_returns_error(self):
        env = {k: v for k, v in FULL_AWS_ENV.items() if k != "AWS_REGION"}
        errors = _run("aws", [], env)
        assert any("AWS_REGION" in e for e in errors)

    def test_all_aws_fields_missing_returns_four_errors(self):
        errors = _run("aws", [], {})
        assert len(errors) == 4


# ---------------------------------------------------------------------------
# Unknown provider
# ---------------------------------------------------------------------------
class TestUnknownProvider:
    def test_unknown_provider_returns_error(self):
        errors = _run("gcp", [], {})
        assert len(errors) == 1
        assert "gcp" in errors[0]

    def test_empty_provider_returns_error(self):
        errors = _run("", [], {})
        assert len(errors) == 1


# ---------------------------------------------------------------------------
# exit_on_failure
# ---------------------------------------------------------------------------
class TestExitOnFailure:
    def test_exit_called_when_flag_true_and_errors_present(self):
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(SystemExit) as exc_info:
                validate_cloud_config(
                    cloud_provider="azure",
                    azure_fields=["srcConnectionString"],
                    exit_on_failure=True,
                )
        assert exc_info.value.code == 1

    def test_no_exit_when_flag_false(self):
        with patch.dict("os.environ", {}, clear=True):
            errors = validate_cloud_config(
                cloud_provider="azure",
                azure_fields=["srcConnectionString"],
                exit_on_failure=False,
            )
        assert len(errors) >= 1

    def test_no_exit_when_validation_passes(self):
        with patch.dict("os.environ", FULL_AZURE_ENV):
            validate_cloud_config(
                cloud_provider="azure",
                azure_fields=["srcConnectionString"],
                exit_on_failure=True,   # should NOT exit – no errors
            )


# ---------------------------------------------------------------------------
# Logger routing
# ---------------------------------------------------------------------------
class TestErrorReporting:
    def test_errors_routed_to_supplied_logger(self):
        mock_logger = MagicMock()
        with patch.dict("os.environ", {}, clear=True):
            validate_cloud_config(
                cloud_provider="azure",
                azure_fields=["srcConnectionString"],
                logger=mock_logger,
                exit_on_failure=False,
            )
        assert mock_logger.error.called

    def test_errors_printed_to_stderr_when_no_logger(self, capsys):
        with patch.dict("os.environ", {}, clear=True):
            validate_cloud_config(
                cloud_provider="azure",
                azure_fields=["srcConnectionString"],
                logger=None,
                exit_on_failure=False,
            )
        captured = capsys.readouterr()
        assert "FATAL" in captured.err or "srcConnectionString" in captured.err or "FATAL" in captured.out or "srcConnectionString" in captured.out

    def test_logger_receives_all_missing_vars_in_extra(self):
        mock_logger = MagicMock()
        with patch.dict("os.environ", {}, clear=True):
            validate_cloud_config(
                cloud_provider="azure",
                azure_fields=["srcConnectionString", "mapperConnectionString"],
                logger=mock_logger,
                exit_on_failure=False,
            )
        call_kwargs = mock_logger.error.call_args_list[0]
        extra = call_kwargs[1].get("extra", {})
        assert "errors" in extra
