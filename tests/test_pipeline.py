"""Tests for src/pipeline.py — provider detection, deduplication, pipeline integration."""

from datetime import date, timedelta

import pandas as pd
import pytest

from src.pipeline import dedupe_leaks, detect_provider, run_pipeline_from_df


# ===================== detect_provider =====================

class TestDetectProvider:
    def test_aws_columns(self):
        df = pd.DataFrame(columns=["line_item_usage_account_id", "line_item_line_item_type", "cost"])
        assert detect_provider(df) == "AWS"

    def test_azure_columns(self):
        df = pd.DataFrame(columns=["SubscriptionId", "UsageDate", "MeterName"])
        assert detect_provider(df) == "AZURE"

    def test_gcp_columns(self):
        df = pd.DataFrame(columns=["billing_account_id", "project_id", "service_description"])
        assert detect_provider(df) == "GCP"

    def test_override_lowercases(self):
        df = pd.DataFrame(columns=["line_item_usage_account_id"])
        assert detect_provider(df, override="aws") == "AWS"

    def test_override_ignores_columns(self):
        # Even if columns look like Azure, override wins
        df = pd.DataFrame(columns=["SubscriptionId", "UsageDate", "MeterName"])
        assert detect_provider(df, override="gcp") == "GCP"

    def test_provider_column_present(self):
        df = pd.DataFrame({"provider": ["azure"]})
        assert detect_provider(df) == "AZURE"

    def test_ambiguous_raises(self):
        df = pd.DataFrame(columns=["some_unknown_column"])
        with pytest.raises(ValueError, match="Ambiguous or unknown"):
            detect_provider(df)

    def test_multiple_matched_raises(self):
        # Columns from both AWS and Azure — ambiguous
        df = pd.DataFrame(columns=[
            "line_item_usage_account_id",  # AWS signal
            "SubscriptionId",              # Azure signal
        ])
        with pytest.raises(ValueError):
            detect_provider(df)


# ===================== dedupe_leaks =====================

class TestDedupeLeaks:
    def _leak(self, leak_type="ZOMBIE_RESOURCE", provider="AWS", service="ec2", resource_id="i-001"):
        return {"leak_type": leak_type, "provider": provider, "service": service,
                "resource_id": resource_id}

    def test_removes_exact_duplicate(self):
        leaks = [self._leak(), self._leak()]
        assert len(dedupe_leaks(leaks)) == 1

    def test_keeps_different_types(self):
        leaks = [self._leak("ZOMBIE_RESOURCE"), self._leak("IDLE_RESOURCE")]
        assert len(dedupe_leaks(leaks)) == 2

    def test_keeps_different_resources(self):
        leaks = [self._leak(resource_id="i-001"), self._leak(resource_id="i-002")]
        assert len(dedupe_leaks(leaks)) == 2

    def test_keeps_different_services(self):
        leaks = [self._leak(service="ec2"), self._leak(service="rds")]
        assert len(dedupe_leaks(leaks)) == 2

    def test_preserves_first_occurrence(self):
        leak_a = {**self._leak(), "reason": "first"}
        leak_b = {**self._leak(), "reason": "second"}
        result = dedupe_leaks([leak_a, leak_b])
        assert result[0]["reason"] == "first"

    def test_empty_input(self):
        assert dedupe_leaks([]) == []

    def test_none_resource_ids_deduplicated(self):
        leak_a = {"leak_type": "RUNAWAY_COST", "provider": "AWS", "service": "ec2", "resource_id": None}
        leak_b = {"leak_type": "RUNAWAY_COST", "provider": "AWS", "service": "ec2", "resource_id": None}
        assert len(dedupe_leaks([leak_a, leak_b])) == 1


# ===================== run_pipeline_from_df =====================

class TestRunPipelineFromDf:
    def _make_df(self, n_days=20):
        """Pre-normalized DataFrame that can go straight to feature engineering."""
        end = date(2024, 3, 31)
        rows = []
        for i in range(n_days):
            d = end - timedelta(days=n_days - 1 - i)
            cost = 2.0 + i * 5  # runaway growth
            rows.append({"date": d, "provider": "AWS", "service": "ec2",
                         "cost": cost, "usage": 1.0, "resource_id": "i-test", "region": "us-east-1"})
        return pd.DataFrame(rows)

    def test_returns_expected_keys(self):
        result = run_pipeline_from_df(
            self._make_df(),
            provider="AWS",
            no_forecast=True,
            already_normalized=True,
        )
        assert set(result.keys()) >= {"summary", "leaks", "forecasts", "pipeline_stats"}

    def test_summary_has_numeric_waste(self):
        result = run_pipeline_from_df(
            self._make_df(),
            provider="AWS",
            no_forecast=True,
            already_normalized=True,
        )
        assert isinstance(result["summary"]["estimated_monthly_waste_usd"], float)
        assert isinstance(result["summary"]["total_leaks"], int)

    def test_leaks_is_list(self):
        result = run_pipeline_from_df(
            self._make_df(),
            provider="AWS",
            no_forecast=True,
            already_normalized=True,
        )
        assert isinstance(result["leaks"], list)

    def test_pipeline_stats_has_provider(self):
        result = run_pipeline_from_df(
            self._make_df(),
            provider="AWS",
            no_forecast=True,
            already_normalized=True,
        )
        assert result["pipeline_stats"]["provider"] == "AWS"

    def test_no_dates_in_output(self):
        """All date objects must be serialized to ISO strings."""
        import datetime
        result = run_pipeline_from_df(
            self._make_df(),
            provider="AWS",
            no_forecast=True,
            already_normalized=True,
        )
        def _check(obj):
            if isinstance(obj, dict):
                for v in obj.values():
                    _check(v)
            elif isinstance(obj, list):
                for item in obj:
                    _check(item)
            else:
                assert not isinstance(obj, (datetime.date, datetime.datetime)), \
                    f"Unserialised date found: {obj!r}"
        _check(result)

    def test_unknown_provider_raises(self):
        df = pd.DataFrame({"date": [date(2024, 1, 1)], "cost": [1.0]})
        with pytest.raises(ValueError):
            run_pipeline_from_df(df, provider=None)

    def test_forecast_disabled(self):
        result = run_pipeline_from_df(
            self._make_df(),
            provider="AWS",
            no_forecast=True,
            already_normalized=True,
        )
        assert result["forecasts"] == []

    def test_annual_waste_is_12x_monthly(self):
        result = run_pipeline_from_df(
            self._make_df(),
            provider="AWS",
            no_forecast=True,
            already_normalized=True,
        )
        s = result["summary"]
        assert abs(s["estimated_annual_waste_usd"] - s["estimated_monthly_waste_usd"] * 12) < 0.01
