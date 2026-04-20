"""Tests for AWS, Azure, and GCP normalizers and schema enforcer."""

from datetime import date, timedelta

import pandas as pd
import pytest

from src.normalization.aws_normalizer import normalize_aws
from src.normalization.azure_normalizer import normalize_azure
from src.normalization.gcp_normalizer import normalize_gcp
from src.normalization.schema_enforcer import enforce_schema


# ===================== enforce_schema =====================

class TestEnforceSchema:
    def test_drops_rows_with_null_cost(self):
        df = pd.DataFrame({
            "date": [date(2024, 1, 1), date(2024, 1, 2)],
            "service": ["ec2", "rds"],
            "cost": [10.0, None],
        })
        result = enforce_schema(df)
        assert len(result) == 1

    def test_drops_rows_with_null_date(self):
        df = pd.DataFrame({
            "date": [date(2024, 1, 1), None],
            "service": ["ec2", "ec2"],
            "cost": [10.0, 5.0],
        })
        result = enforce_schema(df)
        assert len(result) == 1

    def test_drops_rows_with_null_service(self):
        df = pd.DataFrame({
            "date": [date(2024, 1, 1), date(2024, 1, 2)],
            "service": ["ec2", None],
            "cost": [10.0, 5.0],
        })
        result = enforce_schema(df)
        assert len(result) == 1

    def test_coerces_cost_to_numeric(self):
        df = pd.DataFrame({
            "date": [date(2024, 1, 1)],
            "service": ["ec2"],
            "cost": ["12.34"],
        })
        result = enforce_schema(df)
        assert result["cost"].dtype in [float, "float64"]
        assert result["cost"].iloc[0] == pytest.approx(12.34)

    def test_adds_missing_optional_columns(self):
        df = pd.DataFrame({
            "date": [date(2024, 1, 1)],
            "service": ["ec2"],
            "cost": [10.0],
        })
        result = enforce_schema(df)
        for col in ["usage", "resource_id", "region"]:
            assert col in result.columns

    def test_preserves_valid_rows(self):
        df = pd.DataFrame({
            "date": [date(2024, 1, 1), date(2024, 1, 2)],
            "service": ["ec2", "rds"],
            "cost": [10.0, 5.0],
        })
        result = enforce_schema(df)
        assert len(result) == 2


# ===================== normalize_aws =====================

class TestNormalizeAWS:
    def _make_cur_df(self, n_rows=3):
        end = date(2024, 3, 31)
        return pd.DataFrame({
            "line_item_usage_start_date": [
                str(end - timedelta(days=n_rows - 1 - i)) for i in range(n_rows)
            ],
            "product_servicecode":    ["AmazonEC2"] * n_rows,
            "line_item_resource_id":  [f"i-{i:04d}" for i in range(n_rows)],
            "line_item_usage_amount": [10.0] * n_rows,
            "line_item_unblended_cost": [5.0] * n_rows,
            "product_region":         ["us-east-1"] * n_rows,
        })

    def test_output_has_canonical_columns(self):
        result = normalize_aws(self._make_cur_df())
        for col in ["date", "service", "cost", "usage", "resource_id", "region"]:
            assert col in result.columns, f"Missing column: {col}"

    def test_date_parsed_correctly(self):
        result = normalize_aws(self._make_cur_df(1))
        assert isinstance(result["date"].iloc[0], date)

    def test_cost_is_numeric(self):
        result = normalize_aws(self._make_cur_df())
        assert result["cost"].dtype in [float, "float64"]

    def test_provider_column_set(self):
        result = normalize_aws(self._make_cur_df())
        assert (result["provider"] == "AWS").all()

    def test_no_rows_dropped_for_valid_data(self):
        df = self._make_cur_df(5)
        result = normalize_aws(df)
        assert len(result) == 5

    def test_drops_rows_with_invalid_cost(self):
        df = self._make_cur_df(3)
        df.loc[1, "line_item_unblended_cost"] = "invalid"
        result = normalize_aws(df)
        assert len(result) == 2


    def test_tax_line_items_filtered_out(self):
        end = date(2024, 3, 31)
        df = pd.DataFrame({
            "line_item_usage_start_date":  [str(end), str(end)],
            "product_servicecode":         ["AmazonEC2", "AmazonEC2"],
            "line_item_resource_id":       ["i-0001", "i-0002"],
            "line_item_usage_amount":      [10.0, 5.0],
            "line_item_unblended_cost":    [8.0, 2.0],
            "product_region":              ["us-east-1", "us-east-1"],
            "line_item_line_item_type":    ["Usage", "Tax"],
        })
        result = normalize_aws(df)
        assert len(result) == 1
        assert result["resource_id"].iloc[0] == "i-0001"

    def test_tax_filter_works_after_slash_rename(self):
        end = date(2024, 3, 31)
        df = pd.DataFrame({
            "lineItem/UsageStartDate":  [str(end), str(end)],
            "product/servicecode":      ["AmazonEC2", "AmazonEC2"],
            "lineItem/ResourceId":      ["i-0001", "i-0002"],
            "lineItem/UsageAmount":     [10.0, 5.0],
            "lineItem/UnblendedCost":   [8.0, 2.0],
            "product/region":           ["us-east-1", "us-east-1"],
            "lineItem/LineItemType":    ["Usage", "Tax"],
        })
        result = normalize_aws(df)
        assert len(result) == 1
        assert result["resource_id"].iloc[0] == "i-0001"

    def test_slash_format_columns_accepted(self):
        """Newer CUR exports use slash-format column names — must normalize identically."""
        end = date(2024, 3, 31)
        df = pd.DataFrame({
            "lineItem/UsageStartDate":  [str(end - timedelta(days=i)) for i in range(3)],
            "product/servicecode":      ["AmazonEC2"] * 3,
            "lineItem/ResourceId":      [f"i-{i:04d}" for i in range(3)],
            "lineItem/UsageAmount":     [10.0] * 3,
            "lineItem/UnblendedCost":   [5.0] * 3,
            "product/region":           ["us-east-1"] * 3,
        })
        result = normalize_aws(df)
        assert len(result) == 3
        for col in ["date", "service", "cost", "usage", "resource_id", "region"]:
            assert col in result.columns

    def test_slash_and_underscore_produce_same_schema(self):
        end = date(2024, 3, 31)
        dates = [str(end - timedelta(days=i)) for i in range(3)]

        df_slash = pd.DataFrame({
            "lineItem/UsageStartDate": dates,
            "product/servicecode":     ["AmazonEC2"] * 3,
            "lineItem/ResourceId":     ["i-0001"] * 3,
            "lineItem/UsageAmount":    [10.0] * 3,
            "lineItem/UnblendedCost":  [5.0] * 3,
            "product/region":          ["us-east-1"] * 3,
        })
        df_under = pd.DataFrame({
            "line_item_usage_start_date": dates,
            "product_servicecode":        ["AmazonEC2"] * 3,
            "line_item_resource_id":      ["i-0001"] * 3,
            "line_item_usage_amount":     [10.0] * 3,
            "line_item_unblended_cost":   [5.0] * 3,
            "product_region":             ["us-east-1"] * 3,
        })

        r_slash = normalize_aws(df_slash).reset_index(drop=True)
        r_under = normalize_aws(df_under).reset_index(drop=True)

        shared = [c for c in r_slash.columns if c in r_under.columns]
        for col in shared:
            assert list(r_slash[col]) == list(r_under[col]), f"Mismatch in column: {col}"


# ===================== normalize_azure =====================

class TestNormalizeAzure:
    def _make_azure_df(self, n_rows=3):
        return pd.DataFrame({
            "UsageDate":    [f"2024-03-{i + 1:02d}" for i in range(n_rows)],
            "ServiceName":  ["Virtual Machines"] * n_rows,
            "CostInUSD":    [10.0] * n_rows,
            "Usage":        [100.0] * n_rows,
            "ResourceId":   [f"vm-{i:04d}" for i in range(n_rows)],
            "Region":       ["eastus"] * n_rows,
            "Tags":         ["Owner=team-a;Environment=prod"] * n_rows,
        })

    def test_output_has_canonical_columns(self):
        result = normalize_azure(self._make_azure_df())
        for col in ["date", "service", "cost"]:
            assert col in result.columns

    def test_uses_costusd_column(self):
        result = normalize_azure(self._make_azure_df(1))
        assert result["cost"].iloc[0] == pytest.approx(10.0)

    def test_falls_back_to_cost_column(self):
        df = self._make_azure_df(1)
        df = df.rename(columns={"CostInUSD": "Cost"})
        result = normalize_azure(df)
        assert result["cost"].iloc[0] == pytest.approx(10.0)

    def test_tag_extraction(self):
        result = normalize_azure(self._make_azure_df(1))
        assert "labels.owner" in result.columns

    def test_provider_column_set(self):
        result = normalize_azure(self._make_azure_df())
        assert (result["provider"] == "Azure").all()


# ===================== normalize_gcp =====================

class TestNormalizeGCP:
    def _make_gcp_df(self, n_rows=3):
        return pd.DataFrame({
            "usage_start_time":  [f"2024-03-{i + 1:02d} 00:00:00" for i in range(n_rows)],
            "service_description": ["Compute Engine"] * n_rows,
            "cost":              [15.0] * n_rows,
            "usage_amount":      [200.0] * n_rows,
            "resource_name":     [f"instance-{i}" for i in range(n_rows)],
            "region":            ["us-central1"] * n_rows,
            "label_environment": ["prod"] * n_rows,
        })

    def test_output_has_canonical_columns(self):
        result = normalize_gcp(self._make_gcp_df())
        for col in ["date", "service", "cost"]:
            assert col in result.columns

    def test_cost_preserved(self):
        result = normalize_gcp(self._make_gcp_df(1))
        assert result["cost"].iloc[0] == pytest.approx(15.0)

    def test_provider_column_set(self):
        result = normalize_gcp(self._make_gcp_df())
        assert (result["provider"] == "GCP").all()

    def test_missing_resource_name_filled(self):
        df = self._make_gcp_df(2)
        df.loc[0, "resource_name"] = None
        result = normalize_gcp(df)
        # Should not drop the row — fills with "unknown"
        assert len(result) == 2
        assert result["resource_id"].iloc[0] == "unknown"
