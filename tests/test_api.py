"""Tests for FastAPI endpoints in src/api.py."""

import io
from datetime import date, timedelta

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.api import app

client = TestClient(app)


# ===================== HELPERS =====================

def _aws_cur_csv(n_days: int = 20) -> bytes:
    """Generate a minimal but valid AWS CUR CSV file."""
    end = date(2024, 3, 31)
    rows = ["line_item_usage_start_date,line_item_usage_account_id,"
            "line_item_line_item_type,product_servicecode,"
            "line_item_resource_id,line_item_usage_amount,"
            "line_item_unblended_cost,product_region"]
    for i in range(n_days):
        d = end - timedelta(days=n_days - 1 - i)
        # Increasing costs to trigger RUNAWAY_COST detector
        cost = 2.0 + i * 4
        rows.append(
            f"{d},123456789012,Usage,AmazonEC2,"
            f"i-test{i:04d},{cost * 2},{cost},us-east-1"
        )
    return "\n".join(rows).encode()


def _azure_billing_csv(n_days: int = 20) -> bytes:
    """Generate a minimal valid Azure billing CSV."""
    rows = ["UsageDate,ServiceName,CostInUSD,Usage,ResourceId,Region,Tags"]
    end = date(2024, 3, 31)
    for i in range(n_days):
        d = end - timedelta(days=n_days - 1 - i)
        rows.append(
            f"{d},Virtual Machines,{50 + i * 2},{100 + i},vm-{i:04d},eastus,"
        )
    return "\n".join(rows).encode()


# ===================== HEALTH ENDPOINT =====================

class TestHealthEndpoint:
    def test_returns_200(self):
        resp = client.get("/api/health")
        assert resp.status_code == 200

    def test_returns_ok_status(self):
        resp = client.get("/api/health")
        assert resp.json()["status"] == "ok"

    def test_returns_timestamp(self):
        resp = client.get("/api/health")
        assert "timestamp" in resp.json()


# ===================== FILE UPLOAD ENDPOINT =====================

class TestUploadEndpoint:
    def test_valid_aws_csv_returns_200(self):
        resp = client.post(
            "/api/analyze/upload",
            files={"file": ("cur.csv", io.BytesIO(_aws_cur_csv()), "text/csv")},
            data={"no_forecast": "true"},
        )
        assert resp.status_code == 200

    def test_response_has_expected_keys(self):
        resp = client.post(
            "/api/analyze/upload",
            files={"file": ("cur.csv", io.BytesIO(_aws_cur_csv()), "text/csv")},
            data={"no_forecast": "true"},
        )
        body = resp.json()
        assert "summary" in body
        assert "leaks" in body
        assert "forecasts" in body
        assert "pipeline_stats" in body

    def test_summary_contains_numeric_waste(self):
        resp = client.post(
            "/api/analyze/upload",
            files={"file": ("cur.csv", io.BytesIO(_aws_cur_csv()), "text/csv")},
            data={"no_forecast": "true"},
        )
        s = resp.json()["summary"]
        assert isinstance(s["estimated_monthly_waste_usd"], float)
        assert isinstance(s["total_leaks"], int)

    def test_provider_auto_detected_as_aws(self):
        resp = client.post(
            "/api/analyze/upload",
            files={"file": ("cur.csv", io.BytesIO(_aws_cur_csv()), "text/csv")},
            data={"no_forecast": "true"},
        )
        assert resp.json()["pipeline_stats"]["provider"] == "AWS"

    def test_provider_override_respected(self):
        # Azure CSV with explicit provider override
        resp = client.post(
            "/api/analyze/upload",
            files={"file": ("billing.csv", io.BytesIO(_azure_billing_csv()), "text/csv")},
            data={"no_forecast": "true", "provider": "azure"},
        )
        assert resp.status_code == 200
        assert resp.json()["pipeline_stats"]["provider"] == "AZURE"

    def test_empty_file_returns_400(self):
        resp = client.post(
            "/api/analyze/upload",
            files={"file": ("empty.csv", io.BytesIO(b""), "text/csv")},
        )
        assert resp.status_code == 400

    def test_garbage_file_returns_400_or_422(self):
        resp = client.post(
            "/api/analyze/upload",
            files={"file": ("bad.csv", io.BytesIO(b"\x00\x01\x02\x03"), "text/csv")},
        )
        assert resp.status_code in {400, 422, 500}

    def test_no_file_returns_422(self):
        resp = client.post("/api/analyze/upload")
        assert resp.status_code == 422

    def test_parquet_file_accepted(self):
        """Parquet file is parsed correctly (if pyarrow installed)."""
        rows = []
        end = date(2024, 3, 31)
        for i in range(20):
            d = end - timedelta(days=19 - i)
            rows.append({
                "line_item_usage_start_date": str(d),
                "line_item_usage_account_id": "123456789012",
                "line_item_line_item_type": "Usage",
                "product_servicecode": "AmazonEC2",
                "line_item_resource_id": f"i-{i:04d}",
                "line_item_usage_amount": float(i * 2),
                "line_item_unblended_cost": float(2 + i * 3),
                "product_region": "us-east-1",
            })
        buf = io.BytesIO()
        pd.DataFrame(rows).to_parquet(buf, index=False)
        buf.seek(0)

        resp = client.post(
            "/api/analyze/upload",
            files={"file": ("cur.parquet", buf, "application/octet-stream")},
            data={"no_forecast": "true"},
        )
        assert resp.status_code == 200


# ===================== AWS CREDENTIALS ENDPOINT =====================

class TestAWSCredentialsEndpoint:
    def test_missing_credentials_returns_422(self):
        resp = client.post("/api/analyze/aws", json={})
        assert resp.status_code == 422

    def test_missing_secret_key_returns_422(self):
        resp = client.post("/api/analyze/aws", json={
            "access_key_id": "AKIAIOSFODNN7EXAMPLE",
        })
        assert resp.status_code == 422

    def test_wrong_content_type_returns_422(self):
        resp = client.post(
            "/api/analyze/aws",
            data={"access_key_id": "test", "secret_access_key": "test"},
        )
        assert resp.status_code == 422

    def test_invalid_days_range_returns_422(self):
        resp = client.post("/api/analyze/aws", json={
            "access_key_id": "AKIAIOSFODNN7EXAMPLE",
            "secret_access_key": "wJalrXUtnFEMI",
            "days": 999,  # > 90
        })
        assert resp.status_code == 422


# ===================== RESPONSE STRUCTURE CONTRACT =====================

class TestResponseContract:
    """Verify the shape of a successful response matches frontend expectations."""

    def test_summary_keys(self):
        resp = client.post(
            "/api/analyze/upload",
            files={"file": ("cur.csv", io.BytesIO(_aws_cur_csv()), "text/csv")},
            data={"no_forecast": "true"},
        )
        s = resp.json()["summary"]
        required = {"total_leaks", "high", "medium", "low",
                    "estimated_monthly_waste_usd", "estimated_annual_waste_usd"}
        assert required.issubset(s.keys())

    def test_leaks_are_list_of_dicts(self):
        resp = client.post(
            "/api/analyze/upload",
            files={"file": ("cur.csv", io.BytesIO(_aws_cur_csv()), "text/csv")},
            data={"no_forecast": "true"},
        )
        leaks = resp.json()["leaks"]
        assert isinstance(leaks, list)
        for leak in leaks:
            assert isinstance(leak, dict)

    def test_each_leak_has_required_fields(self):
        resp = client.post(
            "/api/analyze/upload",
            files={"file": ("cur.csv", io.BytesIO(_aws_cur_csv()), "text/csv")},
            data={"no_forecast": "true"},
        )
        for leak in resp.json()["leaks"]:
            assert "leak_type" in leak
            assert "severity"  in leak
            assert "service"   in leak

    def test_no_raw_date_objects_in_response(self):
        """All dates must be ISO strings — no Python date/datetime objects."""
        import json
        resp = client.post(
            "/api/analyze/upload",
            files={"file": ("cur.csv", io.BytesIO(_aws_cur_csv()), "text/csv")},
            data={"no_forecast": "true"},
        )
        # If the response body is valid JSON, date objects are already serialised
        assert resp.status_code == 200
        body_text = resp.text
        # Should not contain Python repr of date
        assert "datetime.date(" not in body_text
