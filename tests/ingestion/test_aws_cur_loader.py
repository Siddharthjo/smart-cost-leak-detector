"""
Unit tests for AWS CUR Ingestion Module
tests/ingestion/test_aws_cur_loader.py
"""

import pandas as pd
import pytest
import tempfile
import os
from datetime import date

# Import functions to test
from src.ingestion.aws_cur_loader import (
    load_aws_cur_file,
    detect_aws_cur_columns,
    validate_aws_cur_columns,
    extract_aws_cur_data,
    aggregate_aws_cur_data,
    ingest_aws_cur,
    get_aws_cur_stats,
    SERVICE_CODE_MAPPING,
)


# ===================== FIXTURES =====================

@pytest.fixture
def sample_aws_cur_data():
    """Create sample AWS CUR DataFrame for testing."""
    return pd.DataFrame({
        "line_item_usage_start_date": [
            "2025-01-01", "2025-01-01", "2025-01-02",
            "2025-01-02", "2025-01-03", "2025-01-03"
        ],
        "product_servicecode": ["AmazonEC2", "AmazonS3", "AmazonEC2", "AmazonRDS", "AmazonEC2", "AmazonS3"],
        "line_item_resource_id": ["i-12345", "s3://bucket1", "i-12345", "rds-db-1", "i-67890", "s3://bucket1"],
        "line_item_usage_type": ["BoxUsage", "StandardIO", "BoxUsage", "StorageIO", "BoxUsage", "StandardIO"],
        "line_item_usage_amount": [100.0, 50.0, 80.0, 200.0, 120.0, 45.0],
        "line_item_unblended_cost": [50.0, 10.0, 40.0, 150.0, 60.0, 9.0],
        "product_region": ["us-east-1", "us-west-2", "us-east-1", "eu-west-1", "us-east-1", "us-west-2"],
        "line_item_line_item_type": ["Usage", "Usage", "Usage", "Usage", "Usage", "Usage"],
    })


@pytest.fixture
def sample_csv_file(sample_aws_cur_data):
    """Create temporary CSV file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        sample_aws_cur_data.to_csv(f.name, index=False)
        temp_path = f.name
    yield temp_path
    os.unlink(temp_path)


@pytest.fixture
def sample_parquet_file(sample_aws_cur_data):
    """Create temporary Parquet file for testing."""
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        sample_aws_cur_data.to_parquet(f.name, index=False)
        temp_path = f.name
    yield temp_path
    os.unlink(temp_path)


# ===================== TESTS: FILE LOADING =====================

class TestFileLoading:
    """Tests for load_aws_cur_file function."""
    
    def test_load_csv_file(self, sample_csv_file):
        """Should load CSV file successfully."""
        df = load_aws_cur_file(sample_csv_file)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 6
        assert "line_item_usage_start_date" in df.columns
    
    def test_load_parquet_file(self, sample_parquet_file):
        """Should load Parquet file successfully."""
        df = load_aws_cur_file(sample_parquet_file)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 6
    
    def test_load_with_sample_rows(self, sample_csv_file):
        """Should load only N sample rows."""
        df = load_aws_cur_file(sample_csv_file, sample_rows=3)
        assert len(df) == 3
    
    def test_load_nonexistent_file(self):
        """Should raise FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            load_aws_cur_file("/nonexistent/path/file.csv")
    
    def test_load_unsupported_format(self):
        """Should raise ValueError for unsupported file format."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx') as f:
            with pytest.raises(ValueError, match="Unsupported file format"):
                load_aws_cur_file(f.name)
    
    def test_load_empty_file(self):
        """Should raise ValueError for empty CSV."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("")
            temp_path = f.name
        
        try:
            with pytest.raises(ValueError, match="no data"):
                load_aws_cur_file(temp_path)
        finally:
            os.unlink(temp_path)


# ===================== TESTS: COLUMN DETECTION =====================

class TestColumnDetection:
    """Tests for detect_aws_cur_columns function."""
    
    def test_detect_standard_columns(self, sample_aws_cur_data):
        """Should detect standard AWS CUR columns."""
        mapping = detect_aws_cur_columns(sample_aws_cur_data)
        
        assert "line_item_usage_start_date" in mapping
        assert mapping["line_item_usage_start_date"] == "date"
        assert mapping["product_servicecode"] == "service"
        assert mapping["line_item_unblended_cost"] == "cost"
    
    def test_detect_empty_dataframe(self):
        """Should handle empty DataFrame gracefully."""
        df = pd.DataFrame()
        mapping = detect_aws_cur_columns(df)
        assert mapping == {}
    
    def test_detect_with_missing_columns(self):
        """Should detect only available columns."""
        df = pd.DataFrame({
            "line_item_usage_start_date": ["2025-01-01"],
            "line_item_unblended_cost": [100.0]
        })
        mapping = detect_aws_cur_columns(df)
        assert len(mapping) == 2
        assert "date" in mapping.values()


# ===================== TESTS: VALIDATION =====================

class TestValidation:
    """Tests for validate_aws_cur_columns function."""
    
    def test_valid_aws_cur_data(self, sample_aws_cur_data):
        """Should validate proper AWS CUR data."""
        is_valid, message = validate_aws_cur_columns(sample_aws_cur_data)
        assert is_valid is True
        assert "Valid" in message
    
    def test_missing_cost_data(self):
        """Should fail if no cost/usage data."""
        df = pd.DataFrame({
            "date": ["2025-01-01"],
            "service": ["EC2"],
        })
        is_valid, message = validate_aws_cur_columns(df)
        assert is_valid is False
        assert "cost or usage" in message.lower()
    
    def test_strict_validation(self, sample_aws_cur_data):
        """Should require all columns in strict mode."""
        is_valid, msg = validate_aws_cur_columns(sample_aws_cur_data, required=True)
        assert is_valid is True
    
    def test_strict_validation_missing_columns(self):
        """Should fail strict validation with missing columns."""
        df = pd.DataFrame({
            "line_item_unblended_cost": [100.0]
        })
        is_valid, msg = validate_aws_cur_columns(df, required=True)
        assert is_valid is False


# ===================== TESTS: EXTRACTION =====================

class TestExtraction:
    """Tests for extract_aws_cur_data function."""
    
    def test_extract_basic_fields(self, sample_aws_cur_data):
        """Should extract key fields correctly."""
        extracted = extract_aws_cur_data(sample_aws_cur_data)
        
        assert "date" in extracted.columns
        assert "service" in extracted.columns
        assert "cost" in extracted.columns
        assert "usage_type" in extracted.columns
        assert "resource_id" in extracted.columns
    
    def test_date_conversion(self, sample_aws_cur_data):
        """Should convert dates to date objects."""
        extracted = extract_aws_cur_data(sample_aws_cur_data)
        
        assert all(isinstance(d, (date, type(None))) for d in extracted["date"])
        assert extracted["date"].iloc[0] == date(2025, 1, 1)
    
    def test_service_code_mapping(self, sample_aws_cur_data):
        """Should map service codes to readable names."""
        extracted = extract_aws_cur_data(sample_aws_cur_data)
        
        services = extracted["service"].unique()
        assert "EC2" in services
        assert "S3" in services
        assert "RDS" in services
    
    def test_cost_conversion_to_float(self, sample_aws_cur_data):
        """Should convert costs to float."""
        extracted = extract_aws_cur_data(sample_aws_cur_data)
        
        assert extracted["cost"].dtype in [float, "float64", "float32"]
        assert extracted["cost"].sum() > 0
    
    def test_usage_conversion_to_float(self, sample_aws_cur_data):
        """Should convert usage to float."""
        extracted = extract_aws_cur_data(sample_aws_cur_data)
        
        assert extracted["usage"].dtype in [float, "float64", "float32"]
    
    def test_invalid_dates_dropped(self):
        """Should drop rows with invalid dates."""
        df = pd.DataFrame({
            "line_item_usage_start_date": ["2025-01-01", "invalid", "2025-01-02"],
            "line_item_unblended_cost": [100, 50, 75],
            "product_servicecode": ["EC2", "S3", "EC2"],
        })
        extracted = extract_aws_cur_data(df)
        assert len(extracted) < len(df)  # Invalid date removed
    
    def test_invalid_costs_handled(self):
        """Should handle invalid cost values."""
        df = pd.DataFrame({
            "line_item_usage_start_date": ["2025-01-01", "2025-01-01"],
            "line_item_unblended_cost": ["invalid", "100.0"],
            "product_servicecode": ["EC2", "S3"],
        })
        extracted = extract_aws_cur_data(df)
        assert extracted["cost"].iloc[0] == 0  # Invalid cost → 0
        assert extracted["cost"].iloc[1] == 100.0


# ===================== TESTS: AGGREGATION =====================

class TestAggregation:
    """Tests for aggregate_aws_cur_data function."""
    
    def test_aggregate_duplicate_entries(self, sample_aws_cur_data):
        """Should sum costs for duplicate entries."""
        extracted = extract_aws_cur_data(sample_aws_cur_data)
        aggregated = aggregate_aws_cur_data(extracted)
        
        # i-12345 appears twice on 2025-01-01, should be aggregated
        ec2_jan1 = aggregated[
            (aggregated["date"] == date(2025, 1, 1)) &
            (aggregated["service"] == "EC2")
        ]
        assert len(ec2_jan1) == 1
    
    def test_aggregate_costs_sum(self, sample_aws_cur_data):
        """Should sum costs correctly in aggregation."""
        extracted = extract_aws_cur_data(sample_aws_cur_data)
        aggregated = aggregate_aws_cur_data(extracted)
        
        total_before = extracted["cost"].sum()
        total_after = aggregated["cost"].sum()
        assert abs(total_before - total_after) < 0.01  # Floating point tolerance
    
    def test_custom_groupby(self, sample_aws_cur_data):
        """Should support custom grouping."""
        extracted = extract_aws_cur_data(sample_aws_cur_data)
        aggregated = aggregate_aws_cur_data(extracted, group_by=["service"])
        
        assert "service" in aggregated.columns
        assert len(aggregated) == 3  # 3 unique services


# ===================== TESTS: MAIN PIPELINE =====================

class TestIngestPipeline:
    """Tests for main ingest_aws_cur function."""
    
    def test_full_pipeline_csv(self, sample_csv_file):
        """Should run full pipeline on CSV."""
        df = ingest_aws_cur(sample_csv_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "date" in df.columns
        assert "service" in df.columns
        assert "cost" in df.columns
    
    def test_full_pipeline_parquet(self, sample_parquet_file):
        """Should run full pipeline on Parquet."""
        df = ingest_aws_cur(sample_parquet_file)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
    
    def test_pipeline_with_sample_rows(self, sample_csv_file):
        """Should limit rows in pipeline."""
        df = ingest_aws_cur(sample_csv_file, sample_rows=3)
        assert len(df) <= 3
    
    def test_pipeline_without_aggregation(self, sample_csv_file):
        """Should work without aggregation."""
        df_agg = ingest_aws_cur(sample_csv_file, aggregate=True)
        df_raw = ingest_aws_cur(sample_csv_file, aggregate=False)
        
        assert len(df_agg) <= len(df_raw)


# ===================== TESTS: STATISTICS =====================

class TestStatistics:
    """Tests for get_aws_cur_stats function."""
    
    def test_get_stats(self, sample_csv_file):
        """Should return valid statistics."""
        df = ingest_aws_cur(sample_csv_file)
        stats = get_aws_cur_stats(df)
        
        assert "total_rows" in stats
        assert "total_cost" in stats
        assert "unique_services" in stats
        assert "unique_resources" in stats
        assert "date_range" in stats
        
        assert stats["total_rows"] > 0
        assert stats["total_cost"] > 0
        assert stats["unique_services"] > 0
    
    def test_stats_date_range(self, sample_csv_file):
        """Should provide correct date range."""
        df = ingest_aws_cur(sample_csv_file)
        stats = get_aws_cur_stats(df)
        
        assert stats["date_range"]["start"] == "2025-01-01"
        assert stats["date_range"]["end"] == "2025-01-03"


# ===================== TESTS: INTEGRATION =====================

class TestIntegration:
    """Integration tests with full pipeline."""
    
    def test_pipeline_preserves_data_integrity(self, sample_csv_file):
        """Should preserve data integrity through pipeline."""
        raw_df = load_aws_cur_file(sample_csv_file)
        processed_df = ingest_aws_cur(sample_csv_file)
        
        # Total cost should be preserved
        raw_total = raw_df["line_item_unblended_cost"].sum()
        processed_total = processed_df["cost"].sum()
        
        assert abs(raw_total - processed_total) < 0.01
    
    def test_output_ready_for_normalization(self, sample_csv_file):
        """Output should be compatible with normalization."""
        df = ingest_aws_cur(sample_csv_file)
        
        # Add provider (required by normalizer)
        df["provider"] = "AWS"
        
        # Check required columns exist
        required = ["date", "service", "cost", "provider"]
        for col in required:
            assert col in df.columns
    
    def test_no_nulls_in_critical_fields(self, sample_csv_file):
        """Critical fields should not have nulls."""
        df = ingest_aws_cur(sample_csv_file)
        
        assert df["date"].notna().all()
        assert df["service"].notna().all()
        assert df["cost"].notna().all()


# ===================== EDGE CASES =====================

class TestEdgeCases:
    """Tests for edge cases and error conditions."""
    
    def test_zero_cost_records(self):
        """Should handle zero-cost records."""
        df = pd.DataFrame({
            "line_item_usage_start_date": ["2025-01-01", "2025-01-01"],
            "product_servicecode": ["EC2", "S3"],
            "line_item_unblended_cost": [0, 100.0],
        })
        extracted = extract_aws_cur_data(df)
        assert len(extracted) == 2
        assert extracted["cost"].iloc[0] == 0
    
    def test_missing_resource_ids(self):
        """Should handle missing resource IDs."""
        df = pd.DataFrame({
            "line_item_usage_start_date": ["2025-01-01"],
            "product_servicecode": ["EC2"],
            "line_item_resource_id": [None],
            "line_item_unblended_cost": [100.0],
        })
        extracted = extract_aws_cur_data(df)
        assert len(extracted) == 1  # Should still include
    
    def test_service_without_mapping(self):
        """Should handle unmapped service codes."""
        df = pd.DataFrame({
            "line_item_usage_start_date": ["2025-01-01"],
            "product_servicecode": ["UnknownService123"],
            "line_item_unblended_cost": [100.0],
        })
        extracted = extract_aws_cur_data(df)
        assert extracted["service"].iloc[0] == "UnknownService123"
    
    def test_very_large_costs(self):
        """Should handle very large costs."""
        df = pd.DataFrame({
            "line_item_usage_start_date": ["2025-01-01"],
            "product_servicecode": ["EC2"],
            "line_item_unblended_cost": [999999999.99],
        })
        extracted = extract_aws_cur_data(df)
        assert extracted["cost"].iloc[0] == 999999999.99


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
