"""
AWS CUR Ingestion Examples
==========================

This file demonstrates how to use the aws_cur_loader module
with real-world scenarios.
"""

from src.ingestion.aws_cur_loader import (
    load_aws_cur_file,
    ingest_aws_cur,
    detect_aws_cur_columns,
    validate_aws_cur_columns,
    extract_aws_cur_data,
    get_aws_cur_stats,
    print_aws_cur_stats,
)
import pandas as pd


# ===================== EXAMPLE 1: BASIC LOADING =====================

def example_basic_load():
    """
    Simplest usage - load AWS CUR CSV and print stats.
    """
    print("\n" + "="*60)
    print(" EXAMPLE 1: Basic AWS CUR Loading")
    print("="*60)
    
    df = ingest_aws_cur("data/raw/aws/aws_cur_january.csv")
    print_aws_cur_stats(df)
    print(df.head())


# ===================== EXAMPLE 2: PARQUET LOADING =====================

def example_load_parquet():
    """
    Load from Parquet file (faster for large datasets).
    """
    print("\n" + "="*60)
    print(" EXAMPLE 2: Load from Parquet")
    print("="*60)
    
    df = ingest_aws_cur("data/raw/aws/aws_cur_january.parquet")
    print_aws_cur_stats(df)


# ===================== EXAMPLE 3: SAMPLE LOADING =====================

def example_sample_loading():
    """
    Load only first 10,000 rows for quick testing.
    Useful for large CUR files.
    """
    print("\n" + "="*60)
    print(" EXAMPLE 3: Load Sample (10K rows)")
    print("="*60)
    
    df = ingest_aws_cur(
        "data/raw/aws/aws_cur_january.csv",
        sample_rows=10000
    )
    print_aws_cur_stats(df)


# ===================== EXAMPLE 4: CUSTOM EXTRACTION =====================

def example_custom_extraction():
    """
    Manual step-by-step extraction for debugging or custom logic.
    """
    print("\n" + "="*60)
    print(" EXAMPLE 4: Step-by-Step Extraction")
    print("="*60)
    
    # Step 1: Load raw file
    raw_df = load_aws_cur_file("data/raw/aws/aws_cur_january.csv")
    print(f"Raw columns: {raw_df.columns.tolist()}")
    
    # Step 2: Detect available columns
    column_mapping = detect_aws_cur_columns(raw_df)
    print(f"Detected mapping: {column_mapping}")
    
    # Step 3: Validate
    is_valid, msg = validate_aws_cur_columns(raw_df)
    print(f"Validation: {msg}")
    
    # Step 4: Extract
    if is_valid:
        extracted = extract_aws_cur_data(raw_df, column_mapping)
        print(f"Extracted shape: {extracted.shape}")
        print(extracted.head())


# ===================== EXAMPLE 5: NO AGGREGATION =====================

def example_raw_extraction():
    """
    Extract without aggregating duplicates.
    Useful for detailed row-level analysis.
    """
    print("\n" + "="*60)
    print(" EXAMPLE 5: Raw Extraction (No Aggregation)")
    print("="*60)
    
    df = ingest_aws_cur(
        "data/raw/aws/aws_cur_january.csv",
        aggregate=False
    )
    print(f"Total rows (raw): {len(df)}")
    print(df.info())


# ===================== EXAMPLE 6: COST BREAKDOWN =====================

def example_cost_breakdown():
    """
    Analyze cost distribution after loading.
    """
    print("\n" + "="*60)
    print(" EXAMPLE 6: Cost Breakdown by Service")
    print("="*60)
    
    df = ingest_aws_cur("data/raw/aws/aws_cur_january.csv")
    
    # Breakdown by service
    service_costs = df.groupby("service")["cost"].sum().sort_values(ascending=False)
    print("\nCost by Service:")
    print(service_costs)
    
    # Top 10 resources by cost
    if "resource_id" in df.columns:
        resource_costs = (
            df[df["resource_id"] != ""]
            .groupby("resource_id")["cost"]
            .sum()
            .nlargest(10)
        )
        print("\nTop 10 Resources by Cost:")
        print(resource_costs)


# ===================== EXAMPLE 7: USAGE ANALYSIS =====================

def example_usage_analysis():
    """
    Analyze usage patterns and efficiency.
    """
    print("\n" + "="*60)
    print(" EXAMPLE 7: Usage Analysis")
    print("="*60)
    
    df = ingest_aws_cur("data/raw/aws/aws_cur_january.csv")
    
    # Resources with high cost but low usage
    if "usage" in df.columns:
        df["cost_per_unit"] = df["cost"] / (df["usage"] + 0.001)
        
        inefficient = (
            df[df["cost_per_unit"] > df["cost_per_unit"].quantile(0.9)]
            .groupby(["service", "resource_id"])"cost"]
            .sum()
            .nlargest(10)
        )
        print("\nMost Inefficient Resources (by cost/unit):")
        print(inefficient)


# ===================== EXAMPLE 8: DAILY COST TREND =====================

def example_daily_trends():
    """
    Analyze daily cost trends.
    """
    print("\n" + "="*60)
    print(" EXAMPLE 8: Daily Cost Trends")
    print("="*60)
    
    df = ingest_aws_cur("data/raw/aws/aws_cur_january.csv")
    
    # Daily total cost
    daily_cost = df.groupby("date")["cost"].sum()
    print("\nDaily Costs:")
    print(daily_cost)
    
    # Service trends
    daily_service = df.groupby(["date", "service"])"cost"].sum().unstack(fill_value=0)
    print("\nDaily Costs by Service (first 5 days):")
    print(daily_service.head())


# ===================== EXAMPLE 9: FILTER BY SERVICE =====================

def example_filter_service():
    """
    Filter AWS CUR data to specific service.
    Useful for focused analysis.
    """
    print("\n" + "="*60)
    print(" EXAMPLE 9: Filter by Service (EC2 Only)")
    print("="*60)
    
    df = ingest_aws_cur("data/raw/aws/aws_cur_january.csv")
    
    ec2_data = df[df["service"] == "EC2"]
    print(f"EC2 records: {len(ec2_data)}")
    print(f"Total EC2 cost: ${ec2_data['cost'].sum():,.2f}")
    
    # EC2 by usage type
    ec2_usage = ec2_data.groupby("usage_type")"cost"].sum().sort_values(ascending=False)
    print("\nEC2 Cost by Usage Type:")
    print(ec2_usage.head(10))


# ===================== EXAMPLE 10: EXPORT FOR DOWNSTREAM =====================

def example_export_for_normalization():
    """
    Load AWS CUR and prepare for normalization pipeline.
    """
    print("\n" + "="*60)
    print(" EXAMPLE 10: Export for Normalization Pipeline")
    print("="*60)
    
    df = ingest_aws_cur("data/raw/aws/aws_cur_january.csv")
    
    # Select columns needed by normalizer
    pipeline_df = df[[
        "date",
        "service",
        "cost",
        "usage",
        "resource_id",
        "region"
    ]].copy()
    
    # Add provider column (required by normalization)
    pipeline_df["provider"] = "AWS"
    
    print(f"Ready for pipeline: {pipeline_df.shape}")
    print(pipeline_df.dtypes)
    print(pipeline_df.head())
    
    # Can save for further processing
    # pipeline_df.to_csv("data/processed/aws_cur_prepared.csv", index=False)
    
    return pipeline_df


# ===================== EXAMPLE 11: ERROR HANDLING =====================

def example_error_handling():
    """
    Demonstrate error handling and validation.
    """
    print("\n" + "="*60)
    print(" EXAMPLE 11: Error Handling")
    print("="*60)
    
    # Example 1: File not found
    try:
        df = ingest_aws_cur("data/nonexistent.csv")
    except FileNotFoundError as e:
        print(f"❌ FileNotFoundError: {e}")
    
    # Example 2: Invalid file format
    try:
        df = ingest_aws_cur("data/file.xlsx")  # Not CSV or Parquet
    except ValueError as e:
        print(f"❌ ValueError: {e}")
    
    # Example 3: Empty file
    try:
        df = ingest_aws_cur("data/empty.csv")
    except ValueError as e:
        print(f"❌ ValueError: {e}")


# ===================== MAIN RUNNER =====================

if __name__ == "__main__":
    """
    Run individual examples:
        python -c "from src.ingestion.aws_cur_examples import example_basic_load; example_basic_load()"
    
    Or run all:
        python -c "from src.ingestion.aws_cur_examples import *; 
                  [example_basic_load(), example_load_parquet(), ...]"
    """
    
    print("\n" + "="*60)
    print(" AWS CUR INGESTION EXAMPLES")
    print("="*60)
    
    # Uncomment examples to run:
    # example_basic_load()
    # example_load_parquet()
    # example_sample_loading()
    # example_custom_extraction()
    # example_raw_extraction()
    # example_cost_breakdown()
    # example_usage_analysis()
    # example_daily_trends()
    # example_filter_service()
    # example_export_for_normalization()
    # example_error_handling()
    
    print("\n✓ Examples loaded. Run individual functions to test.\n")