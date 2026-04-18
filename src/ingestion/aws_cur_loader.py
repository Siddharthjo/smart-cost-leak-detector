import pandas as pd
import os
from typing import Tuple, Dict, Optional
from datetime import datetime


# ===================== AWS CUR COLUMN MAPPINGS =====================

# Standard AWS Cost & Usage Report column names
AWS_CUR_COLUMNS = {
    "line_item_usage_start_date": "date",
    "line_item_product_code": "service_code",
    "product_servicecode": "service",
    "line_item_resource_id": "resource_id",
    "line_item_usage_type": "usage_type",
    "line_item_usage_amount": "usage",
    "line_item_unblended_cost": "cost",
    "product_region": "region",
    "resource_tags_user_owner": "owner_tag",
    "resource_tags_user_environment": "environment_tag",
    "line_item_line_item_type": "item_type",
}

# Alternative column names (AWS sometimes varies these)
AWS_CUR_COLUMN_ALIASES = {
    "bill_billing_period_start_date": "date",
    "product_product_name": "service",
    "line_item_unblended_rate": "unit_cost",
    "pricing_currency": "currency",
}

# Service code to human-readable service name
SERVICE_CODE_MAPPING = {
    "AmazonEC2": "EC2",
    "AmazonRDS": "RDS",
    "AmazonS3": "S3",
    "AWSLambda": "Lambda",
    "AmazonDynamoDB": "DynamoDB",
    "AmazonElastiCache": "ElastiCache",
    "AmazonESG": "Elasticsearch",
    "AmazonCloudFront": "CloudFront",
    "AmazonSNS": "SNS",
    "AmazonSQS": "SQS",
    "AWSKms": "KMS",
    "AmazonVPC": "VPC",
    "AWSCloudTrail": "CloudTrail",
}


# ===================== FILE LOADING =====================

def load_aws_cur_file(
    file_path: str,
    sample_rows: Optional[int] = None
) -> pd.DataFrame:
    """
    Load AWS CUR data from CSV or Parquet file.
    
    Args:
        file_path: Path to CSV or Parquet file
        sample_rows: Optional - load only first N rows for testing
    
    Returns:
        Raw DataFrame from file
    
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file format unsupported
        
    Examples:
        df = load_aws_cur_file("data/aws_cur.csv")
        df_sample = load_aws_cur_file("data/aws_cur.parquet", sample_rows=1000)
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    file_ext = os.path.splitext(file_path)[1].lower()
    
    if file_ext == ".csv":
        df = pd.read_csv(file_path, nrows=sample_rows)
    elif file_ext in [".parquet", ".pqt"]:
        df = pd.read_parquet(file_path)
        if sample_rows:
            df = df.head(sample_rows)
    else:
        raise ValueError(f"Unsupported file format: {file_ext}. Use CSV or Parquet.")
    
    if df.empty:
        raise ValueError("File contains no data rows")
    
    return df


# ===================== COLUMN DETECTION & MAPPING =====================

def detect_aws_cur_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Detect available AWS CUR columns in DataFrame.
    Returns mapping of {original_col_name: standard_name}
    
    Example output:
        {
            "line_item_usage_start_date": "date",
            "product_servicecode": "service",
            "line_item_resource_id": "resource_id",
            ...
        }
    """
    detected = {}
    
    for original_col, standard_name in AWS_CUR_COLUMNS.items():
        if original_col in df.columns:
            detected[original_col] = standard_name
    
    # Try aliases if primary columns not found
    for alias_col, standard_name in AWS_CUR_COLUMN_ALIASES.items():
        if alias_col in df.columns and standard_name not in detected.values():
            detected[alias_col] = standard_name
    
    return detected


def validate_aws_cur_columns(df: pd.DataFrame, required: bool = False) -> Tuple[bool, str]:
    """
    Validate that DataFrame contains necessary AWS CUR columns.
    
    Args:
        df: DataFrame to validate
        required: If True, requires all key columns. If False, only checks for data.
    
    Returns:
        (is_valid: bool, message: str)
    """
    detected = detect_aws_cur_columns(df)
    
    # Minimum required columns for basic analysis
    minimum_required = ["date", "service", "cost"]
    
    detected_values = set(detected.values())
    
    if required:
        missing = set(minimum_required) - detected_values
        if missing:
            return False, f"Missing required AWS CUR columns: {missing}"
    else:
        # Just ensure some cost/usage data exists
        if "cost" not in detected_values and "usage" not in detected_values:
            return False, "No cost or usage data found in file"
    
    return True, f"Valid AWS CUR file with columns: {detected_values}"


# ===================== DATA EXTRACTION & CLEANING =====================

def extract_aws_cur_data(
    df: pd.DataFrame,
    column_mapping: Optional[Dict[str, str]] = None
) -> pd.DataFrame:
    """
    Extract and clean key AWS CUR fields.
    Returns minimal clean DataFrame with service, cost, usage_type, resource_id.
    
    Args:
        df: Raw AWS CUR DataFrame
        column_mapping: Custom column mapping (uses auto-detect if None)
    
    Returns:
        Clean DataFrame with standardized columns
        
    Example output:
        | date       | service | cost  | usage_type  | resource_id      |
        |------------|---------|-------|-------------|------------------|
        | 2025-01-01 | EC2     | 50.00 | BoxUsage    | i-0123456789abcd |
    """
    # Auto-detect columns if not provided
    if column_mapping is None:
        column_mapping = detect_aws_cur_columns(df)
    
    if not column_mapping:
        raise ValueError("No AWS CUR columns detected in DataFrame")
    
    # Start with raw data
    extracted = df.copy()
    
    # Rename columns to standard names
    rename_map = {v: k for k, v in column_mapping.items()}
    extracted = extracted.rename(columns=rename_map)
    
    # ---- DATE HANDLING ----
    if "date" in extracted.columns:
        extracted["date"] = pd.to_datetime(
            extracted["date"],
            errors="coerce"
        ).dt.date
        # Remove rows with invalid dates
        extracted = extracted.dropna(subset=["date"])
    
    # ---- SERVICE NAME CLEANUP ----
    if "service" in extracted.columns:
        extracted["service"] = extracted["service"].fillna("Unknown")
        # Map service codes to readable names
        extracted["service"] = extracted["service"].map(
            lambda x: SERVICE_CODE_MAPPING.get(str(x).strip(), str(x))
        )
    
    # ---- COST CONVERSION (ensure float) ----
    if "cost" in extracted.columns:
        extracted["cost"] = pd.to_numeric(
            extracted["cost"],
            errors="coerce"
        ).fillna(0)
    
    # ---- USAGE CONVERSION (ensure float) ----
    if "usage" in extracted.columns:
        extracted["usage"] = pd.to_numeric(
            extracted["usage"],
            errors="coerce"
        ).fillna(0)
    
    # ---- USAGE TYPE STANDARDIZATION ----
    if "usage_type" in extracted.columns:
        extracted["usage_type"] = extracted["usage_type"].fillna("Unknown")
        extracted["usage_type"] = extracted["usage_type"].str.split("-").str[0]  # BoxUsage -> BoxUsage
    
    # ---- RESOURCE ID CLEANUP ----
    if "resource_id" in extracted.columns:
        # Remove rows with invalid resource IDs (empty or invalid)
        extracted["resource_id"] = extracted["resource_id"].fillna("")
        extracted["resource_id"] = extracted["resource_id"].str.strip()
    
    # ---- REGION CLEANUP ----
    if "region" in extracted.columns:
        extracted["region"] = extracted["region"].fillna("Unknown")
    
    # ---- ITEM TYPE FILTERING ----
    # Keep only usage charges (exclude tax, credits, etc)
    if "item_type" in extracted.columns:
        extracted = extracted[
            extracted["item_type"].isin(["Usage", "DiscountedUsage", "Fee"])
        ]
    
    return extracted


def aggregate_aws_cur_data(
    df: pd.DataFrame,
    group_by: Optional[list] = None
) -> pd.DataFrame:
    """
    Aggregate AWS CUR data to remove duplicates.
    Sums costs and usages within same group.
    
    Args:
        df: Extracted DataFrame
        group_by: Columns to group by (default: date, service, resource_id, usage_type)
    
    Returns:
        Aggregated DataFrame
    """
    if group_by is None:
        group_by = ["date", "service", "resource_id", "usage_type"]
    
    # Only group by columns that exist
    group_by = [col for col in group_by if col in df.columns]
    
    if not group_by:
        # If no grouping columns, just return data
        return df
    
    aggregated = df.groupby(
        group_by,
        as_index=False,
        dropna=False
    ).agg({
        "cost": "sum",
        "usage": "sum" if "usage" in df.columns else "first",
        "region": "first",
        "owner_tag": "first",
        "environment_tag": "first",
    }).round(6)
    
    return aggregated


# ===================== MAIN INGESTION PIPELINE =====================

def ingest_aws_cur(
    file_path: str,
    aggregate: bool = True,
    sample_rows: Optional[int] = None
) -> pd.DataFrame:
    """
    Complete AWS CUR ingestion pipeline.
    Load → Extract → Clean → Aggregate
    
    Args:
        file_path: Path to AWS CUR CSV or Parquet file
        aggregate: Whether to aggregate duplicate entries
        sample_rows: Optional - load only first N rows
    
    Returns:
        Clean, ready-to-use DataFrame
    
    Raises:
        FileNotFoundError: File doesn't exist
        ValueError: Invalid file format or no valid data
    
    Example:
        >>> df = ingest_aws_cur("data/aws_cur_2025_01.csv")
        >>> print(df.head())
        >>> print(df.dtypes)
        >>> print(f"Loaded {len(df)} records")
    """
    # Step 1: Load file
    print(f"Loading AWS CUR file: {file_path}")
    raw_df = load_aws_cur_file(file_path, sample_rows=sample_rows)
    print(f"✓ Loaded {len(raw_df)} rows, {len(raw_df.columns)} columns")
    
    # Step 2: Validate columns exist
    is_valid, message = validate_aws_cur_columns(raw_df)
    if not is_valid:
        raise ValueError(f"Validation failed: {message}")
    print(f"✓ {message}")
    
    # Step 3: Extract key fields
    print("Extracting AWS CUR fields...")
    extracted_df = extract_aws_cur_data(raw_df)
    print(f"✓ Extracted {len(extracted_df)} valid records")
    
    # Step 4: Aggregate (optional)
    if aggregate:
        print("Aggregating duplicate entries...")
        final_df = aggregate_aws_cur_data(extracted_df)
        print(f"✓ Aggregated to {len(final_df)} unique records")
    else:
        final_df = extracted_df
    
    # Step 5: Final cleanup
    # Select only expected columns
    expected_cols = [
        "date", "service", "cost", "usage_type", "resource_id",
        "usage", "region", "owner_tag", "environment_tag"
    ]
    final_cols = [col for col in expected_cols if col in final_df.columns]
    final_df = final_df[final_cols]
    
    print(f"✓ Final dataset: {len(final_df)} rows, {len(final_df.columns)} columns")
    
    return final_df


# ===================== UTILITIES =====================

def get_aws_cur_stats(df: pd.DataFrame) -> Dict:
    """
    Print summary statistics of loaded AWS CUR data.
    Useful for data quality checks.
    """
    stats = {
        "total_rows": len(df),
        "total_cost": df["cost"].sum() if "cost" in df.columns else 0,
        "unique_services": df["service"].nunique() if "service" in df.columns else 0,
        "unique_resources": df["resource_id"].nunique() if "resource_id" in df.columns else 0,
        "date_range": {
            "start": str(df["date"].min()) if "date" in df.columns else None,
            "end": str(df["date"].max()) if "date" in df.columns else None,
        },
        "columns_available": df.columns.tolist(),
    }
    return stats


def print_aws_cur_stats(df: pd.DataFrame) -> None:
    """Pretty-print AWS CUR data statistics."""
    stats = get_aws_cur_stats(df)
    
    print("\n" + "="*50)
    print(" AWS CUR DATA SUMMARY")
    print("="*50)
    print(f"Total Records: {stats['total_rows']:,}")
    print(f"Total Cost: ${stats['total_cost']:,.2f}")
    print(f"Unique Services: {stats['unique_services']}")
    print(f"Unique Resources: {stats['unique_resources']}")
    print(f"Date Range: {stats['date_range']['start']} to {stats['date_range']['end']}")
    print("="*50 + "\n")
