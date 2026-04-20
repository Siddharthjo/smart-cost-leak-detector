# AWS CUR Ingestion Module

Complete AWS Cost & Usage Report (CUR) ingestion pipeline for smart-cost-leak-detector.

## Overview

The AWS CUR loader provides:
- **CSV & Parquet Support**: Load from compressed or uncompressed AWS CUR exports
- **Auto-Detection**: Automatically maps AWS column names to standardized fields
- **Data Cleaning**: Handles missing values, type conversions, duplicate aggregation
- **Validation**: Ensures data quality before processing
- **Statistics**: Built-in data profiling and quality checks

## Quick Start

### Basic Usage (3 lines)

```python
from src.ingestion.aws_cur_loader import ingest_aws_cur

df = ingest_aws_cur("data/aws_cur_2025_01.csv")
print(df.head())
```

### Output

```
        date service  cost usage_type     resource_id region owner_tag environment_tag
0 2025-01-01      EC2 50.00    BoxUsage  i-0123456789 us-east-1      devops          prod
1 2025-01-01       S3 12.50  StandardIO  s3://bucket-1 us-east-1       data          prod
2 2025-01-01      RDS 150.00  StorageIO  rds-instance-1 us-east-1      db-team      prod
```

## API Reference

### Core Functions

#### `ingest_aws_cur(file_path, aggregate=True, sample_rows=None)`

Complete ingestion pipeline. Use this for most cases.

**Parameters:**
- `file_path` (str): Path to CSV or Parquet file
- `aggregate` (bool): Sum duplicate entries (default: True)
- `sample_rows` (int): Load only first N rows for testing (default: None = all)

**Returns:** Clean pandas DataFrame

**Raises:**
- `FileNotFoundError`: File doesn't exist
- `ValueError`: Unsupported format or invalid data

**Examples:**

```python
# Load full file

df = ingest_aws_cur("data/aws_cur.csv")

# Load sample for testing

df_sample = ingest_aws_cur("data/aws_cur.csv", sample_rows=10000)

# Load Parquet (faster for large files)

df = ingest_aws_cur("data/aws_cur.parquet")

# Get raw data (no aggregation)

df = ingest_aws_cur("data/aws_cur.csv", aggregate=False)
```

---

#### `load_aws_cur_file(file_path, sample_rows=None)`

Load raw file (CSV or Parquet). Returns unprocessed DataFrame.

**Parameters:**
- `file_path` (str): Path to file
- `sample_rows` (int): Optional row limit

**Returns:** Raw pandas DataFrame

**Raises:** FileNotFoundError, ValueError

**Example:**

```python
from src.ingestion.aws_cur_loader import load_aws_cur_file

raw_df = load_aws_cur_file("data/aws_cur.csv")
print(raw_df.shape)  # (1000000, 89)
```

---

#### `detect_aws_cur_columns(df)`

Auto-detect AWS CUR columns in DataFrame.

**Parameters:**
- `df` (DataFrame): Raw AWS CUR data

**Returns:** Dict mapping original column names to standardized names

**Example:**

```python
from src.ingestion.aws_cur_loader import detect_aws_cur_columns

mapping = detect_aws_cur_columns(raw_df)
# {
#   'line_item_usage_start_date': 'date',
#   'product_servicecode': 'service',
#   'line_item_resource_id': 'resource_id',
#   'line_item_usage_amount': 'usage',
#   'line_item_unblended_cost': 'cost',
#   ...
# }
```

---

#### `validate_aws_cur_columns(df, required=False)`

Validate that DataFrame has necessary AWS CUR columns.

**Parameters:**
- `df` (DataFrame): Data to validate
- `required` (bool): Strict validation if True (default: False)

**Returns:** Tuple of (bool, str) - (is_valid, message)

**Example:**

```python
from src.ingestion.aws_cur_loader import validate_aws_cur_columns

is_valid, msg = validate_aws_cur_columns(raw_df)
if not is_valid:
    raise ValueError(f"Invalid AWS CUR: {msg}")
```

---

#### `extract_aws_cur_data(df, column_mapping=None)`

Extract and clean key AWS CUR fields.

**Parameters:**
- `df` (DataFrame): Raw AWS CUR data
- `column_mapping` (dict): Custom mapping (auto-detected if None)

**Returns:** Clean DataFrame with standardized columns

**Example:**

```python
from src.ingestion.aws_cur_loader import extract_aws_cur_data

extracted = extract_aws_cur_data(raw_df)
print(extracted.columns)  # date, service, cost, usage_type, resource_id, usage, region, ...
```

---

#### `aggregate_aws_cur_data(df, group_by=None)`

Aggregate duplicate entries, summing costs and usage.

**Parameters:**
- `df` (DataFrame): Extracted data
- `group_by` (list): Columns to group by (default: date, service, resource_id, usage_type)

**Returns:** Aggregated DataFrame

**Example:**

```python
from src.ingestion.aws_cur_loader import aggregate_aws_cur_data

# Default aggregation
agg_df = aggregate_aws_cur_data(extracted_df)

# Custom grouping (by day and service only)
agg_df = aggregate_aws_cur_data(extracted_df, group_by=["date", "service"])
```

---

#### `get_aws_cur_stats(df)`

Get summary statistics of loaded data.

**Parameters:**
- `df` (DataFrame): AWS CUR data

**Returns:** Dict with summary stats

**Example:**

```python
from src.ingestion.aws_cur_loader import get_aws_cur_stats

stats = get_aws_cur_stats(df)
print(stats)
# {
#   'total_rows': 1000000,
#   'total_cost': 45000.50,
#   'unique_services': 12,
#   'unique_resources': 5000,
#   'date_range': {'start': '2025-01-01', 'end': '2025-01-31'},
#   'columns_available': [...] 
# }
```

---

#### `print_aws_cur_stats(df)`

Pretty-print AWS CUR data statistics to console.

**Parameters:**
- `df` (DataFrame): AWS CUR data

**Example:**

```python
from src.ingestion.aws_cur_loader import print_aws_cur_stats

print_aws_cur_stats(df)
# ==================================================
#  AWS CUR DATA SUMMARY
# ==================================================
# Total Records: 1,000,000
# Total Cost: $45,000.50
# Unique Services: 12
# Unique Resources: 5,000
# Date Range: 2025-01-01 to 2025-01-31
# ==================================================
```

## Supported AWS CUR Columns

### Input Columns Detected

| Original Column | Standard Name | Description |
|---|---|---|
| `line_item_usage_start_date` | `date` | Date of usage |
| `product_servicecode` | `service` | AWS service code (EC2, S3, etc) |
| `line_item_resource_id` | `resource_id` | AWS resource ID |
| `line_item_usage_type` | `usage_type` | Usage type (BoxUsage, etc) |
| `line_item_usage_amount` | `usage` | Quantity used |
| `line_item_unblended_cost` | `cost` | Unblended cost in USD |
| `product_region` | `region` | AWS region |
| `resource_tags_user_owner` | `owner_tag` | Owner tag value |
| `resource_tags_user_environment` | `environment_tag` | Environment tag value |
| `line_item_line_item_type` | `item_type` | Fee type |

### Output Columns

After ingestion, DataFrame contains:

```
date                 : date           - Date of usage
service              : string         - Human-readable service name
cost                 : float          - Cost in USD
usage_type           : string         - Type of usage
resource_id          : string         - AWS resource identifier
usage                : float          - Usage quantity
region               : string         - AWS region
owner_tag            : string         - Owner tag (if available)
environment_tag      : string         - Environment tag (if available)
```

## Service Code Mapping

AWS service codes are automatically mapped to readable names:

| Code | Name |
|---|---|
| `AmazonEC2` | `EC2` |
| `AmazonRDS` | `RDS` |
| `AmazonS3` | `S3` |
| `AWSLambda` | `Lambda` |
| `AmazonDynamoDB` | `DynamoDB` |
| `AmazonElastiCache` | `ElastiCache` |
| `AmazonCloudFront` | `CloudFront` |
| ... | ... |

## Data Cleaning Rules

### Dates
- Converted to Python `date` objects
- Invalid dates dropped
- Validates format automatically

### Costs & Usage
- Converted to `float`
- Invalid values replaced with 0
- Rounded to 6 decimal places

### Service Names
- Mapped from service codes (EC2 → readable EC2)
- Missing values filled with "Unknown"
- Stripped of whitespace

### Resource IDs
- Whitespace trimmed
- Empty/null values removed
- Rows without valid resource_id can still be analyzed at service level

### Item Types
- Filters to usage charges only (excludes tax, credits, support)
- Keeps: Usage, DiscountedUsage, Fee

## Integration with Pipeline

### With Normalization

```python
from src.ingestion.aws_cur_loader import ingest_aws_cur
from src.normalization.aws_normalizer import normalize_aws

# Load AWS CUR
df = ingest_aws_cur("data/aws_cur.csv")

# Already in normalized format, just verify
normalized = normalize_aws(df)
```

### With Leak Detection

```python
from src.ingestion.aws_cur_loader import ingest_aws_cur
from src.intelligence.feature_engineering.cost_features import daily_cost_per_service

df = ingest_aws_cur("data/aws_cur.csv")
df["provider"] = "AWS"

# Compute features
daily_costs = daily_cost_per_service(df)
```

### Full Pipeline Example

```python
from src.ingestion.aws_cur_loader import ingest_aws_cur, print_aws_cur_stats
from src.intelligence.feature_engineering.cost_features import daily_cost_per_service

# Load
df = ingest_aws_cur("data/aws_cur.csv")
print_aws_cur_stats(df)

# Add provider
df["provider"] = "AWS"

# Analyze
daily = daily_cost_per_service(df)
print(daily.head())
```

## Performance Tips

### Large Files (> 100MB)

Use Parquet format instead of CSV:

```python
# Slow
df = ingest_aws_cur("data/aws_cur.csv")

# Fast (10x faster)
df = ingest_aws_cur("data/aws_cur.parquet")
```

### Memory Constraints

Load samples first:

```python
# Quick test (100k rows)
df_test = ingest_aws_cur("data/aws_cur.csv", sample_rows=100000)

# Then process full file
df_full = ingest_aws_cur("data/aws_cur.csv")
```

### Aggregation Overhead

Disable aggregation if not needed:

```python
# With aggregation (slower, less memory)
df = ingest_aws_cur("data/aws_cur.csv", aggregate=True)

# Without aggregation (faster, more memory)
df = ingest_aws_cur("data/aws_cur.csv", aggregate=False)
```

## Error Handling

### Common Errors

**FileNotFoundError**
```python
try:
    df = ingest_aws_cur("data/missing.csv")
except FileNotFoundError:
    print("File not found. Check path.")
```

**Invalid Format**
```python
try:
    df = ingest_aws_cur("data/file.xlsx")  # Not CSV/Parquet
except ValueError as e:
    print(f"Invalid format: {e}")
```

**No Valid Data**
```python
try:
    df = ingest_aws_cur("data/corrupted.csv")
except ValueError as e:
    print(f"No valid data: {e}")
```

## Examples

See `aws_cur_examples.py` for 11 complete working examples including:
1. Basic loading
2. Parquet loading
3. Sample loading
4. Step-by-step extraction
5. Raw extraction (no aggregation)
6. Cost breakdown analysis
7. Usage efficiency analysis
8. Daily trend analysis
9. Service filtering
10. Export for pipeline
11. Error handling

Run any example:

```bash
python -c "from src.ingestion.aws_cur_examples import example_basic_load; example_basic_load()"
```

## Testing

AWS CUR ingestion is fully tested. See test files for examples.

```bash
pytest tests/ingestion/test_aws_cur_loader.py -v
```

## Requirements

- pandas >= 1.0
- pyarrow >= 5.0 (for Parquet support)

```bash
pip install pandas pyarrow
```

## Contributing

To add new features:
1. Add function to `aws_cur_loader.py`
2. Add example to `aws_cur_examples.py`
3. Add tests to `tests/ingestion/test_aws_cur_loader.py`
4. Update this README

## License

Same as smart-cost-leak-detector project.
