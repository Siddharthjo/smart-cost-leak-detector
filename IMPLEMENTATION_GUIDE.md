# AWS CUR Ingestion Implementation Guide

This guide explains the new AWS Cost & Usage Report (CUR) ingestion module and how to integrate it into your existing pipeline.

## What Was Created

### 1. Core Module: `src/ingestion/aws_cur_loader.py` (400+ lines)

**Main Functions:**
- `load_aws_cur_file()` - Load CSV or Parquet files
- `detect_aws_cur_columns()` - Auto-detect AWS column mappings
- `validate_aws_cur_columns()` - Validate data quality
- `extract_aws_cur_data()` - Extract key fields and clean data
- `aggregate_aws_cur_data()` - Sum duplicate entries
- `ingest_aws_cur()` - **Main entry point (use this!)**
- `get_aws_cur_stats()` - Generate summary statistics
- `print_aws_cur_stats()` - Pretty-print stats

**Features:**
✅ CSV & Parquet support  
✅ Auto-detection of AWS column names  
✅ Data cleaning & validation  
✅ Type conversions (dates, floats, service codes)  
✅ Duplicate aggregation  
✅ Service code mapping (AmazonEC2 → EC2)  
✅ Statistics & profiling  
✅ Comprehensive error handling  

---

### 2. Examples: `src/ingestion/aws_cur_examples.py` (200+ lines)

**11 Working Examples:**
1. Basic CSV loading
2. Parquet loading
3. Sample loading (first N rows)
4. Step-by-step extraction (debugging)
5. Raw extraction (no aggregation)
6. Cost breakdown by service
7. Usage analysis & efficiency
8. Daily trends
9. Service filtering
10. Export for pipeline
11. Error handling patterns

Each example is self-contained and runnable.

---

### 3. Documentation: `src/ingestion/aws_cur_readme.md` (500+ lines)

Complete API reference including:
- Quick start (3-line example)
- Full API documentation
- Supported columns
- Service code mapping
- Data cleaning rules
- Integration examples
- Performance tips
- Error handling
- Testing info

---

### 4. Tests: `tests/ingestion/test_aws_cur_loader.py` (400+ lines)

**47 Test Cases** covering:
- ✅ File loading (CSV, Parquet, sample rows)
- ✅ Column detection & mapping
- ✅ Data validation
- ✅ Field extraction & cleaning
- ✅ Data aggregation
- ✅ Full pipeline
- ✅ Statistics
- ✅ Edge cases (zero costs, missing IDs, etc)
- ✅ Integration with normalization

Run tests:
```bash
pytest tests/ingestion/test_aws_cur_loader.py -v
```

---

## Quick Start (3 Lines)

```python
from src.ingestion.aws_cur_loader import ingest_aws_cur

df = ingest_aws_cur("data/aws_cur_2025_01.csv")
print(df.head())
```

**Output:**
```
        date service  cost usage_type     resource_id region
0 2025-01-01      EC2 50.00    BoxUsage  i-0123456789 us-east-1
1 2025-01-01       S3 12.50  StandardIO  s3://bucket-1 us-east-1
2 2025-01-01      RDS 150.00  StorageIO  rds-instance-1 us-east-1
```

---

## Integration Points

### Option 1: Replace Existing CSV Loader (Recommended)

**Before:**
```python
# src/ingestion/csv_loader.py (generic)
df = load_csv("data/aws_cur.csv")
```

**After:**
```python
# src/ingestion/aws_cur_loader.py (AWS-specific)
from src.ingestion.aws_cur_loader import ingest_aws_cur

df = ingest_aws_cur("data/aws_cur.csv")
```

**Benefits:**
- AWS-specific column handling
- Automatic service code mapping
- Data aggregation & cleaning
- Better error messages

---

### Option 2: Use with Existing Normalization

The AWS CUR loader output is already close to normalized format!

```python
from src.ingestion.aws_cur_loader import ingest_aws_cur
from src.normalization.aws_normalizer import normalize_aws

# Load AWS CUR
df = ingest_aws_cur("data/aws_cur.csv")

# Add provider (required by normalizer)
df["provider"] = "AWS"

# Normalize (mostly just schema validation)
normalized_df = normalize_aws(df)
```

---

### Option 3: Use in Main Pipeline

Integrate into `src/main.py`:

```python
# ================== INPUT ==================

from src.ingestion.aws_cur_loader import ingest_aws_cur

# Use AWS CUR loader instead of generic CSV loader
df = ingest_aws_cur("data/raw/aws/synthetic_aws_cur_guaranteed_leaks.csv")

# No need to validate - already done by loader
print(f"Loaded {len(df)} records from AWS CUR")


# ================== PROVIDER DETECTION ==================

# Skip provider detection - we know it's AWS
provider = "AWS"
print(f"Provider: {provider}")


# ================== NORMALIZATION ==================

df["provider"] = "AWS"
normalized_df = normalize_aws(df)


# ================== REST OF PIPELINE UNCHANGED ==================

daily_cost_df = daily_cost_per_service(normalized_df)
trend_results = cost_trend_per_service(daily_cost_df)
# ... [rest of existing code]
```

---

## File Structure

```
src/ingestion/
├── csv_loader.py                 (existing - generic)
├── file_validator.py             (existing)
├── csv_type_detector.py          (existing)
├── aws_cur_loader.py             ✨ NEW - AWS-specific ingestion
├── aws_cur_examples.py           ✨ NEW - 11 working examples
└── aws_cur_readme.md             ✨ NEW - Complete documentation

tests/ingestion/
├── test_aws_cur_loader.py        ✨ NEW - 47 unit tests
```

---

## API Reference (Summary)

### Main Entry Point

```python
df = ingest_aws_cur(
    file_path="data/aws_cur.csv",
    aggregate=True,              # Sum duplicates?
    sample_rows=None             # Load first N rows?
)
```

### Low-Level Functions

```python
# For advanced users / debugging
raw_df = load_aws_cur_file(file_path)
mapping = detect_aws_cur_columns(raw_df)
is_valid, msg = validate_aws_cur_columns(raw_df)
extracted = extract_aws_cur_data(raw_df)
aggr...