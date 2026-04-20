# Smart Cost Leak Detector

Detect cloud billing leaks across AWS, Azure, and GCP — from a CSV file or live AWS credentials. Surfaces waste patterns, scores by severity, and optionally enriches findings with Claude AI recommendations.

---

## Features

- **9 leak detectors**: zombie resources, idle compute, runaway costs, always-on unowned services, orphaned storage, idle databases, snapshot sprawl, untagged resources, RI/Savings Plan waste
- **Multi-cloud**: AWS Cost & Usage Report, Azure Cost Management Export, GCP Cloud Billing Export
- **Two input modes**: upload a billing file or pull live data from AWS Cost Explorer
- **30-day cost forecast** per service using linear regression
- **Severity scoring**: HIGH / MEDIUM / LOW with estimated monthly and annual waste
- **Claude AI recommendations** (optional): root cause, exact fix command, remediation time, risk level
- **Web UI + REST API**: FastAPI backend with a single-file HTML frontend
- **CLI**: run the full pipeline from the terminal

---

## Quick Start

### 1. Install

```bash
pip install -r requirements.txt
```

### 2. Run the web server

```bash
uvicorn src.api:app --reload --host 0.0.0.0 --port 8000
```

Open **http://localhost:8000** in your browser.

### 3. Or run from the CLI

```bash
python -m src.main --file data/raw/aws/cur.csv
python -m src.main --file data/raw/aws/cur.csv --llm --output both
python -m src.main --file data/raw/azure/usage.csv --provider azure
python -m src.main --file data/raw/aws/cur.csv --no-forecast --output console
```

---

## Input Modes

### File Upload

Supported formats: **CSV** and **Parquet**.

| Provider | Expected Format |
|----------|----------------|
| AWS | Cost & Usage Report (CUR) |
| Azure | Cost Management Export |
| GCP | Cloud Billing Export (flattened CSV) |

Provider is auto-detected from column signatures. Override with `--provider aws/azure/gcp`.

### AWS Live Credentials

The web UI and API accept temporary AWS credentials to pull from **Cost Explorer** directly. Credentials are:
- used only for the `ce:GetCostAndUsage` API call
- **never stored, logged, or written to disk**
- discarded from memory immediately after the request

Required IAM permission: `ce:GetCostAndUsage`

---

## API Reference

Base URL: `http://localhost:8000`

### `GET /api/health`
Returns server status.

### `POST /api/analyze/upload`
Analyze a billing file.

| Field | Type | Description |
|-------|------|-------------|
| `file` | File | CSV or Parquet billing export |
| `provider` | string (optional) | `aws`, `azure`, or `gcp` |
| `use_llm` | bool | Enable Claude AI enrichment (default: false) |
| `no_forecast` | bool | Skip 30-day forecast (default: false) |
| `top_untagged` | int | Max untagged resource leaks (default: 20) |
| `api_key` | string (optional) | Anthropic API key for LLM mode |

### `POST /api/analyze/aws`
Analyze via live AWS Cost Explorer.

```json
{
  "access_key_id": "AKIAIOSFODNN7EXAMPLE",
  "secret_access_key": "...",
  "session_token": "...",
  "region": "us-east-1",
  "days": 30,
  "use_llm": false,
  "no_forecast": false
}
```

### Response shape (both endpoints)

```json
{
  "summary": {
    "total_leaks": 12,
    "high": 3,
    "medium": 6,
    "low": 3,
    "estimated_monthly_waste_usd": 4200.00,
    "estimated_annual_waste_usd": 50400.00
  },
  "leaks": [
    {
      "leak_type": "RUNAWAY_COST",
      "provider": "AWS",
      "service": "ec2",
      "resource_id": "i-0abc1234",
      "severity": "HIGH",
      "severity_score": 88,
      "estimated_monthly_waste": 1200.00,
      "reason": "Cost spike: $240/day is 4.2 stddevs above 7-day baseline",
      "recommended_action": "Review recent deployments and set a cost budget alert"
    }
  ],
  "forecasts": [
    {
      "provider": "AWS",
      "service": "ec2",
      "projected_monthly_cost": 7800.00,
      "last_30d_actual": 6500.00,
      "trend_pct": 20.0
    }
  ],
  "pipeline_stats": {
    "provider": "AWS",
    "total_records": 45000,
    "normalized_records": 44800,
    "forecast_services": 12,
    "llm_enabled": false
  }
}
```

---

## Leak Types

| Leak Type | Description |
|-----------|-------------|
| `ZOMBIE_RESOURCE` | Long-running resource (14+ days) with usage-to-cost ratio below service p25 |
| `IDLE_RESOURCE` | Compute active 3+ days with low utilisation |
| `RUNAWAY_COST` | Daily cost 2.5+ standard deviations above 7-day rolling baseline |
| `ALWAYS_ON_HIGH_COST` | Compute/database averaging $50+/day with 90%+ presence and no ownership tags |
| `ORPHANED_STORAGE` | Storage billing in date+region windows where no compute was running |
| `IDLE_DATABASE` | Database active 7+ days with near-zero usage ratio and $10+/day cost |
| `SNAPSHOT_SPRAWL` | Snapshot or backup incurring cost with no matching active parent resource |
| `UNTAGGED_RESOURCE` | Resource with no owner/project/environment tag (top N by cost) |
| `RI_UNUSED_RESERVATION` | AWS Reserved Instance or Savings Plan paying for unused capacity |

---

## Claude AI Recommendations (optional)

Pass `--llm` (CLI) or `use_llm: true` (API) to enrich HIGH and MEDIUM findings with:

- **Root cause**: why the leak exists
- **Fix command**: exact AWS CLI / console action
- **Estimated remediation time**: in minutes
- **Risk level** and risk note
- **Priority reason**: why to act now

Requires an Anthropic API key — set via `--api-key` flag or `ANTHROPIC_API_KEY` environment variable.

---

## Project Structure

```
src/
├── api.py                    FastAPI backend
├── pipeline.py               Core detection pipeline (shared by CLI and API)
├── main.py                   CLI entry point
├── ingestion/                CSV/Parquet loading and validation
├── normalization/            AWS / Azure / GCP → unified schema
├── intelligence/
│   ├── feature_engineering/  Daily costs, z-scores, lifespan, usage ratios
│   ├── leak_detection/       Nine detector modules
│   ├── severity/             Scoring, percentiles, waste estimation
│   └── llm/                  Claude AI enrichment
└── output/                   Pretty printer, JSON and Markdown report writer

frontend/
└── index.html                Single-file web UI (Tailwind CSS + vanilla JS)

tests/
├── test_pipeline.py          Provider detection, dedup, integration
├── test_api.py               FastAPI endpoint tests
├── intelligence/             Rule-based and structural detector tests
└── normalization/            AWS, Azure, GCP normalizer tests
```

---

## Running Tests

```bash
python -m pytest tests/ -q
```

116 tests, no external dependencies required (AWS calls are not exercised in tests).

---

## AWS CUR Setup

To generate an AWS Cost & Usage Report:

1. Go to **Billing → Cost & Usage Reports** in the AWS Console
2. Create a report with: time granularity **Daily**, include resource IDs, format **CSV**
3. Download the exported CSV and pass it to `--file`

Minimum required columns: `line_item_usage_start_date`, `product_servicecode`, `line_item_unblended_cost`
