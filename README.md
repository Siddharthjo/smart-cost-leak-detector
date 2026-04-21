# Smart Cloud Cost Leak Detector

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688?style=flat&logo=fastapi&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)
![Cloud](https://img.shields.io/badge/Cloud-AWS%20%7C%20Azure%20%7C%20GCP-orange?style=flat)
![AI](https://img.shields.io/badge/AI-Claude%20(Anthropic)-blueviolet?style=flat)

**AI-powered cloud billing analysis that automatically detects hidden cost waste.**

Upload a billing export — or connect live credentials — and get a ranked list of cost leaks with dollar impact, severity scores, 30-day forecasts, and Claude-generated remediation commands. Built as a complete, production-style backend with a clean web UI.

---

## Demo

> _Screenshot placeholder — add `docs/demo.png` here_

```
┌─────────────────────────────────────────────────────────────┐
│  Leaks detected: 14       Estimated monthly waste: $4,312   │
│  HIGH: 3   MEDIUM: 8   LOW: 3                               │
│                                                             │
│  #1  HIGH    RUNAWAY_COST         AmazonS3     +340%  $1,820/mo │
│  #2  HIGH    ZOMBIE_RESOURCE      i-0abc123           $720/mo  │
│  #3  MEDIUM  UNTAGGED_RESOURCES   14 resources        $612/mo  │
│  ...                                                        │
└─────────────────────────────────────────────────────────────┘
```

---

## What It Does

- **Ingests** AWS Cost and Usage Reports, Azure Cost Management Exports, and GCP Billing Exports (CSV or Parquet)
- **Detects** nine categories of cost waste using statistical and structural analysis — not fixed magic-number thresholds
- **Scores** each finding by severity (HIGH / MEDIUM / LOW) with estimated monthly and annual waste in dollars
- **Forecasts** 30-day cost per service using linear regression over the billing window
- **Enriches** findings with Claude AI: root cause, exact CLI remediation command, and estimated fix time
- **Reports** to timestamped JSON and Markdown files, with delta detection (NEW vs EXISTING leaks across runs)
- **Serves** a web UI with drag-and-drop upload, currency selector (USD / EUR / GBP), and provider filter

---

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/your-username/smart-cost-leak-detector.git
cd smart-cost-leak-detector
pip install -r requirements.txt

# 2. Start the API server and web UI
uvicorn src.api:app --reload --port 8000

# 3. Or run the CLI directly against a billing file
python -m src.main --file data/raw/aws/synthetic_aws_cur_with_leaks.csv
```

Open `http://localhost:8000` to use the web interface, or `http://localhost:8000/docs` for the auto-generated Swagger UI.

---

## Architecture

```
Billing Export (CSV / Parquet)  ──or──  Live API Credentials
              │
              ▼
  ┌───────────────────┐
  │     Ingestion     │  csv_loader · file_validator · csv_type_detector
  └────────┬──────────┘
           │
           ▼
  ┌───────────────────┐
  │   Normalization   │  aws_normalizer · azure_normalizer · gcp_normalizer
  │                   │  → unified schema: date, service, cost, usage,
  └────────┬──────────┘    provider, resource_id, region, tags
           │
           ▼
  ┌─────────────────────────┐
  │   Feature Engineering   │  daily_cost_per_service · cost_trend
  │                         │  resource_lifespan · usage_cost_ratio
  └────────┬────────────────┘  z-score rolling baseline · 30d forecast
           │
           ▼
  ┌────────────────────────────────────────────────────┐
  │                 Leak Detectors (9)                 │
  │   rule_based · structural · ri_detector            │
  │   Independent — one detector failure does not      │
  │   abort the pipeline or suppress other results     │
  └────────┬───────────────────────────────────────────┘
           │
           ▼
  ┌───────────────────┐
  │  Severity Scoring │  waste_estimator · cost_context · scorer
  │                   │  → HIGH / MEDIUM / LOW + $monthly + $annual
  └────────┬──────────┘
           │
           ▼
  ┌───────────────────┐
  │   LLM Enrichment  │  Claude API (optional — --llm flag)
  │                   │  → root cause + exact fix command per leak
  └────────┬──────────┘
           │
           ▼
  ┌───────────────────┐
  │      Output       │  JSON report · Markdown report · Console · Web UI
  └───────────────────┘
```

The pipeline runs identically whether triggered from the CLI, the upload endpoint, or the live-credentials endpoints. Each stage has clean input/output contracts.

---

## Supported Leak Types

| Leak Type | Detection Logic | Typical Confidence |
|---|---|---|
| **ZOMBIE_RESOURCE** | Running 14+ days; usage ratio below service p25 and cost above p50 | HIGH |
| **IDLE_COMPUTE** | Low usage-to-cost ratio over minimum 3-day observation window | MEDIUM |
| **RUNAWAY_COST** | Daily cost ≥ 2.5σ above 7-day rolling baseline; growth > 30% | HIGH |
| **ALWAYS_ON_HIGH_COST** | Present on ≥ 90% of days; avg cost > $50/day; no ownership tags | MEDIUM |
| **ORPHANED_STORAGE** | Storage charges in date+region windows with no corresponding compute | HIGH |
| **IDLE_DATABASE** | Database active 7+ days with near-zero usage ratio and meaningful daily cost | MEDIUM |
| **SNAPSHOT_SPRAWL** | Snapshot/backup cost growing with no corresponding active parent resource | LOW |
| **UNTAGGED_RESOURCES** | Top-N resources by cost with missing owner / project / environment tags | MEDIUM |
| **RI_UNUSED_RESERVATION** | AWS Reserved Instance or Savings Plan paying for underutilised capacity | HIGH |

Statistical thresholds (p25 percentile, z-score) are computed from each billing dataset — the detectors adapt to your actual cost distribution rather than using hardcoded dollar values.

---

## Sample Output

**Console summary**

```
[HIGH]   RUNAWAY_COST       AmazonS3          $920.70/mo   z=3.41  ★ NEW
[HIGH]   ZOMBIE_RESOURCE    AmazonRDS         $360.00/mo   rds-idle-001
[MEDIUM] ZOMBIE_RESOURCE    AmazonEC2         $255.00/mo   i-zombie-ec2-999
```

**Markdown report (excerpt)**

```markdown
## Executive Summary
| Metric                  | Value        |
|-------------------------|--------------|
| Total leaks detected    | **3**        |
| New since last run      | **2**        |
| High severity           | 1            |
| Estimated monthly waste | **$1,535**   |
| Estimated annual waste  | **$18,428**  |

## 30-Day Cost Forecast
| Provider | Service   | Projected Monthly | Last 30d Actual | Trend      |
|----------|-----------|-------------------|-----------------|------------|
| AWS      | AmazonS3  | $49,419           | $3,069          | +1510% ⚠️  |
| AWS      | AmazonRDS | $360              | $360            | 0.0%       |
```

**Claude AI enrichment (--llm)**

```json
{
  "leak_type": "RUNAWAY_COST",
  "service": "AmazonS3",
  "recommendation": "Cost spike correlates with a new data pipeline writing
    un-lifecycled objects. Apply an S3 Lifecycle rule to transition objects
    to Glacier after 30 days.",
  "fix_command": "aws s3api put-bucket-lifecycle-configuration \
    --bucket YOUR_BUCKET --lifecycle-configuration file://lifecycle.json",
  "estimated_remediation_minutes": 15,
  "risk_level": "LOW"
}
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/health` | Health check |
| `POST` | `/api/analyze/upload` | Analyze an uploaded CSV or Parquet billing file |
| `POST` | `/api/analyze/aws` | Pull from AWS Cost Explorer and analyze |
| `POST` | `/api/analyze/azure` | Pull from Azure Cost Management and analyze |

Full request/response schemas at `http://localhost:8000/docs`.

### Upload endpoint parameters (multipart form)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `file` | File | required | Billing CSV or Parquet |
| `provider` | string | auto | `aws` / `azure` / `gcp` |
| `use_llm` | bool | `false` | Enrich findings with Claude AI |
| `llm_max` | int | `10` | Max leaks to send to the LLM |
| `api_key` | string | env var | Anthropic API key |
| `no_forecast` | bool | `false` | Skip 30-day forecast |
| `top_untagged` | int | `20` | Max untagged leaks to surface |

---

## Running with Claude AI Recommendations

Set your Anthropic API key, then pass `--llm`:

```bash
# CLI
export ANTHROPIC_API_KEY=sk-ant-...
python -m src.main \
  --file data/raw/aws/synthetic_aws_cur_with_leaks.csv \
  --llm \
  --llm-max 10

# API
curl -X POST http://localhost:8000/api/analyze/upload \
  -F "file=@billing.csv" \
  -F "use_llm=true" \
  -F "api_key=sk-ant-..."
```

Claude enriches each HIGH/MEDIUM leak with a root-cause hypothesis, an exact remediation command, an estimated fix time, and a risk assessment.

---

## Live Credentials Mode

Credentials are **never stored to disk or logged** at any level. They exist only within the function scope of the API call handler and are explicitly deleted before the response is returned.

```bash
# AWS Cost Explorer — last 30 days, requires ce:GetCostAndUsage permission
curl -X POST http://localhost:8000/api/analyze/aws \
  -H "Content-Type: application/json" \
  -d '{
    "access_key_id": "AKIA...",
    "secret_access_key": "...",
    "region": "eu-west-1",
    "days": 30,
    "use_llm": false
  }'

# Azure Cost Management — requires Cost Management Reader on the subscription
curl -X POST http://localhost:8000/api/analyze/azure \
  -H "Content-Type: application/json" \
  -d '{
    "subscription_id": "...",
    "tenant_id": "...",
    "client_id": "...",
    "client_secret": "...",
    "days": 30
  }'
```

---

## CLI Reference

```
python -m src.main [OPTIONS]

Required:
  --file PATH              Billing CSV or Parquet file

Optional:
  --provider {aws,azure,gcp}    Provider override (auto-detected if omitted)
  --output {json,markdown,both,console}
                                Output format (default: both)
  --llm                         Enrich HIGH/MEDIUM leaks with Claude AI
  --llm-max INT                 Max leaks to enrich (default: 10)
  --api-key STR                 Anthropic API key
  --no-forecast                 Skip 30-day cost forecast
  --top-untagged INT            Max untagged resource leaks to surface (default: 20)
```

---

## Docker

```dockerfile
# Build
docker build -t cost-leak-detector .

# Run the API server
docker run -p 8000:8000 \
  -e ANTHROPIC_API_KEY=sk-ant-... \
  cost-leak-detector

# Run CLI on a mounted file
docker run --rm \
  -v $(pwd)/data:/app/data \
  -e ANTHROPIC_API_KEY=sk-ant-... \
  cost-leak-detector \
  python -m src.main --file data/raw/aws/cur.csv --llm
```

> A `Dockerfile` is not yet in the repo — contributions welcome (see below).

---

## Project Structure

```
smart-cost-leak-detector/
├── src/
│   ├── main.py                     CLI entrypoint
│   ├── api.py                      FastAPI application
│   ├── pipeline.py                 Core pipeline (shared by CLI + API)
│   ├── ingestion/                  CSV/Parquet loading, validation, type detection
│   ├── normalization/              Per-provider normalizers → unified schema
│   ├── intelligence/
│   │   ├── feature_engineering/    Cost features, z-score, linear forecasts
│   │   ├── leak_detection/         9 independent detectors
│   │   ├── severity/               Waste estimation, scoring, cost percentiles
│   │   └── llm/                    Claude AI enrichment
│   ├── output/                     JSON + Markdown report writers, pretty printer
│   └── insights/                   Insight generator
├── frontend/
│   └── index.html                  Web UI — drag-and-drop, currency selector
├── tests/                          pytest suite (unit + integration)
├── data/
│   ├── raw/                        Billing exports (gitignored)
│   └── outputs/reports/            Generated timestamped reports
└── docs/                           Architecture, data flow, assumptions
```

---

## Running Tests

```bash
pip install pytest
pytest tests/ -v
```

No external API calls are made during tests. AWS and Azure calls are not exercised.

---

## Privacy and GDPR

**Designed with data minimisation in mind.**

- Uploaded files are parsed entirely in memory and never written to disk.
- AWS and Azure credentials are held only within the request handler function scope and are explicitly deleted (`del`) before the response returns.
- No billing data, resource identifiers, or credentials are written to logs at any severity level.
- All generated reports are written to the local `data/outputs/` directory only. No data is transmitted to external services unless you opt in to the `--llm` flag.

---

## Roadmap

- [ ] GCP live credentials mode (Cloud Billing API)
- [ ] Azure live API refinements — resource-level granularity
- [ ] Slack / Teams alert integration for new HIGH-severity leaks
- [ ] Scheduled analysis with cron mode
- [ ] Interactive dashboard with historical trend charts
- [ ] Terraform remediation plan generation
- [ ] AWS Organizations multi-account support
- [ ] Dockerfile + docker-compose

---

## Contributing

Contributions are welcome. The detectors in `src/intelligence/leak_detection/` are intentionally modular — adding a new one means implementing a single function that returns a list of leak dicts, then wiring it into `src/pipeline.py`.

```bash
# Fork, clone, then create a feature branch
git checkout -b feat/your-detector

# Run the test suite before opening a PR
pytest tests/ -v
```

Please keep PRs focused: one detector or one bug fix per PR.

---

## License

MIT — see [LICENSE](LICENSE) for details.

---

<sub>Built with FastAPI · pandas · numpy · Anthropic Claude API</sub>
