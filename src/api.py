"""
FastAPI backend for Smart Cost Leak Detector.

Security policy: AWS credentials are NEVER stored to disk or logged.
They are used only to create an in-memory boto3 session, then discarded.
"""

import io
import logging
import math
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, SecretStr

from src.pipeline import run_pipeline_from_df


# ===================== HELPERS =====================

def sanitize_floats(obj):
    """Replace NaN/Inf floats with None so JSONResponse never produces invalid JSON."""
    if isinstance(obj, dict):
        return {k: sanitize_floats(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_floats(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj

logger = logging.getLogger(__name__)

# ===================== APP =====================

app = FastAPI(
    title="Smart Cost Leak Detector",
    description="Detect cloud cost leaks from billing files or live AWS credentials",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ===================== MODELS =====================

class AWSCredentialsRequest(BaseModel):
    access_key_id: str = Field(..., description="AWS Access Key ID")
    secret_access_key: SecretStr = Field(..., description="AWS Secret Access Key — never stored")
    session_token: Optional[SecretStr] = Field(None, description="STS session token for temporary credentials")
    region: str = Field("us-east-1", description="AWS region for your account")
    days: int = Field(30, ge=7, le=90, description="Days of billing history to pull from Cost Explorer")
    use_llm: bool = Field(False, description="Enrich findings with Claude AI recommendations")
    llm_max: int = Field(10, ge=1, le=20, description="Max leaks to send to LLM")
    api_key: Optional[str] = Field(None, description="Anthropic API key (or set ANTHROPIC_API_KEY env var)")
    no_forecast: bool = Field(False, description="Skip 30-day cost forecast")
    top_untagged: int = Field(20, ge=1, le=100, description="Max untagged resource leaks to surface")


# ===================== ENDPOINTS =====================

@app.get("/api/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}


@app.post("/api/analyze/upload")
async def analyze_upload(
    file: UploadFile = File(..., description="AWS/Azure/GCP billing CSV or Parquet file"),
    provider: Optional[str] = Form(None, description="Provider override: aws / azure / gcp"),
    use_llm: bool = Form(False),
    llm_max: int = Form(10),
    api_key: Optional[str] = Form(None),
    no_forecast: bool = Form(False),
    top_untagged: int = Form(20),
):
    """
    Analyze billing data from an uploaded CSV or Parquet file.
    File is parsed in memory — never written to disk.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    contents = await file.read()
    fname = file.filename.lower()

    try:
        if fname.endswith(".parquet") or fname.endswith(".pqt"):
            df = pd.read_parquet(io.BytesIO(contents))
        else:
            df = pd.read_csv(io.BytesIO(contents))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {exc}")

    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file contains no data rows")

    try:
        result = run_pipeline_from_df(
            df,
            provider=provider or None,
            use_llm=use_llm,
            llm_max=llm_max,
            api_key=api_key or None,
            no_forecast=no_forecast,
            top_untagged=top_untagged,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        logger.exception("Pipeline error in upload mode")
        raise HTTPException(status_code=500, detail=f"Pipeline error: {exc}")

    return JSONResponse(content=sanitize_floats(result))


@app.post("/api/analyze/aws")
def analyze_aws(req: AWSCredentialsRequest):
    """
    Analyze costs using live AWS Cost Explorer API.

    Credentials are used only to make the Cost Explorer API call and are
    never written to disk, never logged, and discarded after the request.
    """
    try:
        df = _fetch_cost_explorer(
            access_key_id=req.access_key_id,
            secret_access_key=req.secret_access_key.get_secret_value(),
            session_token=req.session_token.get_secret_value() if req.session_token else None,
            region=req.region,
            days=req.days,
        )
    except ImportError as exc:
        raise HTTPException(
            status_code=500,
            detail="boto3 is required for AWS live mode. Install with: pip install boto3",
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"AWS Cost Explorer error: {exc}")

    try:
        result = run_pipeline_from_df(
            df,
            provider="AWS",
            use_llm=req.use_llm,
            llm_max=req.llm_max,
            api_key=req.api_key or None,
            no_forecast=req.no_forecast,
            top_untagged=req.top_untagged,
            already_normalized=True,
        )
    except Exception as exc:
        logger.exception("Pipeline error in AWS API mode")
        raise HTTPException(status_code=500, detail=f"Pipeline error: {exc}")

    return JSONResponse(content=sanitize_floats(result))


# ===================== AWS COST EXPLORER =====================

def _fetch_cost_explorer(
    access_key_id: str,
    secret_access_key: str,
    session_token: Optional[str],
    region: str,
    days: int,
) -> pd.DataFrame:
    """
    Pull daily cost-by-service from AWS Cost Explorer.
    Credentials live only in this function scope and are deleted before return.
    Never logged, never persisted.
    """
    import boto3  # optional dependency — deferred import

    end   = datetime.utcnow().date()
    start = end - timedelta(days=days)

    session = boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token,
        region_name=region,
    )
    # Cost Explorer endpoint is always us-east-1
    ce = session.client("ce", region_name="us-east-1")

    records    = []
    next_token = None

    while True:
        kwargs: dict = {
            "TimePeriod": {
                "Start": start.strftime("%Y-%m-%d"),
                "End":   end.strftime("%Y-%m-%d"),
            },
            "Granularity": "DAILY",
            "Metrics":     ["BlendedCost", "UsageQuantity"],
            "GroupBy":     [{"Type": "DIMENSION", "Key": "SERVICE"}],
        }
        if next_token:
            kwargs["NextPageToken"] = next_token

        resp = ce.get_cost_and_usage(**kwargs)

        for result in resp.get("ResultsByTime", []):
            date_val = pd.to_datetime(result["TimePeriod"]["Start"]).date()
            for group in result.get("Groups", []):
                service = group["Keys"][0]
                cost    = float(group["Metrics"]["BlendedCost"]["Amount"])
                usage   = float(group["Metrics"].get("UsageQuantity", {}).get("Amount", 0))
                if cost > 0:
                    records.append({
                        "date":        date_val,
                        "service":     service,
                        "cost":        cost,
                        "usage":       usage,
                        "provider":    "AWS",
                        "resource_id": "",
                        "region":      region,
                    })

        next_token = resp.get("NextPageToken")
        if not next_token:
            break

    # Explicitly clear credentials from local scope
    del secret_access_key, session_token, session, ce

    if not records:
        raise ValueError(
            "No cost data returned from AWS Cost Explorer for the specified period. "
            "Verify your credentials have ce:GetCostAndUsage permission."
        )

    return pd.DataFrame(records)


# ===================== STATIC FRONTEND =====================
# Mount last so API routes take priority

import os

_frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(_frontend_dir):
    app.mount("/", StaticFiles(directory=_frontend_dir, html=True), name="frontend")
