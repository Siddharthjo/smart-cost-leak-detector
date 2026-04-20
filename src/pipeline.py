"""
Core detection pipeline — shared by CLI (main.py) and API (api.py).
"""

import logging
from datetime import date
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ===================== IMPORTS =====================

from src.normalization.aws_normalizer import normalize_aws
from src.normalization.azure_normalizer import normalize_azure
from src.normalization.gcp_normalizer import normalize_gcp

from src.intelligence.feature_engineering.cost_features import (
    daily_cost_per_service,
    cost_trend_per_service,
    resource_lifespan,
    usage_cost_ratio,
)
from src.intelligence.feature_engineering.anomaly_features import (
    compute_cost_zscore,
    compute_30day_forecast,
)
from src.intelligence.severity.cost_context import build_cost_percentiles

from src.intelligence.leak_detection.rule_based import (
    detect_idle_resources,
    detect_zombie_resources,
    detect_runaway_costs,
    detect_always_on_high_cost,
)
from src.intelligence.leak_detection.structural import (
    detect_orphaned_storage,
    detect_idle_databases,
    detect_snapshot_sprawl,
    detect_untagged_resources,
)
from src.intelligence.leak_detection.ri_detector import detect_reserved_instance_waste

from src.intelligence.severity.scorer import score_leaks
from src.intelligence.llm.recommender import enrich_leaks_with_llm

from src.output.pretty_printer import select_primary_leaks


# ===================== HELPERS =====================

def detect_provider(df: pd.DataFrame, override: Optional[str] = None) -> str:
    if override:
        return override.upper()

    if "provider" in df.columns:
        return df["provider"].iloc[0].upper()

    cols = set(df.columns)
    matches = {
        "AWS": bool(
            {"line_item_usage_account_id", "line_item_line_item_type",
             "bill_payer_account_id"} & cols
            or
            {"lineItem/UsageAccountId", "lineItem/LineItemType",
             "bill/PayerAccountId"} & cols
        ),
        "AZURE": bool({"SubscriptionId", "UsageDate", "MeterName"} & cols),
        "GCP":   bool({"billing_account_id", "project_id", "service_description"} & cols),
    }

    matched = [k for k, v in matches.items() if v]
    if len(matched) != 1:
        raise ValueError(
            f"Ambiguous or unknown billing format. Signals found: {matches}. "
            "Specify provider explicitly."
        )
    return matched[0]


def dedupe_leaks(leaks: list) -> list:
    seen = set()
    unique = []
    for leak in leaks:
        key = (
            leak.get("leak_type"),
            leak.get("provider"),
            leak.get("service"),
            leak.get("resource_id"),
        )
        if key not in seen:
            seen.add(key)
            unique.append(leak)
    return unique


def _serialize(obj):
    """Recursively convert date/datetime objects to ISO strings for JSON safety."""
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(v) for v in obj]
    if isinstance(obj, date):
        return obj.isoformat()
    return obj


# ===================== PIPELINE =====================

def run_pipeline_from_df(
    raw_df: pd.DataFrame,
    provider: Optional[str] = None,
    use_llm: bool = False,
    llm_max: int = 10,
    api_key: Optional[str] = None,
    no_forecast: bool = False,
    top_untagged: int = 20,
    already_normalized: bool = False,
) -> dict:
    """
    Run the full leak detection pipeline on a DataFrame.

    Args:
        raw_df:             Raw billing DataFrame (or pre-normalized when already_normalized=True).
        provider:           Provider override ("aws"/"azure"/"gcp"). Auto-detected if None.
        use_llm:            Enrich HIGH/MEDIUM leaks with Claude AI recommendations.
        llm_max:            Max leaks to send to LLM.
        api_key:            Anthropic API key (falls back to ANTHROPIC_API_KEY env var).
        no_forecast:        Skip 30-day cost forecast.
        top_untagged:       Max untagged resource leaks to surface.
        already_normalized: True when raw_df is already in unified schema format
                            (e.g. built from AWS Cost Explorer API). Skips normalization
                            and RI detection (which needs raw CUR columns).

    Returns:
        dict with keys: summary, leaks, forecasts, pipeline_stats
    """

    def _safe(fn, *a, **kw):
        try:
            return fn(*a, **kw)
        except Exception as exc:
            logger.warning(f"Detector {fn.__name__} skipped: {exc}")
            return [] if fn.__name__ != "detect_zombie_resources" else ([], set())

    # ---- NORMALIZATION ----
    if already_normalized:
        normalized_df = raw_df.copy()
        detected_provider = (provider or "AWS").upper()
        normalized_df["provider"] = detected_provider
        logger.info(f"Skipping normalization (pre-normalized). Provider: {detected_provider}")
    else:
        try:
            detected_provider = detect_provider(raw_df, provider)
        except ValueError as exc:
            raise

        logger.info(f"Provider: {detected_provider}")

        try:
            if detected_provider == "AWS":
                normalized_df = normalize_aws(raw_df)
            elif detected_provider == "AZURE":
                normalized_df = normalize_azure(raw_df)
            elif detected_provider == "GCP":
                normalized_df = normalize_gcp(raw_df)
            else:
                raise ValueError(f"Unsupported provider: {detected_provider}")
        except Exception as exc:
            raise ValueError(f"Normalization failed: {exc}") from exc

        normalized_df["provider"] = detected_provider

    logger.info(f"Records to analyze: {len(normalized_df):,}")

    # ---- FEATURE ENGINEERING ----
    daily_cost_df    = daily_cost_per_service(normalized_df)
    daily_cost_df    = compute_cost_zscore(daily_cost_df)
    _                = cost_trend_per_service(daily_cost_df)
    lifespan_results = resource_lifespan(normalized_df)
    ratio_results    = usage_cost_ratio(normalized_df)
    percentiles      = build_cost_percentiles(normalized_df)

    if not ratio_results:
        logger.warning("No usage data found — zombie/idle detectors will produce no results.")

    # ---- 30-DAY FORECAST ----
    forecasts = []
    if not no_forecast:
        try:
            forecasts = compute_30day_forecast(daily_cost_df)
            logger.info(f"Forecast computed for {len(forecasts)} services")
        except Exception as exc:
            logger.warning(f"Forecast failed (non-fatal): {exc}")

    # ---- LEAK DETECTION ----
    zombie_leaks, zombie_ids = _safe(
        detect_zombie_resources, lifespan_results, ratio_results, percentiles
    )
    idle_leaks      = _safe(detect_idle_resources,      lifespan_results, ratio_results, daily_cost_df, zombie_ids)
    runaway_leaks   = _safe(detect_runaway_costs,       daily_cost_df, ratio_results)
    always_on_leaks = _safe(detect_always_on_high_cost, daily_cost_df, normalized_df)
    orphaned_leaks  = _safe(detect_orphaned_storage,    normalized_df)
    idle_db_leaks   = _safe(detect_idle_databases,      lifespan_results, ratio_results, daily_cost_df, normalized_df)
    snapshot_leaks  = _safe(detect_snapshot_sprawl,     normalized_df)
    untagged_leaks  = _safe(detect_untagged_resources,  normalized_df, daily_cost_df, top_untagged)

    ri_leaks = []
    if detected_provider == "AWS" and not already_normalized:
        ri_leaks = _safe(detect_reserved_instance_waste, raw_df)

    all_leaks = dedupe_leaks(
        zombie_leaks + idle_leaks + runaway_leaks + always_on_leaks
        + orphaned_leaks + idle_db_leaks + snapshot_leaks
        + untagged_leaks + ri_leaks
    )
    logger.info(f"Unique leaks: {len(all_leaks)}")

    # ---- SCORING ----
    scored_leaks  = score_leaks(all_leaks, daily_cost_df, lifespan_results)
    primary_leaks = select_primary_leaks(scored_leaks)

    # ---- LLM ENRICHMENT ----
    if use_llm:
        logger.info("Enriching with Claude AI recommendations...")
        primary_leaks = enrich_leaks_with_llm(
            primary_leaks,
            api_key=api_key,
            max_leaks=llm_max,
        )

    # ---- BUILD RESPONSE ----
    total_monthly = sum(l.get("estimated_monthly_waste", 0) for l in primary_leaks)

    summary = {
        "total_leaks":                       len(primary_leaks),
        "high":   sum(1 for l in primary_leaks if l.get("severity") == "HIGH"),
        "medium": sum(1 for l in primary_leaks if l.get("severity") == "MEDIUM"),
        "low":    sum(1 for l in primary_leaks if l.get("severity") == "LOW"),
        "estimated_monthly_waste_usd":        round(total_monthly, 2),
        "estimated_annual_waste_usd":         round(total_monthly * 12, 2),
    }

    pipeline_stats = {
        "provider":                   detected_provider,
        "total_records":              len(raw_df),
        "normalized_records":         len(normalized_df),
        "forecast_services":          len(forecasts),
        "llm_enabled":                use_llm,
    }

    return {
        "summary":        summary,
        "leaks":          _serialize(primary_leaks),
        "forecasts":      _serialize(forecasts),
        "pipeline_stats": pipeline_stats,
    }
