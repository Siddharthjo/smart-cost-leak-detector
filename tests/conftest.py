"""Shared fixtures for all test modules."""

from datetime import date, timedelta

import pandas as pd
import pytest


# ===================== DATE HELPERS =====================

def make_dates(n: int, end: date = None) -> list[date]:
    """Return list of n consecutive dates ending at `end` (default today)."""
    end = end or date(2024, 3, 31)
    return [end - timedelta(days=i) for i in reversed(range(n))]


# ===================== DATAFRAME BUILDERS =====================

def make_normalized_df(rows: list[dict]) -> pd.DataFrame:
    """Build a normalized billing DataFrame from a list of row dicts."""
    df = pd.DataFrame(rows)
    if "date" not in df.columns:
        df["date"] = date(2024, 3, 1)
    if "provider" not in df.columns:
        df["provider"] = "AWS"
    if "cost" not in df.columns:
        df["cost"] = 1.0
    if "usage" not in df.columns:
        df["usage"] = 1.0
    if "resource_id" not in df.columns:
        df["resource_id"] = "res-001"
    if "region" not in df.columns:
        df["region"] = "us-east-1"
    return df


def make_daily_cost_df(rows: list[dict]) -> pd.DataFrame:
    """Build a daily_cost_df as produced by daily_cost_per_service."""
    df = pd.DataFrame(rows)
    if "date" not in df.columns:
        df["date"] = date(2024, 3, 1)
    if "provider" not in df.columns:
        df["provider"] = "AWS"
    if "service" not in df.columns:
        df["service"] = "ec2"
    if "daily_cost" not in df.columns:
        df["daily_cost"] = 10.0
    return df


# ===================== SHARED FIXTURES =====================

@pytest.fixture
def dates_20():
    return make_dates(20)


@pytest.fixture
def normalized_df(dates_20):
    """20-day AWS billing data: ec2 (zombie candidate) + rds (idle DB) + s3."""
    rows = []
    for d in dates_20:
        rows.append({"date": d, "provider": "AWS", "service": "ec2",
                     "cost": 80.0, "usage": 0.01, "resource_id": "i-zombie", "region": "us-east-1"})
        rows.append({"date": d, "provider": "AWS", "service": "rds",
                     "cost": 50.0, "usage": 0.05, "resource_id": "db-idle", "region": "us-east-1"})
        rows.append({"date": d, "provider": "AWS", "service": "s3",
                     "cost": 5.0,  "usage": 100.0, "resource_id": "bucket-1", "region": "us-east-1"})
    return make_normalized_df(rows)


@pytest.fixture
def daily_cost_df(normalized_df):
    from src.intelligence.feature_engineering.cost_features import daily_cost_per_service
    from src.intelligence.feature_engineering.anomaly_features import compute_cost_zscore
    df = daily_cost_per_service(normalized_df)
    return compute_cost_zscore(df)


@pytest.fixture
def lifespan_results(normalized_df):
    from src.intelligence.feature_engineering.cost_features import resource_lifespan
    return resource_lifespan(normalized_df)


@pytest.fixture
def usage_ratio_data(normalized_df):
    from src.intelligence.feature_engineering.cost_features import usage_cost_ratio
    return usage_cost_ratio(normalized_df)


@pytest.fixture
def cost_percentiles(normalized_df):
    from src.intelligence.severity.cost_context import build_cost_percentiles
    return build_cost_percentiles(normalized_df)
