import pandas as pd
from src.normalization.schema_enforcer import enforce_schema


def normalize_gcp(df):
    """
    Normalize GCP Cloud Billing Detailed Usage Export
    into the unified schema.
    """

    normalized = df.rename(columns={
        "usage_start_time": "date",
        "service.description": "service",
        "usage.amount": "usage",
        "cost": "cost",
        "resource.name": "resource_id",
        "location.region": "region",
    })

    # Parse timestamp → date
    if "date" in normalized.columns:
        normalized["date"] = pd.to_datetime(
            normalized["date"], errors="coerce"
        ).dt.date

    normalized["provider"] = "GCP"

    normalized = enforce_schema(normalized)

    return normalized