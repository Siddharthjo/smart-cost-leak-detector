import pandas as pd
from src.normalization.schema_enforcer import enforce_schema

def normalize_gcp(df):
    """
    Normalize GCP Cloud Billing Export (flattened CSV)
    into unified schema
    """

    normalized = pd.DataFrame({
        "date": pd.to_datetime(df["usage_start_time"]).dt.date,
        "service": df["service_description"],
        "cost": df["cost"],
        "usage": df["usage_amount"],
        "resource_id": df["resource_name"].fillna("unknown"),
        "region": df.get("region"),
        "labels.environment": df.get("label_environment"),
        "provider": "GCP"
    })

    # Explicit provider tag
    normalized["provider"] = "GCP"

    # Enforce unified schema
    normalized = enforce_schema(normalized)

    return normalized