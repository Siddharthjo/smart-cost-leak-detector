from src.normalization.schema_enforcer import enforce_schema
import pandas as pd


def normalize_aws(df):
    """
    Normalize AWS Cost & Usage Report (CUR) into unified schema
    """

    # ---- AWS CUR column mapping ----
    normalized = df.rename(columns={
        "line_item_usage_start_date": "date",
        "product_servicecode": "service",
        "line_item_resource_id": "resource_id",
        "line_item_usage_amount": "usage",
        "line_item_unblended_cost": "cost",
        "product_region": "region",
    })

    # Convert date properly
    if "date" in normalized.columns:
        normalized["date"] = pd.to_datetime(
            normalized["date"], errors="coerce"
        ).dt.date

    normalized["provider"] = "AWS"

    # Enforce unified schema (this will drop bad rows safely)
    normalized = enforce_schema(normalized)

    return normalized