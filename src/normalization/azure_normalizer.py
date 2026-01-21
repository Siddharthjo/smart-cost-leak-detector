from src.normalization.schema_enforcer import enforce_schema
import pandas as pd


def normalize_azure(df):
    """
    Normalize Azure Cost Management Export into unified schema
    """

    normalized = df.rename(columns={
        "UsageDate": "date",
        "MeterCategory": "service",
        "CostInBillingCurrency": "cost",
        "ConsumedQuantity": "usage",
        "ResourceId": "resource_id",
        "ResourceLocation": "region",
    })

    # Parse date safely
    if "date" in normalized.columns:
        normalized["date"] = pd.to_datetime(
            normalized["date"], errors="coerce"
        ).dt.date

    normalized["provider"] = "Azure"

    normalized = enforce_schema(normalized)

    return normalized