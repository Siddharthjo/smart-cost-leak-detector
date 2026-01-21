from src.normalization.schema_enforcer import enforce_schema


def normalize_aws(df):
    """
    Normalize AWS billing data into unified schema
    """

    normalized = df.rename(columns={
        "UsageStartDate": "date",
        "Service": "service",
        "ResourceId": "resource_id",
        "UsageQuantity": "usage",
        "UnblendedCost": "cost",
        "Region": "region",
    })

    normalized["provider"] = "AWS"

    normalized = enforce_schema(normalized)

    return normalized