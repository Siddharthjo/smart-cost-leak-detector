from src.normalization.schema_enforcer import enforce_schema


def normalize_azure(df):
    """
    Normalize Azure billing data into unified schema
    """

    normalized = df.rename(columns={
        "UsageDate": "date",
        "ServiceName": "service",
        "ResourceId": "resource_id",
        "UsageQuantity": "usage",
        "Cost": "cost",
        "ResourceLocation": "region",
    })

    normalized["provider"] = "AZURE"

    normalized = enforce_schema(normalized)

    return normalized