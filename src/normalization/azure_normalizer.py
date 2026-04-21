import pandas as pd
from .schema_enforcer import enforce_schema


def _pick(df: pd.DataFrame, *candidates: str):
    """Return df[first matching column], or a Series of None if none match."""
    for c in candidates:
        if c in df.columns:
            return df[c]
    return pd.Series([None] * len(df), index=df.index)


def normalize_azure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Azure Cost Management Export into canonical schema.

    Handles three common Azure export formats:
    - Cost Management Export  (UsageDate, ServiceName, CostInUSD, Quantity, ResourceId)
    - EA Export               (Date, MeterCategory, Cost, UsageQuantity, InstanceId)
    - MCA Export              (date, serviceFamily, costInUSD, quantity, resourceId)
    """
    normalized = pd.DataFrame()

    # ---------------- DATE ----------------
    date_col = _pick(df, "UsageDate", "Date", "BillingPeriodStartDate",
                     "date", "billingPeriodStartDate")
    normalized["date"] = pd.to_datetime(date_col, errors="coerce")

    # ---------------- SERVICE ----------------
    normalized["service"] = _pick(
        df, "ServiceName", "MeterCategory", "ConsumedService",
        "serviceFamily", "serviceName"
    )

    # ---------------- COST ----------------
    normalized["cost"] = pd.to_numeric(
        _pick(df, "CostInUSD", "Cost", "PreTaxCost",
              "CostInBillingCurrency", "costInUSD", "cost"),
        errors="coerce",
    )

    # ---------------- USAGE ----------------
    normalized["usage"] = pd.to_numeric(
        _pick(df, "Usage", "Quantity", "UsageQuantity",
              "quantity", "usageQuantity"),
        errors="coerce",
    )

    # ---------------- PROVIDER ----------------
    normalized["provider"] = "Azure"

    # ---------------- RESOURCE / METADATA ----------------
    normalized["resource_id"] = _pick(
        df, "ResourceId", "InstanceId", "ResourceName",
        "resourceId", "instanceId", "resourceName"
    )
    normalized["region"] = _pick(
        df, "Region", "ResourceLocation", "resourceLocation",
        "region", "ResourceRegion"
    )

    # ---------------- TAG EXTRACTION ----------------
    tags_col = _pick(df, "Tags", "tags", "TagsDictionary")
    if tags_col is None or tags_col.isna().all():
        tags = pd.Series([""] * len(df), index=df.index)
    else:
        tags = tags_col.fillna("")

    normalized["labels.project"] = tags.str.extract(r"[Pp]roject=([^;,]+)")
    normalized["labels.owner"] = tags.str.extract(r"[Oo]wner=([^;,]+)")
    normalized["labels.environment"] = tags.str.extract(r"[Ee]nvironment=([^;,]+)")

    # ---------------- FINAL CLEAN ----------------
    normalized = enforce_schema(normalized)
    return normalized