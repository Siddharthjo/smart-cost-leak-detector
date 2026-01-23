import pandas as pd
from .schema_enforcer import enforce_schema

def normalize_azure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Azure Cost Management Export into canonical schema
    """

    normalized = pd.DataFrame()

    # ---------------- REQUIRED CANONICAL FIELDS ----------------

    normalized["date"] = pd.to_datetime(df["UsageDate"])
    normalized["service"] = df["ServiceName"]

    # IMPORTANT FIX 👇
    if "CostInUSD" in df.columns:
        normalized["cost"] = df["CostInUSD"]
    else:
        normalized["cost"] = df["Cost"]

    normalized["usage"] = df["Usage"]
    normalized["provider"] = "Azure"

    # ---------------- RESOURCE / METADATA ----------------

    normalized["resource_id"] = df["ResourceId"]
    normalized["region"] = df.get("Region")

    # ---------------- TAG EXTRACTION ----------------

    # Azure tags are semi-colon separated
    tags = df.get("Tags", "").fillna("")

    normalized["labels.project"] = tags.str.extract(r"Project=([^;]+)")
    normalized["labels.owner"] = tags.str.extract(r"Owner=([^;]+)")
    normalized["labels.environment"] = tags.str.extract(r"Environment=([^;]+)")

    # ---------------- FINAL CLEAN ----------------

    normalized = enforce_schema(normalized)
    return normalized