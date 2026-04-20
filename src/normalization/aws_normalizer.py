from src.normalization.schema_enforcer import enforce_schema
import pandas as pd


_SLASH_TO_UNDERSCORE = {
    "lineItem/UnblendedCost":   "line_item_unblended_cost",
    "lineItem/UsageStartDate":  "line_item_usage_start_date",
    "lineItem/ResourceId":      "line_item_resource_id",
    "lineItem/UsageAmount":     "line_item_usage_amount",
    "lineItem/LineItemType":    "line_item_line_item_type",
    "lineItem/UsageAccountId":  "line_item_usage_account_id",
    "product/servicecode":      "product_servicecode",
    "product/region":           "product_region",
    "product/ProductName":      "product_product_name",
}


def normalize_aws(df):
    """
    Normalize AWS Cost & Usage Report (CUR) into unified schema
    """

    # ---- Normalise slash-format column names (newer CUR exports) ----
    if "lineItem/UnblendedCost" in df.columns:
        df = df.rename(columns=_SLASH_TO_UNDERSCORE)

    # ---- Drop Tax line items (not actionable waste) ----
    if "line_item_line_item_type" in df.columns:
        df = df[df["line_item_line_item_type"] != "Tax"]

    # ---- AWS CUR column mapping ----
    normalized = df.rename(columns={
        "line_item_usage_start_date": "date",
        "product_servicecode": "service",
        "line_item_resource_id": "resource_id",
        "line_item_usage_amount": "usage",
        "line_item_unblended_cost": "cost",
        "product_region": "region",
    })

    # ---- EBS service label correction ----
    if "service" in normalized.columns:
        normalized["service"] = normalized["service"].replace({
            "Amazon Elastic Block Store": "AmazonEBS",
        })

    if "product_product_name" in normalized.columns:
        ebs_mask = normalized["product_product_name"].str.contains(
            "Elastic Block Store", na=False
        )
        normalized.loc[ebs_mask, "service"] = "AmazonEBS"

    # Convert date properly
    if "date" in normalized.columns:
        normalized["date"] = pd.to_datetime(
            normalized["date"], errors="coerce"
        ).dt.date

    normalized["provider"] = "AWS"

    # Enforce unified schema (this will drop bad rows safely)
    normalized = enforce_schema(normalized)

    return normalized