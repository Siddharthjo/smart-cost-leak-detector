def detect_csv_type(df):
    columns = set(col.lower() for col in df.columns)

    # --- AWS CUR detection ---
    aws_cur_signals = {
        "line_item_usage_start_date",
        "line_item_usage_amount",
        "line_item_unblended_cost",
        "product_servicecode",
        "line_item_resource_id",
    }

    if len(aws_cur_signals.intersection(columns)) >= 2:
        return "COST_USAGE"

    # --- Azure Cost Management Export detection ---
    azure_signals = {
        "usagedate",
        "consumedquantity",
        "costinbillingcurrency",
        "metercategory",
        "resourceid",
        "subscriptionid",
    }

    if len(azure_signals.intersection(columns)) >= 2:
        return "COST_USAGE"

    # --- Invoice detection (fallback) ---
    invoice_signals = {
        "invoice id",
        "billing period",
        "balance due",
        "invoice amount",
    }

    if invoice_signals.intersection(columns):
        return "INVOICE"

    return "UNKNOWN"