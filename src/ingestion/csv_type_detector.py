def detect_csv_type(df):
    columns = set(col.lower() for col in df.columns)

    # ---- AWS CUR / COST & USAGE detection (PRIORITY) ----
    aws_cur_signals = {
        "line_item_usage_start_date",
        "line_item_usage_amount",
        "line_item_unblended_cost",
        "product_servicecode",
        "line_item_resource_id",
    }

    cur_matches = aws_cur_signals.intersection(columns)

    if len(cur_matches) >= 2:
        return "COST_USAGE"

    # ---- Invoice detection (fallback) ----
    invoice_signals = {
        "invoice id",
        "billing period",
        "balance due",
        "invoice amount",
    }

    if invoice_signals.intersection(columns):
        return "INVOICE"

    return "UNKNOWN"