def detect_csv_type(columns):
    """
    Detect whether CSV is INVOICE or COST_USAGE
    """
    column_names = [col.lower() for col in columns]

    invoice_keywords = [
        "invoice",
        "due",
        "balance",
        "payer",
        "billing period"
    ]

    usage_keywords = [
        "usage",
        "resource",
        "service",
        "meter",
        "cost"
    ]

    if any(any(key in col for key in invoice_keywords) for col in column_names):
        return "INVOICE"

    if any(any(key in col for key in usage_keywords) for col in column_names):
        return "COST_USAGE"

    return "UNKNOWN"