import os
import pandas as pd


def validate_csv(file_path: str, df: pd.DataFrame):
    """
    Basic validation for uploaded CSV files.
    Returns (is_valid: bool, message: str)
    """

    # 1. Check file exists
    if not os.path.exists(file_path):
        return False, "File does not exist"

    # 2. Check file is not empty
    if df.empty:
        return False, "CSV file is empty"

    # 3. Check if required cost-related columns exist
    column_names = [col.lower() for col in df.columns]

    cost_like_columns = [
        "cost",
        "amount",
        "usage",
        "quantity"
    ]

    has_cost_signal = any(
        any(keyword in col for keyword in cost_like_columns)
        for col in column_names
    )

    if not has_cost_signal:
        return False, "CSV does not contain cost or usage data"

    return True, "CSV is valid for analysis"