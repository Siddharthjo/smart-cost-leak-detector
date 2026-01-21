from src.normalization.schema import UNIFIED_SCHEMA


def enforce_schema(df):
    """
    Enforces unified schema:
    - Adds missing optional columns
    - Drops rows missing required columns
    - Keeps only unified schema columns
    """

    # 1. Add missing columns
    for column, rules in UNIFIED_SCHEMA.items():
        if column not in df.columns:
            df[column] = None

    # 2. Drop rows missing required fields
    for column, rules in UNIFIED_SCHEMA.items():
        if rules["required"]:
            df = df[df[column].notna()]

    # 3. Keep only unified schema columns
    df = df[list(UNIFIED_SCHEMA.keys())]

    return df