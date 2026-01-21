import pandas as pd


def enforce_schema(df):
    """
    Enforce unified COST_USAGE schema.
    Drops rows only if truly critical fields are missing.
    """

    # --- Type safety ---
    if "cost" in df.columns:
        df["cost"] = pd.to_numeric(df["cost"], errors="coerce")

    if "usage" in df.columns:
        df["usage"] = pd.to_numeric(df["usage"], errors="coerce")

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # --- Drop ONLY truly invalid rows ---
    df = df.dropna(subset=["date", "service", "cost"])

    # --- Ensure optional columns exist ---
    for col in ["usage", "resource_id", "region"]:
        if col not in df.columns:
            df[col] = None

    return df