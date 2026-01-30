import pandas as pd

def build_cost_percentiles(normalized_df):
    """
    Returns a dict:
    (provider, service, resource_id) -> percentile (0–100)
    """

    df = (
        normalized_df
        .groupby(["provider", "service", "resource_id"])["cost"]
        .sum()
        .reset_index()
    )

    percentiles = {}

    for (provider, service), g in df.groupby(["provider", "service"]):
        g = g.sort_values("cost")
        g["percentile"] = g["cost"].rank(pct=True) * 100

        for _, row in g.iterrows():
            percentiles[
                (provider, service, row["resource_id"])
            ] = round(row["percentile"], 1)

    return percentiles