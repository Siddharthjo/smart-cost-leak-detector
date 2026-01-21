def daily_cost_per_service(df):
    """
    Computes daily cost per service.
    Input: normalized DataFrame
    Output: DataFrame with daily cost aggregated per service
    """

    grouped = (
        df.groupby(["date", "provider", "service"], as_index=False)
          .agg(daily_cost=("cost", "sum"))
    )

    return grouped

def cost_trend_per_service(daily_cost_df):
    """
    Determines cost trend per service over time.
    Input: daily cost per service DataFrame
    Output: DataFrame with trend label per service
    """

    trends = []

    grouped = daily_cost_df.groupby(["provider", "service"])

    for (provider, service), group in grouped:
        group = group.sort_values("date")

        if len(group) < 2:
            trend = "FLAT"
        else:
            first_cost = group.iloc[0]["daily_cost"]
            last_cost = group.iloc[-1]["daily_cost"]

            if last_cost > first_cost:
                trend = "INCREASING"
            elif last_cost < first_cost:
                trend = "DECREASING"
            else:
                trend = "FLAT"

        trends.append({
            "provider": provider,
            "service": service,
            "trend": trend
        })

    return trends

def resource_lifespan(df):
    """
    Computes lifespan (number of days) a resource has incurred cost.
    Input: normalized DataFrame
    Output: DataFrame with resource lifespan in days
    """

    if "resource_id" not in df.columns:
        return []

    grouped = df.dropna(subset=["resource_id"]).groupby(
        ["provider", "service", "resource_id"]
    )

    lifespans = []

    for (provider, service, resource_id), group in grouped:
        days_active = group["date"].nunique()

        lifespans.append({
            "provider": provider,
            "service": service,
            "resource_id": resource_id,
            "days_active": days_active
        })

    return lifespans