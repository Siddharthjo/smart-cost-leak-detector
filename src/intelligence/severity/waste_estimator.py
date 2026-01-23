def flat_monthly_waste(avg_daily_cost: float) -> float:
    return round(avg_daily_cost * 30, 2)


def lifespan_adjusted_waste(avg_daily_cost: float, days_active: int) -> float:
    return round(avg_daily_cost * min(days_active, 30), 2)


def runaway_projected_waste(
    first_cost: float,
    last_cost: float,
    days: int
) -> float:
    if days <= 0:
        return 0

    daily_growth = (last_cost - first_cost) / days
    projected = last_cost + (daily_growth * 30)

    baseline = first_cost * 30
    return round(max(projected - baseline, 0), 2)