import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def compute_cost_zscore(daily_cost_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each (provider, service), compute rolling 7-day mean + std.
    Adds z_score and rolling_mean columns so detectors can use
    statistical baselines instead of fixed thresholds.

    Requires min 3 days of history to emit a z-score.
    Rows with insufficient history get NaN z_score — detectors
    fall back to rule-based logic when z_score is NaN.
    """
    if daily_cost_df.empty:
        return daily_cost_df

    result = []
    for (provider, service), g in daily_cost_df.groupby(["provider", "service"]):
        g = g.sort_values("date").copy()
        shifted = g["daily_cost"].shift(1)
        g["rolling_mean"] = shifted.rolling(7, min_periods=3).mean()
        g["rolling_std"]  = shifted.rolling(7, min_periods=3).std()
        g["z_score"] = (
            (g["daily_cost"] - g["rolling_mean"])
            / (g["rolling_std"] + 0.01)
        )
        result.append(g)

    if not result:
        return daily_cost_df

    return pd.concat(result, ignore_index=True)


def compute_30day_forecast(daily_cost_df: pd.DataFrame) -> list:
    """
    Linear regression forecast per (provider, service).
    Requires at least 5 data points.

    Returns list of dicts:
        provider, service, projected_monthly_cost,
        last_30d_actual, trend_pct
    """
    forecasts = []

    for (provider, service), g in daily_cost_df.groupby(["provider", "service"]):
        if g["daily_cost"].sum() < 0.01:
            continue
        g = g.sort_values("date").copy()
        g = g[g["daily_cost"] > 0]
        if len(g) < 3:
            continue

        x = np.arange(len(g), dtype=float)
        y = g["daily_cost"].values.astype(float)

        try:
            slope, intercept = np.polyfit(x, y, 1)
        except Exception as e:
            logger.debug(f"Forecast failed for {provider}/{service}: {e}")
            continue

        # CV guard: skip noisy series where std > mean (coefficient of variation > 1)
        mean_cost = float(y.mean())
        if mean_cost > 0 and float(y.std()) / mean_cost > 1.0:
            logger.debug(f"Forecast skipped for {provider}/{service}: CV > 1 (noisy series)")
            continue

        projected_daily   = intercept + slope * (len(g) + 30)
        if projected_daily < 0:
            projected_daily = mean_cost
        projected_monthly = round(projected_daily * 30, 2)

        last_n           = y[-min(30, len(y)):]
        last_30d_actual  = round(float(last_n.mean()) * 30, 2)
        baseline         = max(last_30d_actual, 0.01)

        # Cap projected cost at 3x actual to suppress regression runaway
        projected_monthly = round(min(projected_monthly, baseline * 3), 2)

        raw_trend = ((projected_monthly - last_30d_actual) / baseline) * 100
        # Clamp trend to ±200% to avoid misleading extreme values
        trend_pct = round(max(-150.0, min(150.0, raw_trend)), 1)

        forecasts.append({
            "provider":              provider,
            "service":               service,
            "projected_monthly_cost": projected_monthly,
            "last_30d_actual":       last_30d_actual,
            "trend_pct":             trend_pct,
        })

    forecasts.sort(key=lambda x: -x["projected_monthly_cost"])
    return forecasts
