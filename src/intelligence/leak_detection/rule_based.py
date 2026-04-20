import pandas as pd
import logging
from typing import List, Dict, Tuple, Optional

logger = logging.getLogger(__name__)

# ===================== CONFIG =====================

ALWAYS_ON_MIN_DAILY_COST   = 50.0
ALWAYS_ON_PRESENCE_RATIO   = 0.9

RUNAWAY_COST_GROWTH_PERCENT = 30
RUNAWAY_MIN_DAYS            = 3
RUNAWAY_MIN_DAILY_COST      = 2.0
RUNAWAY_ZSCORE_THRESHOLD    = 2.5   # stddevs above rolling baseline

IDLE_USAGE_RATIO_THRESHOLD  = 5
IDLE_MIN_DAILY_COST         = 1.0
IDLE_MIN_DAYS_ACTIVE        = 3

ZOMBIE_MIN_DAYS             = 14

# ===================== SERVICE CATEGORIES =====================

COMPUTE_SERVICES   = {"ec2", "virtual machines", "compute engine"}
STORAGE_SERVICES   = {"s3", "storage", "cloud storage"}
SERVERLESS_SERVICES = {"lambda", "functions", "cloud functions"}
DATABASE_SERVICES  = {"rds", "sql", "cosmos", "cloud sql"}


def get_service_category(service_name: str) -> str:
    if not service_name:
        return "other"
    s = service_name.lower()
    if any(k in s for k in COMPUTE_SERVICES):
        return "compute"
    if any(k in s for k in STORAGE_SERVICES):
        return "storage"
    if any(k in s for k in SERVERLESS_SERVICES):
        return "serverless"
    if any(k in s for k in DATABASE_SERVICES):
        return "database"
    return "other"


# ===================== ZOMBIE RESOURCES =====================

def detect_zombie_resources(
    lifespan_results: list,
    usage_ratio_data: list,
    cost_percentiles: Optional[dict] = None,
) -> Tuple[List[Dict], set]:
    """
    Long-running resources with consistently inefficient usage.

    Threshold logic (priority order):
    1. Percentile-based: flag if usage ratio < 25th percentile for that service
       AND cost percentile > 50th (high cost, low usage)
    2. Fallback: provider-aware fixed thresholds if no percentile data
    """
    leaks: List[Dict] = []
    zombie_resource_ids: set = set()

    usage_lookup = {
        (u["provider"], u["service"], u["resource_id"]): u["usage_to_cost_ratio"]
        for u in usage_ratio_data
    }

    for r in lifespan_results:
        provider    = r["provider"]
        service     = r["service"]
        resource_id = r["resource_id"]
        days_active = r["days_active"]

        usage_ratio = usage_lookup.get((provider, service, resource_id))
        if usage_ratio is None:
            continue

        # Compute service-level p25 threshold from available ratio data
        if cost_percentiles:
            service_ratios = [
                v for (p, s, _), v in usage_lookup.items()
                if p == provider and s == service
            ]
            if len(service_ratios) >= 4:
                service_ratios_sorted = sorted(service_ratios)
                p25_idx   = max(0, int(len(service_ratios_sorted) * 0.25) - 1)
                threshold = service_ratios_sorted[p25_idx]
            else:
                threshold = {"AWS": 0.05, "AZURE": 0.10}.get(provider, 3.0)
        else:
            threshold = {"AWS": 0.05, "AZURE": 0.10}.get(provider, 3.0)

        if days_active >= ZOMBIE_MIN_DAYS and usage_ratio < threshold:
            zombie_resource_ids.add(resource_id)
            leaks.append({
                "leak_type":   "ZOMBIE_RESOURCE",
                "provider":    provider,
                "service":     service,
                "resource_id": resource_id,
                "reason": (
                    f"Active {days_active} days with usage-to-cost ratio of "
                    f"{usage_ratio:.4f} (below service p25 threshold {threshold:.4f})"
                ),
            })

    return leaks, zombie_resource_ids


# ===================== IDLE RESOURCES =====================

def detect_idle_resources(
    lifespan_data: list,
    usage_ratio_data: list,
    daily_cost_df: pd.DataFrame,
    excluded_resource_ids: set,
) -> List[Dict]:
    """
    Shorter-lived, low-usage compute.
    Explicitly excludes zombie resources.
    """
    leaks: List[Dict] = []

    usage_lookup = {
        (u["provider"], u["service"], u["resource_id"]): u["usage_to_cost_ratio"]
        for u in usage_ratio_data
    }

    avg_cost_lookup = (
        daily_cost_df
        .groupby(["provider", "service"])["daily_cost"]
        .mean()
        .to_dict()
    )

    for r in lifespan_data:
        provider    = r["provider"]
        service     = r["service"]
        resource_id = r["resource_id"]
        days        = r["days_active"]

        if resource_id in excluded_resource_ids:
            continue
        if get_service_category(service) != "compute":
            continue
        if days < IDLE_MIN_DAYS_ACTIVE:
            continue

        usage_ratio = usage_lookup.get((provider, service, resource_id))
        if usage_ratio is None or usage_ratio > IDLE_USAGE_RATIO_THRESHOLD:
            continue

        daily_cost = avg_cost_lookup.get((provider, service), 0)
        if daily_cost < IDLE_MIN_DAILY_COST:
            continue

        leaks.append({
            "leak_type":   "IDLE_RESOURCE",
            "provider":    provider,
            "service":     service,
            "resource_id": resource_id,
            "reason":      f"Low usage detected over {days} days",
        })

    return leaks


# ===================== RUNAWAY COSTS =====================

def detect_runaway_costs(
    daily_cost_df: pd.DataFrame,
    usage_ratio_data: list,
) -> List[Dict]:
    """
    Detects rapid cost growth.

    Strategy (in priority order):
    1. Z-score: flag if latest day is > RUNAWAY_ZSCORE_THRESHOLD stddevs
       above 7-day rolling baseline (statistical, data-driven)
    2. Fallback: % growth rule if z_score column is absent or NaN
       (preserves backward compatibility)
    """
    leaks: List[Dict] = []
    has_zscore = "z_score" in daily_cost_df.columns

    usage_lookup: dict = {}
    if usage_ratio_data:
        usage_df = pd.DataFrame(usage_ratio_data)
        if not usage_df.empty:
            usage_lookup = (
                usage_df.groupby(["provider", "service"])["usage_to_cost_ratio"]
                .mean()
                .to_dict()
            )

    for (provider, service), g in daily_cost_df.groupby(["provider", "service"]):
        g = g.sort_values("date")
        costs = g["daily_cost"].values

        if len(g) < RUNAWAY_MIN_DAYS:
            continue
        if costs.mean() < RUNAWAY_MIN_DAILY_COST:
            continue

        # Filter out services with strong usage signal (not a leak)
        usage_ratio = usage_lookup.get((provider, service))
        if usage_ratio is not None and usage_ratio > 10:
            continue

        # ---- Z-SCORE PATH ----
        if has_zscore:
            latest_z    = g["z_score"].iloc[-1]
            rolling_mean = g["rolling_mean"].iloc[-1] if "rolling_mean" in g.columns else None

            if pd.notna(latest_z) and latest_z >= RUNAWAY_ZSCORE_THRESHOLD:
                baseline_str = (
                    f"7-day baseline of ${rolling_mean:.2f}"
                    if rolling_mean is not None and pd.notna(rolling_mean)
                    else "recent baseline"
                )
                leaks.append({
                    "leak_type": "RUNAWAY_COST",
                    "provider":  provider,
                    "service":   service,
                    "reason": (
                        f"Cost spike: ${costs[-1]:.2f}/day is {latest_z:.1f} "
                        f"stddevs above {baseline_str}"
                    ),
                    "z_score": round(float(latest_z), 2),
                })
                continue

        # ---- FALLBACK: % GROWTH ----
        growth = ((costs[-1] - costs[0]) / max(costs[0], 0.01)) * 100
        if growth >= RUNAWAY_COST_GROWTH_PERCENT:
            leaks.append({
                "leak_type": "RUNAWAY_COST",
                "provider":  provider,
                "service":   service,
                "reason": (
                    f"Daily cost increased from ${costs[0]:.2f} "
                    f"to ${costs[-1]:.2f} over {len(costs)} days "
                    f"(+{growth:.0f}%)"
                ),
            })

    return leaks


# ===================== ALWAYS-ON HIGH COST =====================

def detect_always_on_high_cost(
    daily_cost_df: pd.DataFrame,
    normalized_df: pd.DataFrame,
) -> List[Dict]:
    """
    Consistently expensive compute/database services with no ownership tags.
    """
    leaks: List[Dict] = []

    total_days = daily_cost_df["date"].nunique()

    days_present = (
        daily_cost_df
        .groupby(["provider", "service"])["date"]
        .nunique()
        .to_dict()
    )

    avg_cost = (
        daily_cost_df
        .groupby(["provider", "service"])["daily_cost"]
        .mean()
        .to_dict()
    )

    for (provider, service), cost in avg_cost.items():
        if get_service_category(service) not in {"compute", "database"}:
            continue
        if cost < ALWAYS_ON_MIN_DAILY_COST:
            continue

        presence_ratio = (
            days_present.get((provider, service), 0) / max(total_days, 1)
        )
        if presence_ratio < ALWAYS_ON_PRESENCE_RATIO:
            continue

        rows = normalized_df[
            (normalized_df["provider"] == provider) &
            (normalized_df["service"]  == service)
        ]

        owner_cols = [
            c for c in rows.columns
            if any(k in c.lower() for k in ["owner", "project", "environment"])
        ]

        if any(rows[c].notna().any() for c in owner_cols):
            continue

        leaks.append({
            "leak_type": "ALWAYS_ON_HIGH_COST",
            "provider":  provider,
            "service":   service,
            "reason": (
                f"Always-on service costing ${cost:.2f}/day "
                f"with no ownership metadata"
            ),
        })

    return leaks
