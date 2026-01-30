import pandas as pd

# ===================== CONFIG =====================

ALWAYS_ON_MIN_DAILY_COST = 50.0
ALWAYS_ON_PRESENCE_RATIO = 0.9

RUNAWAY_COST_GROWTH_PERCENT = 30
RUNAWAY_MIN_DAYS = 3
RUNAWAY_MIN_DAILY_COST = 2.0

IDLE_USAGE_RATIO_THRESHOLD = 5
IDLE_MIN_DAILY_COST = 1.0
IDLE_MIN_DAYS_ACTIVE = 3

ZOMBIE_MIN_DAYS = 14

# ===================== SERVICE CATEGORIES =====================

COMPUTE_SERVICES = {"ec2", "virtual machines", "compute engine"}
STORAGE_SERVICES = {"s3", "storage", "cloud storage"}
SERVERLESS_SERVICES = {"lambda", "functions", "cloud functions"}
DATABASE_SERVICES = {"rds", "sql", "cosmos", "cloud sql"}

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

def detect_zombie_resources(lifespan_results, usage_ratio_data):
    """
    Long-running resources with consistently inefficient usage.
    Highest confidence compute waste signal.
    """
    leaks = []
    zombie_resource_ids = set()

    usage_lookup = {
        (u["provider"], u["service"], u["resource_id"]): u["usage_to_cost_ratio"]
        for u in usage_ratio_data
    }

    for r in lifespan_results:
        provider = r["provider"]
        service = r["service"]
        resource_id = r["resource_id"]
        days_active = r["days_active"]

        usage_ratio = usage_lookup.get((provider, service, resource_id))
        if usage_ratio is None:
            continue

        # Provider-aware thresholds (documented)
        if provider == "AWS":
            threshold = 0.05
        elif provider == "AZURE":
            threshold = 0.10
        else:  # GCP
            threshold = 3.0  # GCP reports usage very differently

        if days_active >= ZOMBIE_MIN_DAYS and usage_ratio < threshold:
            zombie_resource_ids.add(resource_id)

            leaks.append({
                "leak_type": "ZOMBIE_RESOURCE",
                "provider": provider,
                "service": service,
                "resource_id": resource_id,
                "reason": (
                    f"Active {days_active} days with inefficient usage "
                    f"({usage_ratio:.2f})"
                ),
            })

    return leaks, zombie_resource_ids

# ===================== IDLE RESOURCES =====================

def detect_idle_resources(
    lifespan_data,
    usage_ratio_data,
    daily_cost_df,
    excluded_resource_ids: set,
):
    """
    Shorter-lived, low-usage compute.
    Explicitly excludes zombie resources.
    """
    leaks = []

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
        provider = r["provider"]
        service = r["service"]
        resource_id = r["resource_id"]
        days = r["days_active"]

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
            "leak_type": "IDLE_RESOURCE",
            "provider": provider,
            "service": service,
            "resource_id": resource_id,
            "reason": f"Low usage detected over {days} days",
        })

    return leaks

# ===================== RUNAWAY COSTS =====================

def detect_runaway_costs(daily_cost_df, usage_ratio_data):
    """
    Rapid cost growth over a short period.
    """
    leaks = []

    usage_lookup = (
        pd.DataFrame(usage_ratio_data)
        .groupby(["provider", "service"])["usage_to_cost_ratio"]
        .mean()
        .to_dict()
        if usage_ratio_data else {}
    )

    for (provider, service), g in daily_cost_df.groupby(["provider", "service"]):
        if len(g) < RUNAWAY_MIN_DAYS:
            continue

        g = g.sort_values("date")
        costs = g["daily_cost"].values

        if costs.mean() < RUNAWAY_MIN_DAILY_COST:
            continue

        growth = ((costs[-1] - costs[0]) / max(costs[0], 0.01)) * 100
        if growth < RUNAWAY_COST_GROWTH_PERCENT:
            continue

        usage_ratio = usage_lookup.get((provider, service))
        if usage_ratio is not None and usage_ratio > 10:
            continue

        leaks.append({
            "leak_type": "RUNAWAY_COST",
            "provider": provider,
            "service": service,
            "reason": (
                f"Daily cost increased from ${costs[0]:.2f} "
                f"to ${costs[-1]:.2f} over {len(costs)} days"
            ),
        })

    return leaks

# ===================== ALWAYS-ON HIGH COST =====================

def detect_always_on_high_cost(daily_cost_df, normalized_df):
    """
    Consistently expensive compute or database services with no ownership.
    """
    leaks = []

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

        presence_ratio = days_present.get((provider, service), 0) / max(total_days, 1)
        if presence_ratio < ALWAYS_ON_PRESENCE_RATIO:
            continue

        rows = normalized_df[
            (normalized_df["provider"] == provider) &
            (normalized_df["service"] == service)
        ]

        owner_cols = [
            c for c in rows.columns
            if any(k in c.lower() for k in ["owner", "project", "environment"])
        ]

        if any(rows[c].notna().any() for c in owner_cols):
            continue

        leaks.append({
            "leak_type": "ALWAYS_ON_HIGH_COST",
            "provider": provider,
            "service": service,
            "reason": f"Always-on service costing ${cost:.2f}/day with no ownership",
        })

    return leaks