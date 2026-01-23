import pandas as pd

# ===================== CONFIG =====================

# -------- ALWAYS-ON HIGH COST --------
ALWAYS_ON_MIN_DAILY_COST = 50.0
ALWAYS_ON_PRESENCE_RATIO = 0.9

# -------- RUNAWAY COST --------
RUNAWAY_COST_GROWTH_PERCENT = 30
RUNAWAY_MIN_DAYS = 3
RUNAWAY_MIN_DAILY_COST = 2.0

# -------- IDLE --------
IDLE_USAGE_RATIO_THRESHOLD = 5
IDLE_MIN_DAILY_COST = 1.0
IDLE_MIN_DAYS_ACTIVE = 3

# -------- ZOMBIE --------
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

# ===================== IDLE RESOURCES =====================

def detect_idle_resources(lifespan_data, usage_ratio_data, daily_cost_df):
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
        rid = r["resource_id"]
        days = r["days_active"]

        if get_service_category(service) != "compute":
            continue

        if days < IDLE_MIN_DAYS_ACTIVE:
            continue

        usage_ratio = usage_lookup.get((provider, service, rid))
        if usage_ratio is None or usage_ratio > IDLE_USAGE_RATIO_THRESHOLD:
            continue

        avg_cost = avg_cost_lookup.get((provider, service), 0)
        if avg_cost < IDLE_MIN_DAILY_COST:
            continue

        leaks.append({
            "leak_type": "IDLE_RESOURCE",
            "provider": provider,
            "service": service,
            "resource_id": rid,
            "reason": f"Low usage ({usage_ratio:.2f}) with daily cost ${avg_cost:.2f}"
        })

    return leaks

# ===================== ZOMBIE RESOURCES =====================

def detect_zombie_resources(lifespan_results, usage_ratio_data):
    leaks = []

    usage_lookup = {
        (u["provider"], u["service"], u["resource_id"]): u["usage_to_cost_ratio"]
        for u in usage_ratio_data
    }

    for r in lifespan_results:
        provider = r["provider"]
        service = r["service"]
        rid = r["resource_id"]
        days = r["days_active"]

        usage_ratio = usage_lookup.get((provider, service, rid))
        if usage_ratio is None:
            continue

        # Provider-aware thresholds
        if provider == "AWS":
            threshold = 0.05
        elif provider == "Azure":
            threshold = 0.1
        else:  # GCP
            threshold = 3.0

        if days >= ZOMBIE_MIN_DAYS and usage_ratio < threshold:
            leaks.append({
                "leak_type": "ZOMBIE_RESOURCE",
                "provider": provider,
                "service": service,
                "resource_id": rid,
                "reason": f"Active {days} days with inefficient usage ({usage_ratio:.2f})"
            })

    return leaks

# ===================== RUNAWAY COSTS =====================

def detect_runaway_costs(daily_cost_df, usage_ratio_data):
    leaks = []

    if not usage_ratio_data:
        return leaks

    usage_lookup = (
        pd.DataFrame(usage_ratio_data)
        .groupby(["provider", "service"])["usage_to_cost_ratio"]
        .mean()
        .to_dict()
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
            "reason": f"Cost grew {growth:.1f}% in {len(costs)} days"
        })

    return leaks

# ===================== ALWAYS-ON HIGH COST =====================

def detect_always_on_high_cost(daily_cost_df, normalized_df):
    leaks = []

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

        category = get_service_category(service)
        if category not in {"compute", "database"}:
            continue

        if cost < ALWAYS_ON_MIN_DAILY_COST:
            continue

        service_days = days_present.get((provider, service), 0)
        presence_ratio = service_days / max(service_days, 1)

        if presence_ratio < ALWAYS_ON_PRESENCE_RATIO:
            continue

        rows = normalized_df[
            (normalized_df["provider"] == provider) &
            (normalized_df["service"] == service)
        ]

        ownership_cols = [
            c for c in rows.columns
            if any(k in c.lower() for k in ["owner", "project", "environment"])
        ]

        has_owner = False
        for c in ownership_cols:
            if rows[c].notna().any():
                has_owner = True
                break

        if has_owner:
            continue

        leaks.append({
            "leak_type": "ALWAYS_ON_HIGH_COST",
            "provider": provider,
            "service": service,
            "reason": f"Always-on service costing ${cost:.2f}/day with no ownership"
        })

    return leaks