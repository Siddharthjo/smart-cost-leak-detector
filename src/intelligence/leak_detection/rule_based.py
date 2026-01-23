# ---------------- ALWAYS-ON HIGH COST RULE ----------------

ALWAYS_ON_MIN_DAILY_COST = 100.0      # expensive
ALWAYS_ON_PRESENCE_RATIO = 0.9        # present on 90% of days

# ---------------- RUNAWAY RULE THRESHOLDS ----------------

RUNAWAY_COST_GROWTH_PERCENT = 30     # %
RUNAWAY_MIN_DAYS = 3
RUNAWAY_MIN_DAILY_COST = 2.0

# ---------------- IDLE RULE THRESHOLDS ----------------

IDLE_USAGE_RATIO_THRESHOLD = 5      # usage-to-cost ratio
IDLE_MIN_DAILY_COST = 1.0           # ignore tiny spend
IDLE_MIN_DAYS_ACTIVE = 3            # sustained idle window

# ---------------- ZOMBIE RULE THRESHOLDS ----------------

ZOMBIE_DAYS_THRESHOLD = 14
LOW_USAGE_RATIO_THRESHOLD = 5

# ---------------- SERVICE CATEGORIZATION ----------------

COMPUTE_SERVICES = {
    # AWS
    "ec2",
    # Azure
    "virtual machines",
    # GCP
    "compute engine",
}

STORAGE_SERVICES = {
    # AWS
    "s3",
    # Azure
    "storage",
    # GCP
    "cloud storage",
}

SERVERLESS_SERVICES = {
    # AWS
    "lambda",
    # Azure
    "functions",
    # GCP
    "cloud functions",
}

DATABASE_SERVICES = {
    # AWS
    "rds",
    # Azure
    "sql",
    "cosmos",
    # GCP
    "cloud sql",
}

def get_service_category(service_name: str) -> str:
    """
    Classify a cloud service into a category:
    compute, storage, serverless, database, other
    """

    if not service_name:
        return "other"

    service = service_name.lower()

    if any(key in service for key in COMPUTE_SERVICES):
        return "compute"

    if any(key in service for key in STORAGE_SERVICES):
        return "storage"

    if any(key in service for key in SERVERLESS_SERVICES):
        return "serverless"

    if any(key in service for key in DATABASE_SERVICES):
        return "database"

    return "other"

def detect_idle_resources(lifespan_data, usage_ratio_data, daily_cost_df):
    """
    Detect idle compute resources:
    Sustained low usage with meaningful cost.
    """

    idle_leaks = []

    # Build quick lookups
    usage_lookup = {
        (item["provider"], item["service"], item["resource_id"]): item["usage_to_cost_ratio"]
        for item in usage_ratio_data
    }

    daily_cost_lookup = (
        daily_cost_df
        .groupby(["provider", "service"])["daily_cost"]
        .mean()
        .to_dict()
    )

    for item in lifespan_data:
        provider = item["provider"]
        service = item["service"]
        resource_id = item["resource_id"]
        days_active = item["days_active"]

        category = get_service_category(service)

        # 1️⃣ Only compute resources
        if category != "compute":
            continue

        # 2️⃣ Must exist long enough
        if days_active < IDLE_MIN_DAYS_ACTIVE:
            continue

        usage_ratio = usage_lookup.get(
            (provider, service, resource_id), None
        )

        # 3️⃣ Must have sustained low usage
        if usage_ratio is None or usage_ratio > IDLE_USAGE_RATIO_THRESHOLD:
            continue

        avg_daily_cost = daily_cost_lookup.get(
            (provider, service), 0
        )

        # 4️⃣ Ignore tiny spend
        if avg_daily_cost < IDLE_MIN_DAILY_COST:
            continue

        idle_leaks.append({
            "leak_type": "IDLE_RESOURCE",
            "provider": provider,
            "service": service,
            "resource_id": resource_id,
            "reason": f"Compute resource with sustained low usage and daily cost ${avg_daily_cost:.2f}",
        })

    return idle_leaks

def detect_zombie_resources(lifespan_data, usage_ratio_data):
    """
    Detect zombie resources:
    Long-running compute resources with low usage.
    """

    zombies = []

    # Build lookup for usage ratio by resource
    usage_lookup = {
        (item["provider"], item["service"], item["resource_id"]): item["usage_to_cost_ratio"]
        for item in usage_ratio_data
    }

    for item in lifespan_data:
        provider = item["provider"]
        service = item["service"]
        resource_id = item["resource_id"]
        days_active = item["days_active"]

        category = get_service_category(service)

        # 1️⃣ Only compute resources
        if category != "compute":
            continue

        # 2️⃣ Must be active long enough
        if days_active < ZOMBIE_DAYS_THRESHOLD:
            continue

        usage_ratio = usage_lookup.get(
            (provider, service, resource_id), None
        )

        # 3️⃣ Must have low usage
        if usage_ratio is None or usage_ratio > LOW_USAGE_RATIO_THRESHOLD:
            continue

        zombies.append({
            "leak_type": "ZOMBIE_RESOURCE",
            "provider": provider,
            "service": service,
            "resource_id": resource_id,
            "reason": f"Compute resource running {days_active} days with low usage",
        })

    return zombies

def detect_runaway_costs(daily_cost_df, usage_ratio_data):
    """
    Detect runaway costs:
    Rapid cost increase without proportional usage growth.
    """

    runaway_leaks = []

    # Average usage ratio per service
    usage_ratio_lookup = (
        pd.DataFrame(usage_ratio_data)
        .groupby(["provider", "service"])["usage_to_cost_ratio"]
        .mean()
        .to_dict()
    )

    for (provider, service), group in daily_cost_df.groupby(["provider", "service"]):
        if len(group) < RUNAWAY_MIN_DAYS:
            continue

        group = group.sort_values("date")

        costs = group["daily_cost"].values

        # Ignore low-cost services
        if costs.mean() < RUNAWAY_MIN_DAILY_COST:
            continue

        # Calculate growth rate
        growth = ((costs[-1] - costs[0]) / max(costs[0], 0.01)) * 100

        if growth < RUNAWAY_COST_GROWTH_PERCENT:
            continue

        usage_ratio = usage_ratio_lookup.get((provider, service), None)

        # Usage must NOT justify growth
        if usage_ratio and usage_ratio > 10:
            continue

        runaway_leaks.append({
            "leak_type": "RUNAWAY_COST",
            "provider": provider,
            "service": service,
            "reason": f"Cost increased {growth:.1f}% over {len(costs)} days without matching usage growth",
        })

    return runaway_leaks

def detect_always_on_high_cost(daily_cost_df, normalized_df):
    """
    Detect always-on, high-cost services with no clear ownership.
    """

    leaks = []

    # Days present per service
    days_present = (
        daily_cost_df
        .groupby(["provider", "service"])["date"]
        .nunique()
        .to_dict()
    )

    total_days = daily_cost_df["date"].nunique()

    # Average daily cost per service
    avg_daily_cost = (
        daily_cost_df
        .groupby(["provider", "service"])["daily_cost"]
        .mean()
        .to_dict()
    )

    for (provider, service), avg_cost in avg_daily_cost.items():

        category = get_service_category(service)

        # 1️⃣ Only compute or database
        if category not in {"compute", "database"}:
            continue

        # 2️⃣ Must be expensive
        if avg_cost < ALWAYS_ON_MIN_DAILY_COST:
            continue

        # 3️⃣ Must be always-on
        presence_ratio = days_present.get((provider, service), 0) / max(total_days, 1)
        if presence_ratio < ALWAYS_ON_PRESENCE_RATIO:
            continue

        # 4️⃣ Ownership check (any tag is enough)
        service_rows = normalized_df[
            (normalized_df["provider"] == provider) &
            (normalized_df["service"] == service)
        ]

        has_owner = any(
            col for col in service_rows.columns
            if "owner" in col.lower() or "project" in col.lower() or "environment" in col.lower()
        )

        if has_owner:
            continue

        leaks.append({
            "leak_type": "ALWAYS_ON_HIGH_COST",
            "provider": provider,
            "service": service,
            "reason": f"Service costs ${avg_cost:.2f}/day and runs continuously with no clear ownership",
        })

    return leaks