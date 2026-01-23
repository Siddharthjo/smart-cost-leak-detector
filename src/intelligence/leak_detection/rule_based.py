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

def detect_idle_resources(usage_cost_ratios, threshold=0.1):
    """
    Detects idle or over-provisioned resources.

    Rule:
    - usage_to_cost_ratio very low
    - cost is non-zero

    threshold: lower means stricter detection
    """

    leaks = []

    for item in usage_cost_ratios:
        ratio = item.get("usage_to_cost_ratio")

        if ratio is None:
            continue

        if ratio < threshold:
            leaks.append({
                "leak_type": "IDLE_RESOURCE",
                "provider": item["provider"],
                "service": item["service"],
                "resource_id": item["resource_id"],
                "reason": f"Low usage to cost ratio ({ratio:.4f})"
            })

    return leaks

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

def detect_runaway_costs(cost_trends):
    """
    Detects runaway cost increases.

    Rule:
    - Cost trend is INCREASING
    """

    leaks = []

    for item in cost_trends:
        if item.get("trend") == "INCREASING":
            leaks.append({
                "leak_type": "RUNAWAY_COST",
                "provider": item["provider"],
                "service": item["service"],
                "reason": "Cost increasing over time"
            })

    return leaks

def detect_always_on_high_cost(daily_cost_df, threshold=100):
    """
    Detects services that are always on and consistently expensive.

    Rule:
    - Average daily cost above threshold
    """

    leaks = []

    if daily_cost_df.empty:
        return leaks

    grouped = daily_cost_df.groupby(["provider", "service"])

    for (provider, service), group in grouped:
        avg_cost = group["daily_cost"].mean()

        if avg_cost >= threshold:
            leaks.append({
                "leak_type": "ALWAYS_ON_HIGH_COST",
                "provider": provider,
                "service": service,
                "reason": f"Average daily cost {avg_cost:.2f} exceeds threshold {threshold}"
            })

    return leaks