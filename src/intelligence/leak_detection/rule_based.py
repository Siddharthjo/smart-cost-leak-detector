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

def detect_zombie_resources(resource_lifespans, min_days=30):
    """
    Detects zombie resources.

    Rule:
    - Resource active for many days
    - Still incurring cost

    min_days: number of days after which a resource is considered zombie
    """

    leaks = []

    for item in resource_lifespans:
        days_active = item.get("days_active", 0)

        if days_active >= min_days:
            leaks.append({
                "leak_type": "ZOMBIE_RESOURCE",
                "provider": item["provider"],
                "service": item["service"],
                "resource_id": item["resource_id"],
                "reason": f"Resource active for {days_active} days"
            })

    return leaks

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