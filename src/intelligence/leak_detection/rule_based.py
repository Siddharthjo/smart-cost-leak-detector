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