def generate_insights(scored_leaks):
    """
    Generates human-readable insights from scored leaks
    """

    insights = []

    if not scored_leaks:
        insights.append("✅ No cost leaks detected. Your cloud usage looks healthy.")
        return insights

    for leak in scored_leaks:
        severity = leak.get("severity")
        leak_type = leak.get("leak_type")
        provider = leak.get("provider")
        service = leak.get("service")
        resource_id = leak.get("resource_id", "N/A")
        reason = leak.get("reason")
        action = leak.get("recommended_action")

        insight = (
            f"[{severity}] {leak_type} detected in {provider} {service}\n"
            f"→ Resource: {resource_id}\n"
            f"→ Reason: {reason}\n"
            f"→ Recommended action: {action}"
        )

        insights.append(insight)

    return insights