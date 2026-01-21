def score_idle_resource(leak):
    """
    Severity scoring for IDLE_RESOURCE leaks
    """

    severity = "LOW"
    action = "Review resource usage"

    reason = leak.get("reason", "").lower()

    # crude cost-based inference from reason text (v1 logic)
    if "0." in reason or "low" in reason:
        severity = "LOW"
        action = "Consider downsizing or stopping resource"
    else:
        severity = "MEDIUM"
        action = "Downsize or stop unused resource"

    return {
        **leak,
        "severity": severity,
        "recommended_action": action
    }


def score_zombie_resource(leak):
    """
    Severity scoring for ZOMBIE_RESOURCE leaks
    """

    severity = "MEDIUM"
    action = "Investigate resource ownership"

    reason = leak.get("reason", "")
    days = 0

    # extract days from reason string
    for token in reason.split():
        if token.isdigit():
            days = int(token)
            break

    if days >= 60:
        severity = "HIGH"
        action = "Delete or archive long-running unused resource"

    return {
        **leak,
        "severity": severity,
        "recommended_action": action
    }

def score_leaks(leaks):
    """
    Routes leaks to appropriate severity scorer
    """

    scored = []

    for leak in leaks:
        leak_type = leak.get("leak_type")

        if leak_type == "IDLE_RESOURCE":
            scored.append(score_idle_resource(leak))

        elif leak_type == "ZOMBIE_RESOURCE":
            scored.append(score_zombie_resource(leak))

        else:
            scored.append({
                **leak,
                "severity": "MEDIUM",
                "recommended_action": "Review this leak"
            })

    return scored