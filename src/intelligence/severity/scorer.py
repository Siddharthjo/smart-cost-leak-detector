def is_production_resource(leak):
    """
    Detect if a resource is production based on tags or labels.
    """
    possible_keys = [
        "environment",
        "env",
        "labels.environment",
        "resource_tags_user_environment",
    ]

    for key in possible_keys:
        value = leak.get(key)
        if value and isinstance(value, str):
            if value.lower() in {"prod", "production"}:
                return True

    return False

def score_idle_resource(leak):
    """
    Severity scoring for IDLE_RESOURCE leaks
    """

    # Default for idle is MEDIUM
    severity = "MEDIUM"
    action = "Review and optimize idle resource usage"

    # Production idle is HIGH severity
    if is_production_resource(leak):
        severity = "HIGH"
        action = "Scale down or stop idle production resource immediately"

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

    # Production downgrade
    if is_production_resource(leak):
        severity = "LOW"
        action = "Verify necessity of this production resource"

    # Extract days active from reason
    reason = leak.get("reason", "")
    days = 0
    for token in reason.split():
        if token.isdigit():
            days = int(token)
            break

    # Very long-running zombies override severity
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

def score_runaway_cost(leak):
    severity = "MEDIUM"
    action = "Investigate recent cost increase"

    if is_production_resource(leak):
        severity = "HIGH"
        action = "Investigate runaway production cost immediately"

    return {
        **leak,
        "severity": severity,
        "recommended_action": action
    }

def score_always_on_high_cost(leak):
    return {
        **leak,
        "severity": "HIGH",
        "recommended_action": "Assign ownership and review necessity of this always-on service",
    }