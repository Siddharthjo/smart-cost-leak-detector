import re
import pandas as pd
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# ===================== LEAK TYPE WEIGHTS =====================

LEAK_TYPE_WEIGHTS = {
    "RI_SAVINGS_PLAN_WASTE":  50,
    "RI_UNUSED_RESERVATION":  50,
    "ZOMBIE_RESOURCE":        60,
    "RUNAWAY_COST":           55,
    "ALWAYS_ON_HIGH_COST":    35,
    "IDLE_DATABASE":          30,
    "ORPHANED_STORAGE":       25,
    "IDLE_RESOURCE":          20,
    "SNAPSHOT_SPRAWL":        15,
    "UNTAGGED_RESOURCE":      10,
}

# ===================== SEVERITY LABELS =====================

SEVERITY_LABELS = [
    (65, "HIGH"),
    (35, "MEDIUM"),
    (0,  "LOW"),
]


def _severity_label(score: int) -> str:
    for threshold, label in SEVERITY_LABELS:
        if score >= threshold:
            return label
    return "LOW"


# ===================== DOLLAR IMPACT ESTIMATION =====================

def _estimate_monthly_waste(
    leak: Dict,
    avg_daily_lookup: dict,
    lifespan_lookup: dict,
) -> float:
    """
    Estimate monthly dollar waste per leak type.

    RI leaks extract the figure directly from the reason string (exact billing data).
    All other types estimate from average daily cost.
    """
    leak_type   = leak.get("leak_type", "")
    provider    = leak.get("provider")
    service     = leak.get("service")
    resource_id = leak.get("resource_id")

    # If the leak already carries an estimate (e.g. from idle_db detector), trust it
    if leak.get("estimated_monthly_waste"):
        return float(leak["estimated_monthly_waste"])

    # RI: exact figure is in the reason string
    if leak_type in {"RI_SAVINGS_PLAN_WASTE", "RI_UNUSED_RESERVATION"}:
        match = re.search(r"\$([\d,]+\.?\d*)", leak.get("reason", ""))
        if match:
            return float(match.group(1).replace(",", ""))

    avg_daily = avg_daily_lookup.get((provider, service), 0.0)

    multipliers = {
        "ZOMBIE_RESOURCE":     1.0,
        "IDLE_RESOURCE":       1.0,
        "IDLE_DATABASE":       1.0,
        "ALWAYS_ON_HIGH_COST": 1.0,
        "RUNAWAY_COST":        0.3,   # only the excess portion
        "ORPHANED_STORAGE":    0.5,
        "SNAPSHOT_SPRAWL":     0.3,
        "UNTAGGED_RESOURCE":   0.2,   # low confidence, conservative
    }

    return round(avg_daily * 30 * multipliers.get(leak_type, 0.2), 2)


# ===================== CONFIDENCE SCORING =====================

def _confidence(
    leak: Dict,
    lifespan_lookup: dict,
    avg_daily_lookup: dict,
) -> str:
    """
    HIGH:   14+ days of history, cost data present
    MEDIUM: 7-13 days OR cost data present but history thin
    LOW:    <7 days or no supporting data
    """
    provider    = leak.get("provider")
    service     = leak.get("service")
    resource_id = leak.get("resource_id")

    days_active = lifespan_lookup.get(
        (provider, service, resource_id), 0
    )
    avg_daily = avg_daily_lookup.get((provider, service), 0.0)
    has_zscore = "z_score" in leak

    if days_active >= 14 and avg_daily > 0:
        return "HIGH"
    if days_active >= 7 or (avg_daily > 0 and has_zscore):
        return "MEDIUM"
    if leak.get("leak_type") in {
        "RI_SAVINGS_PLAN_WASTE",
        "RI_UNUSED_RESERVATION",
    }:
        return "HIGH"   # direct billing data — always high confidence
    return "LOW"


# ===================== MAIN SCORER =====================

def score_leaks(
    leaks: List[Dict],
    daily_cost_df: Optional[pd.DataFrame] = None,
    lifespan_results: Optional[list] = None,
) -> List[Dict]:
    """
    Assign severity score, dollar impact, confidence, and recommended action.

    Args:
        leaks:            Raw leak dicts from detectors
        daily_cost_df:    Feature-engineered daily cost DataFrame (for dollar impact)
        lifespan_results: Resource lifespan list (for confidence scoring)

    Returns:
        Enriched leak dicts sorted by severity_score DESC, monthly_waste DESC
    """
    # Build lookups
    avg_daily_lookup: dict = {}
    if daily_cost_df is not None and not daily_cost_df.empty:
        avg_daily_lookup = (
            daily_cost_df
            .groupby(["provider", "service"])["daily_cost"]
            .mean()
            .to_dict()
        )

    lifespan_lookup: dict = {}
    if lifespan_results:
        for r in lifespan_results:
            key = (r["provider"], r["service"], r.get("resource_id"))
            lifespan_lookup[key] = r["days_active"]

    scored: List[Dict] = []

    for leak in leaks:
        score    = 0
        leak_type = leak.get("leak_type", "")
        reason   = leak.get("reason", "").lower()

        # Base score from leak type
        score += LEAK_TYPE_WEIGHTS.get(leak_type, 0)

        # Risk amplifiers
        if any(k in reason for k in ["grew", "increase", "spike", "stddevs above"]):
            score += 10
        if any(k in reason for k in ["30 days", "60 days", "90 days", "long-running"]):
            score += 10
        if any(k in reason for k in ["storage", "snapshot", "backup", "egress"]):
            score += 5
        if any(k in reason for k in ["no ownership", "untagged"]):
            score += 5

        # Days-active amplifier: longer-lived waste is harder to miss intentionally
        provider    = leak.get("provider")
        service     = leak.get("service")
        resource_id = leak.get("resource_id")
        days_active = lifespan_lookup.get((provider, service, resource_id), 0)
        if days_active >= 30:
            score += 15
        elif days_active >= 14:
            score += 8

        # Z-score bonus (statistical confidence of runaway signal)
        z = leak.get("z_score")
        if z is not None:
            if z > 3.0:
                score += 15
            elif z > LEAK_TYPE_WEIGHTS.get("RUNAWAY_COST", 40) / 10:
                score += 8

        # Confidence penalty for low-signal types
        if leak_type in {"UNTAGGED_RESOURCE", "SNAPSHOT_SPRAWL"}:
            score -= 5

        score    = max(0, min(score, 100))
        severity = _severity_label(score)

        monthly_waste = _estimate_monthly_waste(leak, avg_daily_lookup, lifespan_lookup)
        confidence    = _confidence(leak, lifespan_lookup, avg_daily_lookup)

        if severity == "HIGH":
            action = "Immediate investigation and remediation required"
        elif severity == "MEDIUM":
            action = "Review and schedule remediation within this sprint"
        else:
            action = "Monitor — clean up when convenient"

        scored.append({
            **leak,
            "severity_score":           score,
            "severity":                 severity,
            "recommended_action":       action,
            "estimated_monthly_waste":  monthly_waste,
            "estimated_annual_waste":   round(monthly_waste * 12, 2),
            "confidence":               confidence,
        })

    # Primary sort: severity score DESC; secondary: dollar impact DESC
    scored.sort(key=lambda x: (-x["severity_score"], -x["estimated_monthly_waste"]))
    return scored
