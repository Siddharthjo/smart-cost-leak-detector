from typing import List, Dict

# ===================== LEAK TYPE WEIGHTS =====================

LEAK_TYPE_WEIGHTS = {
    "ZOMBIE_RESOURCE": 40,
    "RUNAWAY_COST": 40,
    "ALWAYS_ON_HIGH_COST": 35,
    "IDLE_DATABASE": 30,
    "ORPHANED_STORAGE": 25,
    "IDLE_RESOURCE": 20,
    "SNAPSHOT_SPRAWL": 15,
    "UNTAGGED_RESOURCE": 10,
}

# ===================== SEVERITY LABELS =====================

SEVERITY_LABELS = [
    (70, "HIGH"),
    (35, "MEDIUM"),
    (0, "LOW"),
]

# ===================== HELPERS =====================

def _severity_label(score: int) -> str:
    for threshold, label in SEVERITY_LABELS:
        if score >= threshold:
            return label
    return "LOW"

# ===================== MAIN SCORER =====================

def score_leaks(leaks: List[Dict]) -> List[Dict]:
    """
    Assign severity score and severity label.
    Directional only — no cost estimation.
    """

    scored = []

    for leak in leaks:
        score = 0

        # -------- BASE SCORE FROM LEAK TYPE --------
        leak_type = leak.get("leak_type", "")
        score += LEAK_TYPE_WEIGHTS.get(leak_type, 0)

        reason = leak.get("reason", "").lower()

        # -------- RISK SIGNALS --------
        if any(k in reason for k in ["grew", "increase", "spike"]):
            score += 10

        if any(k in reason for k in ["30 days", "60 days", "90 days", "long-running"]):
            score += 10

        if any(k in reason for k in ["storage", "snapshot", "backup", "egress"]):
            score += 5

        if any(k in reason for k in ["no ownership", "untagged"]):
            score += 5

        # -------- CONFIDENCE PENALTY --------
        if leak_type in {
            "UNTAGGED_RESOURCE",
            "SNAPSHOT_SPRAWL",
        }:
            score -= 5

        # Clamp score
        score = max(0, min(score, 100))

        severity = _severity_label(score)

        # -------- RECOMMENDED ACTION --------
        if severity == "HIGH":
            action = "Immediate investigation and remediation required"
        elif severity == "MEDIUM":
            action = "Review and schedule remediation"
        else:
            action = "Monitor and clean up when convenient"

        scored.append({
            **leak,
            "severity_score": score,
            "severity": severity,
            "recommended_action": action,
        })

    # -------- SORT: MOST IMPORTANT FIRST --------
    scored.sort(key=lambda x: -x["severity_score"])

    return scored