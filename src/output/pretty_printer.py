from collections import defaultdict
from typing import List, Dict


# ===================== LEAK PRIORITY =====================
# Higher number = more important if severity_score ties

LEAK_PRIORITY = {
    "RUNAWAY_COST": 100,
    "ALWAYS_ON_HIGH_COST": 90,
    "ZOMBIE_RESOURCE": 80,
    "IDLE_DATABASE": 70,
    "IDLE_RESOURCE": 60,
    "ORPHANED_STORAGE": 50,
    "SNAPSHOT_SPRAWL": 40,
    "UNTAGGED_RESOURCE": 30,
}


# ===================== PRIMARY LEAK SELECTION =====================

def select_primary_leaks(scored_leaks: List[Dict]) -> List[Dict]:
    """
    For each resource, keep only the most important leak.
    Importance is decided by:
      1) severity_score
      2) leak priority
    """

    grouped = defaultdict(list)

    for leak in scored_leaks:
        # Some leaks are service-level (no resource_id)
        key = leak.get("resource_id") or f"{leak['provider']}:{leak['service']}"
        grouped[key].append(leak)

    primary = []

    for _, leaks in grouped.items():
        leaks.sort(
            key=lambda l: (
                -l.get("severity_score", 0),
                -LEAK_PRIORITY.get(l.get("leak_type", ""), 0),
            )
        )
        primary.append(leaks[0])

    return primary


# ===================== CLEAN OUTPUT =====================

def print_clean_output(scored_leaks: List[Dict]) -> None:
    if not scored_leaks:
        print("✅ No cost leaks detected.")
        return

    # Group by leak type for display
    grouped = defaultdict(list)
    for leak in scored_leaks:
        grouped[leak["leak_type"]].append(leak)

    # Severity counts
    severity_count = defaultdict(int)
    for leak in scored_leaks:
        severity_count[leak["severity"]] += 1

    # ---------------- HEADER ----------------
    print("\n==============================")
    print(" CLOUD COST LEAK REPORT")
    print("==============================\n")

    print(f"🔥 High severity: {severity_count.get('HIGH', 0)}")
    print(f"⚠️  Medium severity: {severity_count.get('MEDIUM', 0)}")
    print(f"ℹ️  Low severity: {severity_count.get('LOW', 0)}\n")

    # ---------------- BODY ----------------
    for leak_type in sorted(
        grouped.keys(),
        key=lambda k: -LEAK_PRIORITY.get(k, 0)
    ):
        leaks = grouped[leak_type]
        print(f"{leak_type} ({len(leaks)})")

        for l in leaks:
            print(f"  • {l['provider']} | {l['service']}")

            if l.get("resource_id"):
                print(f"    Resource: {l['resource_id']}")

            print(f"    Reason: {l['reason']}")
            print(f"    Action: {l['recommended_action']}")
            print()

    print("✔ Analysis complete")