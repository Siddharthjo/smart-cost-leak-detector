from collections import defaultdict
from typing import List, Dict


# ===================== LEAK PRIORITY =====================

LEAK_PRIORITY = {
    "RI_SAVINGS_PLAN_WASTE":  120,
    "RI_UNUSED_RESERVATION":  120,
    "RUNAWAY_COST":           100,
    "ALWAYS_ON_HIGH_COST":     90,
    "ZOMBIE_RESOURCE":         80,
    "IDLE_DATABASE":           70,
    "IDLE_RESOURCE":           60,
    "ORPHANED_STORAGE":        50,
    "SNAPSHOT_SPRAWL":         40,
    "UNTAGGED_RESOURCE":       30,
}


# ===================== PRIMARY LEAK SELECTION =====================

def select_primary_leaks(scored_leaks: List[Dict]) -> List[Dict]:
    """
    For each resource, keep only the highest-importance leak.
    Importance: severity_score → leak priority → monthly waste.
    """
    grouped: dict = defaultdict(list)

    for leak in scored_leaks:
        key = leak.get("resource_id") or f"{leak['provider']}:{leak['service']}"
        grouped[key].append(leak)

    primary: List[Dict] = []
    for _, leaks in grouped.items():
        leaks.sort(key=lambda l: (
            -l.get("severity_score", 0),
            -LEAK_PRIORITY.get(l.get("leak_type", ""), 0),
            -l.get("estimated_monthly_waste", 0),
        ))
        primary.append(leaks[0])

    return primary


# ===================== CLEAN CONSOLE OUTPUT =====================

def print_clean_output(scored_leaks: List[Dict]) -> None:
    if not scored_leaks:
        print("✅ No cost leaks detected.")
        return

    grouped: dict = defaultdict(list)
    for leak in scored_leaks:
        grouped[leak["leak_type"]].append(leak)

    severity_count: dict = defaultdict(int)
    for leak in scored_leaks:
        severity_count[leak["severity"]] += 1

    total_monthly = sum(l.get("estimated_monthly_waste", 0) for l in scored_leaks)
    new_count     = sum(1 for l in scored_leaks if l.get("status") == "NEW")

    # ---- HEADER ----
    print("\n" + "="*60)
    print(" CLOUD COST LEAK REPORT")
    print("="*60)
    print(f"\n  🔥 High severity   : {severity_count.get('HIGH', 0)}")
    print(f"  ⚠️  Medium severity  : {severity_count.get('MEDIUM', 0)}")
    print(f"  ℹ️  Low severity     : {severity_count.get('LOW', 0)}")
    print(f"\n  💸 Est. monthly waste : ${total_monthly:,.2f}")
    print(f"  💸 Est. annual waste  : ${total_monthly * 12:,.2f}")
    if new_count:
        print(f"  🆕 New since last run : {new_count}")
    print()

    # ---- BODY ----
    for leak_type in sorted(
        grouped.keys(),
        key=lambda k: -LEAK_PRIORITY.get(k, 0)
    ):
        leaks = grouped[leak_type]
        print(f"{'─'*60}")
        print(f"  {leak_type}  ({len(leaks)} finding{'s' if len(leaks) > 1 else ''})")
        print()

        for l in leaks:
            status_tag = " [NEW]" if l.get("status") == "NEW" else ""
            print(f"  • {l['provider']} | {l['service']}{status_tag}")

            if l.get("resource_id"):
                print(f"    Resource   : {l['resource_id']}")

            print(f"    Reason     : {l['reason']}")
            print(f"    Severity   : {l['severity']} (score: {l.get('severity_score', 'N/A')})")
            print(f"    Confidence : {l.get('confidence', 'N/A')}")

            monthly = l.get("estimated_monthly_waste", 0)
            annual  = l.get("estimated_annual_waste",  0)
            if monthly:
                print(f"    Est. waste : ${monthly:,.2f}/mo  (${annual:,.2f}/yr)")

            # LLM recommendation block
            rec = l.get("llm_recommendation")
            if rec:
                print(f"    ── AI Recommendation ──────────────────────────")
                if rec.get("root_cause"):
                    print(f"    Root cause : {rec['root_cause']}")
                if rec.get("fix_command"):
                    print(f"    Fix        : {rec['fix_command']}")
                if rec.get("estimated_remediation_minutes"):
                    print(f"    Time to fix: {rec['estimated_remediation_minutes']} min")
                if rec.get("risk_level"):
                    print(f"    Risk       : {rec['risk_level']} — {rec.get('risk_note', '')}")
            else:
                print(f"    Action     : {l.get('recommended_action', 'N/A')}")

            print()

    print("="*60)
    print("✔  Analysis complete\n")
