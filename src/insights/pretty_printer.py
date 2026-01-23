from collections import defaultdict

def print_clean_output(scored_leaks):
    if not scored_leaks:
        print("✅ No cost leaks detected.")
        return

    print("\n==============================")
    print(" CLOUD COST LEAK REPORT")
    print("==============================\n")

    # ---------------- Severity Summary ----------------
    severity_count = defaultdict(int)
    for leak in scored_leaks:
        severity_count[leak.get("severity", "UNKNOWN")] += 1

    print(f"🔥 High severity: {severity_count.get('HIGH', 0)}")
    print(f"⚠️  Medium severity: {severity_count.get('MEDIUM', 0)}")
    print(f"ℹ️  Low severity: {severity_count.get('LOW', 0)}\n")

    # ---------------- Group by Leak Type ----------------
    grouped = defaultdict(list)
    for leak in scored_leaks:
        grouped[leak["leak_type"]].append(leak)

    for leak_type in sorted(grouped.keys()):
        leaks = grouped[leak_type]
        print(f"{leak_type} ({len(leaks)})")

        for l in leaks:
            print(f"  • {l.get('provider')} | {l.get('service')}")

            if l.get("resource_id"):
                print(f"    Resource: {l['resource_id']}")

            print(f"    Reason: {l.get('reason')}")
            print(f"    Action: {l.get('recommended_action')}")
            print()

    # ---------------- Cost Summary ----------------
    total_waste = sum(
        l.get("estimated_monthly_waste", 0)
        for l in scored_leaks
        if isinstance(l.get("estimated_monthly_waste", 0), (int, float))
    )

    if total_waste > 0:
        print(f"💸 Estimated Monthly Waste: ${round(total_waste, 2)}\n")

    print("✔ Analysis complete")