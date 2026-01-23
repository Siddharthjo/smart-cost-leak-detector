from src.ingestion.csv_loader import load_csv
from src.ingestion.file_validator import validate_csv

from src.normalization.aws_normalizer import normalize_aws
from src.normalization.azure_normalizer import normalize_azure
from src.normalization.gcp_normalizer import normalize_gcp

from src.intelligence.feature_engineering.cost_features import (
    daily_cost_per_service,
    cost_trend_per_service,
    resource_lifespan,
    usage_cost_ratio,
)

from src.intelligence.leak_detection.rule_based import (
    detect_idle_resources,
    detect_zombie_resources,
    detect_runaway_costs,
    detect_always_on_high_cost,
)

from src.intelligence.leak_detection.structural import (
    detect_orphaned_storage,
    detect_idle_databases,
    detect_snapshot_sprawl,
    detect_untagged_resources,
)

from src.intelligence.severity.scorer import score_leaks
from src.insights.generator import generate_insights


# ================== INPUT ==================

# Uncomment ONE at a time
file_path = "data/raw/aws/synthetic_aws_cur_guaranteed_leaks.csv"
# file_path = "data/raw/azure/synthetic_azure_cost_guaranteed_leaks.csv"
# file_path = "data/raw/gcp/synthetic_gcp_billing_guaranteed_leaks_realistic.csv"

df = load_csv(file_path)

is_valid, message = validate_csv(file_path, df)
print(message)

if not is_valid:
    raise ValueError("Invalid CSV input")

print("\nInput columns:")
print(sorted(df.columns))


# ================== PROVIDER DETECTION ==================

if "provider" in df.columns:
    provider = df["provider"].iloc[0].upper()
    print(f"\nDetected provider from column: {provider}")
else:
    cols = set(df.columns)

    aws_markers = {
        "line_item_usage_account_id",
        "line_item_line_item_type",
        "bill_payer_account_id",
    }

    azure_markers = {
        "SubscriptionId",
        "UsageDate",
        "MeterName",
    }

    gcp_markers = {
        "billing_account_id",
        "project_id",
        "service_description",
    }

    matches = {
        "AWS": bool(aws_markers & cols),
        "AZURE": bool(azure_markers & cols),
        "GCP": bool(gcp_markers & cols),
    }

    print("\nProvider marker matches:", matches)

    if sum(matches.values()) != 1:
        raise ValueError(f"Ambiguous or unknown billing format: {matches}")

    provider = next(k for k, v in matches.items() if v)

    print(f"\nDetected provider from schema: {provider}")


# ================== NORMALIZATION ==================

if provider == "AWS":
    normalized_df = normalize_aws(df)
elif provider == "AZURE":
    normalized_df = normalize_azure(df)
elif provider == "GCP":
    normalized_df = normalize_gcp(df)
else:
    raise ValueError(f"Unsupported provider: {provider}")

normalized_df["provider"] = provider

print("\nFinal normalized columns:", list(normalized_df.columns))
print("Row count after normalization:", len(normalized_df))
print(normalized_df.head())


# ================== FEATURE ENGINEERING ==================

daily_cost_df = daily_cost_per_service(normalized_df)
trend_results = cost_trend_per_service(daily_cost_df)
lifespan_results = resource_lifespan(normalized_df)
ratio_results = usage_cost_ratio(normalized_df)

print("\nDaily cost per service:")
print(daily_cost_df.head())

print("\nCost trend per service:")
print(trend_results)

print("\nResource lifespan:")
print(lifespan_results)

print("\nUsage to cost ratio:")
print(ratio_results)


# ================== LEAK DETECTION ==================

zombie_leaks = detect_zombie_resources(lifespan_results, ratio_results)
idle_leaks = detect_idle_resources(lifespan_results, ratio_results, daily_cost_df)
runaway_leaks = detect_runaway_costs(daily_cost_df, ratio_results)
always_on_leaks = detect_always_on_high_cost(daily_cost_df, normalized_df)

orphaned_storage_leaks = detect_orphaned_storage(normalized_df)
idle_db_leaks = detect_idle_databases(
    lifespan_results,
    ratio_results,
    daily_cost_df,
    normalized_df
)
snapshot_leaks = detect_snapshot_sprawl(normalized_df)
print("\nSnapshot sprawl leaks:")
print(snapshot_leaks)

untagged_leaks = detect_untagged_resources(normalized_df)


# ================== DEDUPLICATION ==================

def dedupe_leaks(leaks):
    seen = set()
    unique = []
    for l in leaks:
        key = (
            l.get("leak_type"),
            l.get("provider"),
            l.get("service"),
            l.get("resource_id"),
        )
        if key not in seen:
            seen.add(key)
            unique.append(l)
    return unique


all_leaks = dedupe_leaks(
    zombie_leaks +
    idle_leaks +
    runaway_leaks +
    always_on_leaks +
    orphaned_storage_leaks +
    idle_db_leaks +
    snapshot_leaks +
    untagged_leaks
)


# ================== SCORING & INSIGHTS ==================

scored_leaks = score_leaks(all_leaks)

print("\nScored leaks:")
print(scored_leaks)

insights = generate_insights(scored_leaks)

print("\n=== COST LEAK INSIGHTS ===")
if not insights:
    print("✅ No cost leaks detected.")
else:
    for insight in insights:
        print(insight)
        print("-" * 50)