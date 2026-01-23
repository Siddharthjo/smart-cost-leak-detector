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

from src.intelligence.severity.scorer import score_leaks
from src.insights.generator import generate_insights


# ---------------- INPUT ----------------

# Uncomment ONE at a time
# file_path = "data/raw/aws/synthetic_aws_cur_guaranteed_leaks.csv"
# file_path = "data/raw/azure/synthetic_azure_cost_guaranteed_leaks.csv"
file_path = "data/raw/gcp/synthetic_gcp_billing_guaranteed_leaks_realistic.csv"

df = load_csv(file_path)

is_valid, message = validate_csv(file_path, df)
print(message)

if not is_valid:
    raise ValueError("Invalid CSV input")

print("\nInput columns:")
print(sorted(df.columns))


# ---------------- PROVIDER DETECTION ----------------

# ✅ 1. TRUST provider column if present (normalized or semi-normalized CSVs)
if "provider" in df.columns:
    provider = df["provider"].iloc[0]

    if provider == "AWS":
        normalized_df = normalize_aws(df)
    elif provider == "Azure":
        normalized_df = normalize_azure(df)
    elif provider == "GCP":
        normalized_df = normalize_gcp(df)
    else:
        raise ValueError(f"Unknown provider value: {provider}")

    print(f"\nDetected provider from column: {provider}")

# ✅ 2. FALLBACK to schema markers (raw CSVs only)
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
        raise ValueError(
            f"Ambiguous or unknown billing format. Matches={matches}"
        )

    if matches["AWS"]:
        provider = "AWS"
        normalized_df = normalize_aws(df)
    elif matches["AZURE"]:
        provider = "AZURE"
        normalized_df = normalize_azure(df)
    else:
        provider = "GCP"
        normalized_df = normalize_gcp(df)

    print(f"\nDetected provider from schema: {provider}")


print("\nFinal normalized columns:", list(normalized_df.columns))
print("Row count after normalization:", len(normalized_df))
print(normalized_df.head())


# ---------------- FEATURE ENGINEERING ----------------

daily_cost_df = daily_cost_per_service(normalized_df)
print("\nDaily cost per service:")
print(daily_cost_df.head())

trend_results = cost_trend_per_service(daily_cost_df)
print("\nCost trend per service:")
print(trend_results)

lifespan_results = resource_lifespan(normalized_df)
print("\nResource lifespan:")
print(lifespan_results)

ratio_results = usage_cost_ratio(normalized_df)
print("\nUsage to cost ratio:")
print(ratio_results)


# ---------------- LEAK DETECTION ----------------

zombie_leaks = detect_zombie_resources(
    lifespan_results,
    ratio_results
)
print("\nZombie resource leaks:")
print(zombie_leaks)

idle_leaks = detect_idle_resources(
    lifespan_results,
    ratio_results,
    daily_cost_df
)
print("\nIdle resource leaks:")
print(idle_leaks)

runaway_leaks = detect_runaway_costs(
    daily_cost_df,
    ratio_results
)
print("\nRunaway cost leaks:")
print(runaway_leaks)

always_on_leaks = detect_always_on_high_cost(
    daily_cost_df,
    normalized_df
)
print("\nAlways-on high cost leaks:")
print(always_on_leaks)


# ---------------- SCORING & INSIGHTS ----------------

all_leaks = (
    zombie_leaks +
    idle_leaks +
    runaway_leaks +
    always_on_leaks
)

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