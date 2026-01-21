from src.ingestion.csv_loader import load_csv
from src.ingestion.file_validator import validate_csv
from src.ingestion.csv_type_detector import detect_csv_type

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


file_path = "data/raw/aws/synthetic_aws_cur_full.csv"
# file_path = "data/raw/azure/synthetic_azure_cost_export.csv"
# file_path = "data/raw/gcp/synthetic_gcp_billing_export.csv"

df = load_csv(file_path)

is_valid, message = validate_csv(file_path, df)
print(message)

if not is_valid:
    raise ValueError("Invalid CSV input")

csv_type = detect_csv_type(df)
print("Detected CSV type:", csv_type)

# ---------------- NORMALIZATION ----------------

if csv_type != "COST_USAGE":
    raise ValueError("Unsupported CSV type")

# AWS
if "line_item_usage_start_date" in df.columns:
    normalized_df = normalize_aws(df)

# Azure
elif "UsageDate" in df.columns:
    normalized_df = normalize_azure(df)

# GCP
elif "usage_start_time" in df.columns:
    normalized_df = normalize_gcp(df)

else:
    raise ValueError("Unsupported COST_USAGE format")

print("Final normalized columns:", list(normalized_df.columns))
print("Row count after normalization:", len(normalized_df))
print(normalized_df.head())

# ---------------- INTELLIGENCE ----------------

daily_cost_df = daily_cost_per_service(normalized_df)
print("Daily cost per service:")
print(daily_cost_df.head())

trend_results = cost_trend_per_service(daily_cost_df)
print("Cost trend per service:")
print(trend_results)

lifespan_results = resource_lifespan(normalized_df)
print("Resource lifespan:")
print(lifespan_results)

ratio_results = usage_cost_ratio(normalized_df)
print("Usage to cost ratio:")
print(ratio_results)

idle_leaks = detect_idle_resources(ratio_results)
print("Idle resource leaks:")
print(idle_leaks)

zombie_leaks = detect_zombie_resources(lifespan_results)
print("Zombie resource leaks:")
print(zombie_leaks)

runaway_leaks = detect_runaway_costs(trend_results)
print("Runaway cost leaks:")
print(runaway_leaks)

always_on_leaks = detect_always_on_high_cost(daily_cost_df)
print("Always-on high cost leaks:")
print(always_on_leaks)

# ---------------- SCORING & INSIGHTS ----------------

all_leaks = (
    idle_leaks +
    zombie_leaks +
    runaway_leaks +
    always_on_leaks
)

scored_leaks = score_leaks(all_leaks)
print("Scored leaks:")
print(scored_leaks)

insights = generate_insights(scored_leaks)

print("\n=== COST LEAK INSIGHTS ===")
if not insights:
    print("✅ No cost leaks detected.")
else:
    for insight in insights:
        print(insight)
        print("-" * 50)