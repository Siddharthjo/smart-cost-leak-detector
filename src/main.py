from src.ingestion.csv_loader import load_csv
from src.ingestion.file_validator import validate_csv
from src.ingestion.csv_type_detector import detect_csv_type

from src.normalization.aws_normalizer import normalize_aws
from src.normalization.azure_normalizer import normalize_azure

from src.intelligence.feature_engineering.cost_features import daily_cost_per_service
from src.intelligence.feature_engineering.cost_features import cost_trend_per_service
from src.intelligence.feature_engineering.cost_features import resource_lifespan
from src.intelligence.feature_engineering.cost_features import usage_cost_ratio

from src.intelligence.leak_detection.rule_based import detect_idle_resources
from src.intelligence.leak_detection.rule_based import detect_zombie_resources
from src.intelligence.leak_detection.rule_based import detect_runaway_costs
from src.intelligence.leak_detection.rule_based import detect_always_on_high_cost

from src.intelligence.severity.scorer import score_leaks

from src.insights.generator import generate_insights

file_path = "data/raw/aws/synthetic_aws_cur_full.csv"

df = load_csv(file_path)

is_valid, message = validate_csv(file_path, df)
print(message)

if is_valid:
    csv_type = detect_csv_type(df)
    print("Detected CSV type:", csv_type)

    if csv_type == "COST_USAGE":
        if "UsageStartDate" in df:
            normalized_df = normalize_aws(df)
        else:
            normalized_df = normalize_azure(df)

        print("Final normalized columns:", list(normalized_df.columns))
        print("Row count after normalization:", len(normalized_df))
        print(normalized_df.head())

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
        for insight in insights:
            print(insight)
            print("-" * 50)