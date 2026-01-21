from src.ingestion.csv_loader import load_csv
from src.ingestion.file_validator import validate_csv
from src.ingestion.csv_type_detector import detect_csv_type

from src.normalization.aws_normalizer import normalize_aws
from src.normalization.azure_normalizer import normalize_azure

from src.intelligence.feature_engineering.cost_features import daily_cost_per_service
from src.intelligence.feature_engineering.cost_features import cost_trend_per_service
from src.intelligence.feature_engineering.cost_features import resource_lifespan

file_path = "data/raw/azure/cost-analysis.csv"

df = load_csv(file_path)

is_valid, message = validate_csv(file_path, df)
print(message)

if is_valid:
    csv_type = detect_csv_type(df.columns)
    print("Detected CSV type:", csv_type)

    if csv_type == "COST_USAGE":
        if "UsageStartDate" in df.columns:
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