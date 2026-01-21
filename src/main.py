from src.ingestion.csv_loader import load_csv
from src.ingestion.file_validator import validate_csv
from src.ingestion.csv_type_detector import detect_csv_type

from src.normalization.aws_normalizer import normalize_aws
from src.normalization.azure_normalizer import normalize_azure

file_path = "data/raw/aws/payments_due_2026-01-20T07_16_02.706Z.csv"

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