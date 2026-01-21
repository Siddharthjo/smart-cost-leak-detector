from src.ingestion.csv_loader import load_csv
from src.ingestion.file_validator import validate_csv

file_path = "data/raw/aws/payments_due_2026-01-20T07_16_02.706Z.csv"

df = load_csv(file_path)

is_valid, message = validate_csv(file_path, df)

print(message)

if is_valid:
    print("Rows:", len(df))
    print("Columns:", list(df.columns))