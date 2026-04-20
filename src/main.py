"""
Smart Cloud Cost Leak Detector
===============================
Entry point. Runs the full detection pipeline.

Usage:
    python -m src.main --file data/raw/aws/cur.csv
    python -m src.main --file data/raw/aws/cur.csv --llm --output both
    python -m src.main --file data/raw/aws/cur.parquet --provider aws --output json
    python -m src.main --file data/raw/aws/cur.csv --no-forecast --output console
"""

import argparse
import logging
import sys

# ===================== LOGGING =====================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ===================== IMPORTS =====================

from src.ingestion.csv_loader import load_csv
from src.ingestion.file_validator import validate_csv

from src.pipeline import run_pipeline_from_df
from src.output.pretty_printer import print_clean_output
from src.output.report_writer import save_json_report, save_markdown_report


# ===================== CLI =====================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smart Cloud Cost Leak Detector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.main --file data/raw/aws/cur.csv
  python -m src.main --file data/raw/aws/cur.csv --llm --output both
  python -m src.main --file data/raw/aws/cur.parquet --provider aws
  python -m src.main --file data/raw/aws/cur.csv --no-forecast --output console
        """,
    )
    parser.add_argument("--file", required=True, help="Path to billing CSV or Parquet file")
    parser.add_argument("--provider", choices=["aws", "azure", "gcp"],
                        help="Cloud provider override (auto-detected if omitted)")
    parser.add_argument("--output", choices=["json", "markdown", "both", "console"],
                        default="both", help="Output format (default: both)")
    parser.add_argument("--llm", action="store_true",
                        help="Enrich HIGH/MEDIUM findings with Claude AI recommendations")
    parser.add_argument("--llm-max", type=int, default=10,
                        help="Max leaks to enrich with LLM (default: 10)")
    parser.add_argument("--api-key", help="Anthropic API key")
    parser.add_argument("--no-forecast", action="store_true",
                        help="Skip 30-day cost forecast computation")
    parser.add_argument("--top-untagged", type=int, default=20,
                        help="Max untagged resource leaks to surface (default: 20)")
    return parser.parse_args()


# ===================== PIPELINE =====================

def run_pipeline(args: argparse.Namespace) -> list:
    logger.info(f"Starting pipeline — file: {args.file}")

    try:
        df = load_csv(args.file)
    except Exception as e:
        logger.error(f"Failed to load file: {e}")
        sys.exit(1)

    is_valid, message = validate_csv(args.file, df)
    logger.info(f"Validation: {message}")
    if not is_valid:
        logger.error("Invalid input — aborting")
        sys.exit(1)

    result = run_pipeline_from_df(
        df,
        provider=args.provider,
        use_llm=args.llm,
        llm_max=args.llm_max,
        api_key=args.api_key,
        no_forecast=args.no_forecast,
        top_untagged=args.top_untagged,
    )

    primary_leaks = result["leaks"]
    forecasts     = result["forecasts"]

    pipeline_stats = {
        **result["pipeline_stats"],
        "file": args.file,
    }

    print_clean_output(primary_leaks)

    if args.output in {"json", "both"}:
        try:
            json_path = save_json_report(primary_leaks, forecasts, pipeline_stats)
            logger.info(f"JSON report → {json_path}")
        except Exception as e:
            logger.warning(f"JSON report save failed: {e}")

    if args.output in {"markdown", "both"}:
        try:
            md_path = save_markdown_report(primary_leaks, forecasts)
            logger.info(f"Markdown report → {md_path}")
        except Exception as e:
            logger.warning(f"Markdown report save failed: {e}")

    logger.info("Pipeline complete")
    return primary_leaks


# ===================== ENTRYPOINT =====================

if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args)
