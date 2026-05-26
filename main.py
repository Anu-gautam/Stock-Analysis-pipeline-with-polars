"""
Stock ETL pipeline entry point.

Extract -> Inspect -> Transform -> Load using Polars Lazy API.
"""

from __future__ import annotations

import logging
import sys

from stock_pipeline.dates import get_last_financial_year_range
from stock_pipeline.extract import extract
from stock_pipeline.inspect import inspect_data_quality
from stock_pipeline.load import load
from stock_pipeline.transform import transform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def prompt_ticker() -> str:
    ticker = input("Enter stock ticker symbol (e.g. AAPL, TSLA, RELIANCE.NS): ").strip()
    if not ticker:
        raise ValueError("Ticker symbol cannot be empty.")
    return ticker.upper()


def run_pipeline(ticker: str) -> None:
    start, end = get_last_financial_year_range()
    logger.info(
        "Last Financial Year window: %s to %s",
        start.isoformat(),
        end.isoformat(),
    )

    lazy_raw = extract(ticker, start, end)
    inspect_data_quality(lazy_raw)

    lazy_clean = transform(lazy_raw)
    output_file = load(lazy_clean, ticker)

    logger.info("Pipeline finished successfully. Output: %s", output_file.resolve())


if __name__ == "__main__":
    try:
        symbol = prompt_ticker()
        run_pipeline(symbol)
    except KeyboardInterrupt:
        logger.info("Pipeline cancelled by user.")
        sys.exit(130)
    except Exception as exc:
        logger.error("Pipeline failed: %s", exc)
        sys.exit(1)
