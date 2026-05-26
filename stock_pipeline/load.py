"""Load stage: execute the lazy graph and persist results."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def _safe_filename(ticker: str) -> str:
    return ticker.strip().upper().replace(".", "_")


def load(lf: pl.LazyFrame, ticker: str, output_dir: Path | str = ".") -> Path:
    """
    Collect the lazy pipeline result and write ``{ticker}_lfy_analysis.csv``.
    """
    output_path = Path(output_dir) / f"{_safe_filename(ticker)}_lfy_analysis.csv"

    logger.info("Collecting lazy graph and writing to %s", output_path)
    df = lf.collect()

    df.write_csv(output_path)
    logger.info("Load complete: %d row(s), %d column(s)", df.height, df.width)

    return output_path
