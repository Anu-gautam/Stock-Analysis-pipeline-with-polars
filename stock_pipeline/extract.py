"""Extract stage: fetch market data and materialize as a Polars LazyFrame."""

from __future__ import annotations

import logging
from datetime import date, timedelta

import polars as pl
import yfinance as yf

logger = logging.getLogger(__name__)


def _flatten_yfinance_columns(df) -> None:
    """Normalize yfinance MultiIndex columns to a single level."""
    import pandas as pd

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)


def extract(ticker: str, start: date, end: date) -> pl.LazyFrame:
    """
    Download daily OHLCV history for *ticker* between *start* and *end*,
    then convert to a Polars LazyFrame for downstream lazy evaluation.
    """
    logger.info(
        "Extracting %s from %s to %s via yfinance",
        ticker,
        start.isoformat(),
        end.isoformat(),
    )

    # yfinance end is exclusive; add one day so March 31 is included.
    end_param = end + timedelta(days=1)

    raw = yf.download(
        ticker,
        start=start.isoformat(),
        end=end_param.isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        group_by="column",
    )

    if raw is None or raw.empty:
        raise ValueError(
            f"No historical data returned for '{ticker}' "
            f"({start.isoformat()} to {end.isoformat()}). "
            "Check the ticker symbol and date range."
        )

    _flatten_yfinance_columns(raw)
    raw = raw.reset_index()

    frame = pl.from_pandas(raw, include_index=False)
    lazy = frame.lazy()

    logger.info("Extract complete: %d row(s) in eager snapshot", frame.height)
    return lazy
