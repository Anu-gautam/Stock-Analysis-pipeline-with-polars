"""Transform stage: clean, type-align, and enrich with technical indicators."""

from __future__ import annotations

import logging

import polars as pl

logger = logging.getLogger(__name__)

SMA_WINDOW = 5


def _rename_to_snake_case(lf: pl.LazyFrame) -> pl.LazyFrame:
    rename_map = {
        col: col.strip().lower().replace(" ", "_")
        for col in lf.collect_schema().names()
    }
    return lf.rename(rename_map)


def _cast_and_align(lf: pl.LazyFrame) -> pl.LazyFrame:
    schema = lf.collect_schema()
    exprs: list[pl.Expr] = []

    if "date" in schema.names():
        dtype = schema["date"]
        if dtype == pl.Datetime:
            exprs.append(pl.col("date").cast(pl.Date).alias("date"))
        elif dtype == pl.Utf8:
            exprs.append(pl.col("date").str.to_date().alias("date"))
        elif dtype != pl.Date:
            exprs.append(pl.col("date").cast(pl.Date).alias("date"))

    for col in ("open", "high", "low", "close"):
        if col in schema.names():
            exprs.append(pl.col(col).cast(pl.Float64).alias(col))

    if "volume" in schema.names():
        exprs.append(pl.col("volume").cast(pl.Int64).alias("volume"))

    if not exprs:
        return lf

    # Apply casts; keep unmentioned columns as-is.
    casted = lf.with_columns(exprs)
    return casted


def _handle_missing(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Forward-fill price columns (trading halts / sparse rows), then drop rows
    still missing critical fields.
    """
    schema = lf.collect_schema()
    price_cols = [c for c in ("open", "high", "low", "close") if c in schema.names()]

    if price_cols:
        lf = lf.with_columns(
            [pl.col(c).forward_fill() for c in price_cols]
        )

    if "volume" in schema.names():
        lf = lf.with_columns(pl.col("volume").forward_fill())

    required = [c for c in ("date", "close") if c in schema.names()]
    if required:
        lf = lf.drop_nulls(subset=required)

    return lf


def _add_indicators(lf: pl.LazyFrame) -> pl.LazyFrame:
    if "close" not in lf.collect_schema().names():
        logger.warning("Column 'close' missing; skipping technical indicators.")
        return lf

    return lf.sort("date").with_columns(
        pl.col("close")
        .rolling_mean(window_size=SMA_WINDOW)
        .alias("sma_5"),
        (
            (pl.col("close") / pl.col("close").shift(1) - 1) * 100
        ).alias("daily_return_pct"),
    )


def transform(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Return a cleaned, typed, enriched LazyFrame ready for collection."""
    logger.info("Transforming data (rename, cast, fill, indicators)")

    lf = _rename_to_snake_case(lf)
    lf = _cast_and_align(lf)
    if "date" in lf.collect_schema().names():
        lf = lf.unique(subset=["date"], keep="last")
    lf = _handle_missing(lf)
    lf = _add_indicators(lf)

    logger.info("Transform graph built (lazy; execution deferred)")
    return lf
