"""Inspect stage: structural and statistical data-quality auditing."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import polars as pl

logger = logging.getLogger(__name__)

PRICE_VOLUME_COLS = ("close", "volume")
OUTLIER_METHODS = ("iqr", "zscore")


@dataclass
class QualityReport:
    """Summary of data-quality findings."""

    null_counts: dict[str, int] = field(default_factory=dict)
    duplicate_rows: int = 0
    dtype_issues: list[str] = field(default_factory=list)
    outliers: dict[str, dict[str, int]] = field(default_factory=dict)

    def log_summary(self) -> None:
        logger.info("--- Data Quality Report ---")
        if self.null_counts:
            for col, count in self.null_counts.items():
                if count:
                    logger.warning("Null values in '%s': %d", col, count)
                else:
                    logger.info("Null values in '%s': 0", col)
        else:
            logger.info("No null-value scan performed (empty frame).")

        logger.info("Duplicate rows: %d", self.duplicate_rows)

        if self.dtype_issues:
            for issue in self.dtype_issues:
                logger.warning("Dtype issue: %s", issue)
        else:
            logger.info("No dtype mismatches detected for expected schema.")

        for col, methods in self.outliers.items():
            for method, count in methods.items():
                if count:
                    logger.warning(
                        "Outliers in '%s' (%s method): %d row(s)",
                        col,
                        method.upper(),
                        count,
                    )
                else:
                    logger.info(
                        "Outliers in '%s' (%s method): 0",
                        col,
                        method.upper(),
                    )
        logger.info("--- End Quality Report ---")


def _normalize_column_names(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Map columns to snake_case for consistent auditing (mirrors transform)."""
    rename_map = {
        col: col.strip().lower().replace(" ", "_")
        for col in lf.collect_schema().names()
    }
    return lf.rename(rename_map)


def _expected_schema_notes(schema: pl.Schema) -> list[str]:
    issues: list[str] = []
    names = set(schema.names())

    if "date" not in names:
        issues.append("Missing required column 'date'.")
    elif schema["date"] not in (pl.Date, pl.Datetime):
        issues.append(
            f"Column 'date' has type {schema['date']}; expected Date or Datetime."
        )

    for price_col in ("open", "high", "low", "close"):
        if price_col in names and schema[price_col] not in (
            pl.Float32,
            pl.Float64,
            pl.Int32,
            pl.Int64,
        ):
            issues.append(
                f"Column '{price_col}' has type {schema[price_col]}; "
                "expected a numeric float type."
            )

    if "volume" in names and schema["volume"] not in (
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt32,
        pl.UInt64,
        pl.Float32,
        pl.Float64,
    ):
        issues.append(
            f"Column 'volume' has type {schema['volume']}; expected integer or numeric."
        )

    return issues


def _count_iqr_outliers(series: pl.Series, factor: float = 1.5) -> int:
    if series.len() == 0 or series.null_count() == series.len():
        return 0
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - factor * iqr
    upper = q3 + factor * iqr
    return int(((series < lower) | (series > upper)).sum())


def _count_zscore_outliers(series: pl.Series, threshold: float = 3.0) -> int:
    if series.len() == 0 or series.null_count() == series.len():
        return 0
    mean = series.mean()
    std = series.std()
    if std is None or std == 0:
        return 0
    z = (series - mean) / std
    return int((z.abs() > threshold).sum())


def inspect_data_quality(lf: pl.LazyFrame) -> QualityReport:
    """
    Audit *lf* for missing values, duplicates, dtype mismatches, and outliers
    in close/volume. Materializes once for statistical checks.
    """
    logger.info("Running data quality inspection")

    lf_named = _normalize_column_names(lf)
    df = lf_named.collect()
    report = QualityReport()

    if df.is_empty():
        logger.warning("Dataset is empty; limited quality checks applied.")
        report.dtype_issues = _expected_schema_notes(lf_named.collect_schema())
        report.log_summary()
        return report

    report.null_counts = {
        col: int(df[col].null_count()) for col in df.columns
    }

    key_cols = [c for c in ("date", "close") if c in df.columns]
    if key_cols:
        report.duplicate_rows = int(
            df.height - df.select(key_cols).unique().height
        )
    else:
        report.duplicate_rows = int(df.height - df.unique().height)

    report.dtype_issues = _expected_schema_notes(df.schema)

    for col in PRICE_VOLUME_COLS:
        if col not in df.columns:
            report.dtype_issues.append(f"Missing column '{col}' for outlier scan.")
            continue
        series = df[col].cast(pl.Float64, strict=False)
        report.outliers[col] = {
            "iqr": _count_iqr_outliers(series),
            "zscore": _count_zscore_outliers(series),
        }

    report.log_summary()
    return report
