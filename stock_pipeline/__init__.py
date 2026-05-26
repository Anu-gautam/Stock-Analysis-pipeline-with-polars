"""Modular Polars-based ETL pipeline for stock analysis."""

from stock_pipeline.dates import get_last_financial_year_range
from stock_pipeline.extract import extract
from stock_pipeline.inspect import inspect_data_quality
from stock_pipeline.load import load
from stock_pipeline.transform import transform

__all__ = [
    "get_last_financial_year_range",
    "extract",
    "inspect_data_quality",
    "transform",
    "load",
]
