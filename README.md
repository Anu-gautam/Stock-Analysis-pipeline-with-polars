# Stock Analysis Pipeline with Polars

> **High-performance ETL pipeline for equity market data — built with [Polars](https://pola.rs/), the next-generation DataFrame library.**

---

## Table of Contents

1. [Overview](#overview)
2. [Why Polars?](#why-polars)
3. [Pipeline Architecture](#pipeline-architecture)
4. [Project Structure](#project-structure)
5. [Stages](#stages)
   - [Extract](#1-extract)
   - [Inspect](#2-inspect--data-quality-audit)
   - [Transform](#3-transform)
   - [Load](#4-load)
6. [Output Schema](#output-schema)
7. [Quick Start](#quick-start)
8. [Requirements](#requirements)
9. [Sample Run](#sample-run)

---

## Overview

This project implements a production-grade **Extract → Inspect → Transform → Load (EITL)** pipeline that ingests daily OHLCV (Open, High, Low, Close, Volume) equity data for any publicly traded stock, performs rigorous data-quality auditing, engineers key technical indicators, and persists a clean analytical dataset — all within a single, modular Python package.

**Target use-case:** Quantitative research and data preparation for equity analysis workflows, aligned with the data-intensive demands of Investment Banking, Equity Research, and Capital Markets.

---

## Why Polars?

The pipeline is built on **Polars** — a DataFrame library written in Rust — chosen deliberately over Pandas for several reasons that matter in financial data engineering:

| Capability | Polars | Pandas |
|---|---|---|
| **Execution model** | Lazy evaluation with query optimisation | Eager (immediate) execution |
| **Performance** | Multi-threaded, SIMD-vectorised operations | Single-threaded by default |
| **Memory efficiency** | Apache Arrow columnar memory layout | Row-oriented memory model |
| **Type safety** | Strict, immutable schema at compile time | Dynamic, implicit type casting |
| **Null handling** | Explicit `Null` type distinct from `NaN` | Mixed `NaN`/`None` semantics |

### Lazy API in practice

The pipeline constructs the entire transformation graph — renaming, casting, deduplication, forward-filling, and indicator computation — **without executing a single row of data** until `lf.collect()` is called at the load stage. Polars' query optimiser can reorder, prune, and parallelise operations automatically:

```python
lazy_raw  = extract(ticker, start, end)        # LazyFrame — no data loaded yet
lazy_clean = transform(lazy_raw)               # entire graph built in memory
output    = load(lazy_clean, ticker)           # collect() triggers execution once
```

This approach eliminates redundant intermediate materialisation, reduces peak memory usage, and scales to multi-ticker batch processing without code changes.

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Stock Analysis Pipeline                    │
│                                                             │
│  ┌───────────┐   ┌───────────┐   ┌───────────┐  ┌───────┐ │
│  │  Extract  │──▶│  Inspect  │──▶│ Transform │─▶│ Load  │ │
│  │           │   │           │   │           │  │       │ │
│  │ yfinance  │   │ QA Report │   │ Lazy API  │  │  CSV  │ │
│  │     ↓     │   │ nulls     │   │ cast      │  │       │ │
│  │ pl.from_  │   │ dupes     │   │ SMA-5     │  │collect│ │
│  │ pandas()  │   │ outliers  │   │ returns   │  │   ↓   │ │
│  │     ↓     │   │ dtypes    │   │           │  │write_ │ │
│  │LazyFrame  │   │           │   │LazyFrame  │  │  csv  │ │
│  └───────────┘   └───────────┘   └───────────┘  └───────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
Stock-Analysis-pipeline-with-polars/
│
├── main.py                      # CLI entry point — orchestrates the pipeline
│
├── stock_pipeline/              # Core package
│   ├── __init__.py
│   ├── dates.py                 # Financial year window utilities
│   ├── extract.py               # Stage 1 — data ingestion via yfinance
│   ├── inspect.py               # Stage 2 — data quality auditing
│   ├── transform.py             # Stage 3 — cleaning & feature engineering
│   └── load.py                  # Stage 4 — materialise & persist results
│
├── AAPL_lfy_analysis.csv        # Sample output: Apple Inc. (Last Financial Year)
├── requirements.txt
└── README.md
```

---

## Stages

### 1. Extract

**File:** `stock_pipeline/extract.py`

Downloads daily OHLCV data for the requested ticker over the **Last Financial Year** window (April 1 – March 31, aligned with the Indian financial calendar) using `yfinance`. The Pandas DataFrame returned by `yfinance` is immediately converted to a **Polars `LazyFrame`**, deferring all computation to downstream stages.

- Supports any ticker available on Yahoo Finance (e.g. `AAPL`, `TSLA`, `RELIANCE.NS`, `MSFT`)
- MultiIndex columns from `yfinance` are normalised to a flat schema
- Returns a `pl.LazyFrame` — no row-level operations performed at this stage

### 2. Inspect — Data Quality Audit

**File:** `stock_pipeline/inspect.py`

Performs a comprehensive audit before any transformation, producing a structured `QualityReport`:

| Check | Description |
|---|---|
| **Null counts** | Per-column missing value counts flagged via `WARNING` logs |
| **Duplicate rows** | Rows sharing the same `date` + `close` pair |
| **Schema validation** | Verifies expected types for `date`, OHLC, and `volume` columns |
| **IQR outlier detection** | Flags `close` and `volume` values outside 1.5× the interquartile range |
| **Z-score outlier detection** | Flags values with \|z\| > 3.0 standard deviations from the mean |

This stage materialises the frame **once** for statistical computation and logs findings at appropriate severity levels, enabling downstream alerting or pipeline halting.

### 3. Transform

**File:** `stock_pipeline/transform.py`

Builds a Polars **lazy transformation graph** composed of four sequential operations:

| Step | Operation | Polars API used |
|---|---|---|
| **Rename** | Normalise all columns to `snake_case` | `lf.rename()` |
| **Cast & align** | Enforce `Date`, `Float64`, `Int64` types | `pl.col().cast()` with `with_columns()` |
| **Deduplicate** | Remove duplicate dates, keeping the last entry | `lf.unique(subset=["date"], keep="last")` |
| **Missing values** | Forward-fill price/volume gaps (trading halts) | `pl.col().forward_fill()` |
| **Indicators** | 5-day Simple Moving Average and daily return % | `rolling_mean()`, arithmetic expressions |

Engineered features:

- **`sma_5`** — 5-day Simple Moving Average of closing price, a core momentum indicator used widely in technical analysis
- **`daily_return_pct`** — percentage change in closing price day-over-day: `(close_t / close_{t-1} − 1) × 100`

No data leaves a `LazyFrame` at this stage — the entire graph is compiled and handed to the load stage.

### 4. Load

**File:** `stock_pipeline/load.py`

Calls `lf.collect()` — the single point at which Polars executes the optimised query plan across all prior stages — and writes the resulting `DataFrame` to a CSV file named `{TICKER}_lfy_analysis.csv`.

---

## Output Schema

| Column | Type | Description |
|---|---|---|
| `date` | `Date` | Trading date |
| `open` | `Float64` | Opening price |
| `high` | `Float64` | Intraday high |
| `low` | `Float64` | Intraday low |
| `close` | `Float64` | Closing price |
| `adj_close` | `Float64` | Dividend/split-adjusted close |
| `volume` | `Int64` | Shares traded |
| `sma_5` | `Float64` | 5-day Simple Moving Average |
| `daily_return_pct` | `Float64` | Daily return (%) |

**Sample output** (`AAPL_lfy_analysis.csv` — Apple Inc., FY 2025):

```
date,adj_close,close,high,low,open,volume,sma_5,daily_return_pct
2025-04-01,222.02,223.19,223.68,218.90,219.81,36412700,,
2025-04-02,222.71,223.89,225.19,221.02,221.32,35905900,,0.314
2025-04-03,202.12,203.19,207.49,201.25,205.54,103419000,,-9.246
2025-04-04,187.39,188.38,199.88,187.34,193.89,125910900,,-7.289
2025-04-07,180.51,181.46,194.15,174.62,177.20,160466300,204.02,-3.673
```

---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/Anu-gautam/Stock-Analysis-pipeline-with-polars.git
cd Stock-Analysis-pipeline-with-polars
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the pipeline

```bash
python main.py
```

You will be prompted to enter a ticker symbol:

```
Enter stock ticker symbol (e.g. AAPL, TSLA, RELIANCE.NS): AAPL
```

The pipeline will run and produce `AAPL_lfy_analysis.csv` in the current directory.

---

## Requirements

| Package | Version | Purpose |
|---|---|---|
| `polars` | ≥ 1.0.0 | Core DataFrame engine (lazy evaluation, Arrow backend) |
| `yfinance` | ≥ 0.2.40 | Yahoo Finance market data ingestion |
| `pandas` | ≥ 2.0.0 | Intermediate bridge for `yfinance` output |
| `pyarrow` | ≥ 14.0.0 | Apache Arrow IPC / columnar memory layer |

---

## Sample Run

```
2025-05-01 09:00:00 | INFO     | __main__ | Last Financial Year window: 2024-04-01 to 2025-03-31
2025-05-01 09:00:00 | INFO     | stock_pipeline.extract | Extracting AAPL from 2024-04-01 to 2025-03-31 via yfinance
2025-05-01 09:00:02 | INFO     | stock_pipeline.extract | Extract complete: 252 row(s) in eager snapshot
2025-05-01 09:00:02 | INFO     | stock_pipeline.inspect | Running data quality inspection
2025-05-01 09:00:02 | INFO     | stock_pipeline.inspect | --- Data Quality Report ---
2025-05-01 09:00:02 | INFO     | stock_pipeline.inspect | Null values in 'close': 0
2025-05-01 09:00:02 | INFO     | stock_pipeline.inspect | Duplicate rows: 0
2025-05-01 09:00:02 | INFO     | stock_pipeline.inspect | No dtype mismatches detected for expected schema.
2025-05-01 09:00:02 | INFO     | stock_pipeline.inspect | Outliers in 'close' (IQR method): 0
2025-05-01 09:00:02 | INFO     | stock_pipeline.inspect | --- End Quality Report ---
2025-05-01 09:00:02 | INFO     | stock_pipeline.transform | Transforming data (rename, cast, fill, indicators)
2025-05-01 09:00:02 | INFO     | stock_pipeline.transform | Transform graph built (lazy; execution deferred)
2025-05-01 09:00:02 | INFO     | stock_pipeline.load | Collecting lazy graph and writing to AAPL_lfy_analysis.csv
2025-05-01 09:00:02 | INFO     | stock_pipeline.load | Load complete: 252 row(s), 9 column(s)
2025-05-01 09:00:02 | INFO     | __main__ | Pipeline finished successfully. Output: AAPL_lfy_analysis.csv
```
