# NIFTY-LAB

NIFTY-LAB is a structured NSE (India) data engineering and research lab for
Equity, Futures, and Options markets.

## Project Structure

- `configs/` – Paths, schemas, symbols
- `data/`
  - `raw/` – Original NSE data
  - `processed/` – Cleaned daily data
  - `continuous/` – Master datasets
- `pipelines/` – Download, clean, validate, append
- `analytics/` – Sanity checks & research
- `strategies/` – Indicators, backtests, alpha research

## Workflow

Raw → Processed → Master → Analytics → Strategies

## Status
Active development 🚀
