#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path

# Base dir = project root (NIFTY-LAB)
BASE_DIR = Path(__file__).resolve().parents[1]

# ----- RAW -----
RAW_DIR         = BASE_DIR / "data" / "raw"
RAW_HIST_DIR    = RAW_DIR / "historical"
RAW_EQUITY_DIR  = RAW_DIR / "equity"
RAW_FUTURES_DIR = RAW_DIR / "futures"
RAW_OPTIONS_DIR = RAW_DIR / "options"

# ----- PROCESSED -----
PROC_DIR          = BASE_DIR / "data" / "processed"
PROC_DAILY_DIR    = PROC_DIR / "daily"
PROC_EQ_DAILY     = PROC_DAILY_DIR / "equity"
PROC_FUT_DAILY    = PROC_DAILY_DIR / "futures"
PROC_OPT_DAILY    = PROC_DAILY_DIR / "options"

PROC_MASTER_DIR   = PROC_DIR / "master"
MASTER_EQUITY     = PROC_MASTER_DIR / "master_equity.parquet"
MASTER_FUTURES    = PROC_MASTER_DIR / "master_futures.parquet"
MASTER_OPTIONS    = PROC_MASTER_DIR / "master_options.parquet"

# ----- CONTINUOUS -----
CONT_DIR          = BASE_DIR / "data" / "continuous"
NIFTY_CONTINUOUS  = CONT_DIR / "nifty_continuous.parquet"
BANKNIFTY_CONT    = CONT_DIR / "banknifty_continuous.parquet"

# ----- OPTIONS per index / expiry -----
OPTIONS_PER_INDEX_DIR = PROC_DIR / "options" / "per_index"  # optional extra root

# ensure important dirs exist (safe)
for p in [
    RAW_EQUITY_DIR, RAW_FUTURES_DIR, RAW_OPTIONS_DIR,
    PROC_EQ_DAILY, PROC_FUT_DAILY, PROC_OPT_DAILY,
    PROC_MASTER_DIR, CONT_DIR
]:
    p.mkdir(parents=True, exist_ok=True)
