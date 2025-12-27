#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | Central Path Registry

✔ Single source of truth for paths
✔ Works for daily + backtest + ML
✔ Safe for imports from any submodule
✔ Windows / Linux compatible
"""

from pathlib import Path

# ==================================================
# PROJECT ROOT
# ==================================================
# configs/paths.py → configs → project root
BASE_DIR = Path(__file__).resolve().parents[1]

# ==================================================
# DATA ROOT
# ==================================================
DATA_DIR = BASE_DIR / "data"

# ==================================================
# RAW DATA
# ==================================================
RAW_DIR         = DATA_DIR / "raw"
RAW_HIST_DIR    = RAW_DIR / "historical"
RAW_EQUITY_DIR  = RAW_DIR / "equity"
RAW_FUTURES_DIR = RAW_DIR / "futures"
RAW_OPTIONS_DIR = RAW_DIR / "options"

# ==================================================
# PROCESSED DATA
# ==================================================
PROC_DIR          = DATA_DIR / "processed"

PROC_DAILY_DIR    = PROC_DIR / "daily"
PROC_EQ_DAILY     = PROC_DAILY_DIR / "equity"
PROC_FUT_DAILY    = PROC_DAILY_DIR / "futures"
PROC_OPT_DAILY    = PROC_DAILY_DIR / "options"

PROC_MASTER_DIR   = PROC_DIR / "master"
MASTER_EQUITY     = PROC_MASTER_DIR / "master_equity.parquet"
MASTER_FUTURES    = PROC_MASTER_DIR / "master_futures.parquet"
MASTER_OPTIONS    = PROC_MASTER_DIR / "master_options.parquet"

# ==================================================
# CONTINUOUS SERIES
# ==================================================
CONT_DIR          = DATA_DIR / "continuous"
NIFTY_CONTINUOUS  = CONT_DIR / "nifty_continuous.parquet"
BANKNIFTY_CONT    = CONT_DIR / "banknifty_continuous.parquet"

# ==================================================
# ML / FUTURES / OPTIONS OUTPUTS
# ==================================================
FUTURES_ML_DIR    = PROC_DIR / "futures_ml"
OPTIONS_ML_DIR    = PROC_DIR / "options_ml"
ML_DIR            = PROC_DIR / "ml"

# ==================================================
# BACKTEST / ANALYSIS / SIGNALS
# ==================================================
BACKTEST_DIR  = DATA_DIR / "backtest"
ANALYSIS_DIR  = DATA_DIR / "analysis"
SIGNAL_DIR    = DATA_DIR / "signals"
AUDIT_DIR     = DATA_DIR / "audit"

# ==================================================
# 🔥 MODELS (FIX FOR YOUR ERROR)
# ==================================================
MODEL_DIR = BASE_DIR / "models"

# ==================================================
# OPTIONAL OPTIONS STRUCTURE
# ==================================================
OPTIONS_PER_INDEX_DIR = PROC_DIR / "options" / "per_index"
OPTIONS_CHAIN_DIR     = PROC_DIR / "options_chain"

# ==================================================
# ENSURE REQUIRED DIRECTORIES EXIST (SAFE)
# ==================================================
for p in [
    RAW_EQUITY_DIR,
    RAW_FUTURES_DIR,
    RAW_OPTIONS_DIR,
    PROC_EQ_DAILY,
    PROC_FUT_DAILY,
    PROC_OPT_DAILY,
    PROC_MASTER_DIR,
    CONT_DIR,
    FUTURES_ML_DIR,
    OPTIONS_ML_DIR,
    ML_DIR,
    BACKTEST_DIR,
    ANALYSIS_DIR,
    SIGNAL_DIR,
    AUDIT_DIR,
    MODEL_DIR,
]:
    p.mkdir(parents=True, exist_ok=True)
