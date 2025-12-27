#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | SINGLE ENTRYPOINT

Usage:
  python run.py --mode backtest
  python run.py --mode daily
"""

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def run(script):
    """Run a python script safely"""
    cmd = [sys.executable, str(script)]
    print(f"\n▶ RUNNING: {script}")
    subprocess.check_call(cmd)

def run_backtest():
    print("\n🚀 BACKTEST MODE STARTED")

    run(ROOT / "pipelines" / "futures" / "build_nifty_fut_oi_historical.py")
    run(ROOT / "pipelines" / "ml" / "build_nifty_ml_features_hist_no_pcr.py")
    run(ROOT / "pipelines" / "ml" / "predict_nifty_ensemble_historical.py")
    run(ROOT / "pipelines" / "backtest" / "batch_options_backtest.py")
    run(ROOT / "pipelines" / "backtest" / "equity_curve_analyzer.py")

    print("\n✅ BACKTEST MODE COMPLETE")

def run_daily():
    print("\n⚡ DAILY MODE STARTED")

    run(ROOT / "pipelines" / "equity" / "daily_download_equ_auto.py")
    run(ROOT / "pipelines" / "futures" / "daily_download_fut_auto.py")
    run(ROOT / "pipelines" / "options" / "daily_download_opt_auto.py")

    run(ROOT / "pipelines" / "equity" / "clean_daily_equ.py")
    run(ROOT / "pipelines" / "futures" / "clean_daily_fut.py")
    run(ROOT / "pipelines" / "options" / "clean_daily_opt.py")

    run(ROOT / "pipelines" / "equity" / "append_master_equ.py")
    run(ROOT / "pipelines" / "futures" / "append_master_futures.py")
    run(ROOT / "pipelines" / "options" / "append_master_options.py")

    run(ROOT / "pipelines" / "ml" / "build_nifty_inference_features.py")
    run(ROOT / "pipelines" / "ml" / "predict_nifty_ensemble.py")

    run(ROOT / "strategies" / "options" / "options_execution_engine.py")

    print("\n✅ DAILY MODE COMPLETE")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        required=True,
        choices=["backtest", "daily"],
        help="Run mode"
    )
    args = parser.parse_args()

    if args.mode == "backtest":
        run_backtest()
    elif args.mode == "daily":
        run_daily()

if __name__ == "__main__":
    main()
