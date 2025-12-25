# =====================================================
# NIFTY-LAB | DAILY RUN PIPELINE (PS 5.1 SAFE)
# =====================================================

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------------- ROOT ----------------
$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ROOT

# ---------------- TIME ----------------
$DATE = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$DATE_TAG = Get-Date -Format "yyyy-MM-dd"

# ---------------- PYTHON ----------------
$PYTHON = "C:\Users\Harshal\anaconda3\envs\TradeSense\python.exe"
if (!(Test-Path $PYTHON)) {
    Write-Host "ERROR: Python not found at $PYTHON"
    exit 1
}

# ---------------- LOG ----------------
$LOG_DIR = Join-Path $ROOT "logs"
if (!(Test-Path $LOG_DIR)) {
    New-Item -ItemType Directory -Path $LOG_DIR | Out-Null
}

$LOG_FILE = Join-Path $LOG_DIR "daily_run_$DATE_TAG.log"

# ---------------- HEADER ----------------
@"
=====================================
NIFTY-LAB DAILY RUN STARTED
Date      : $DATE
Project   : $ROOT
Python    : $PYTHON
Log file  : $LOG_FILE
=====================================
"@ | Tee-Object -FilePath $LOG_FILE

# ---------------- HELPERS ----------------
function Run-Step($name, $module) {
    Write-Host ""
    Write-Host "STEP: $name"
    & $PYTHON -m $module 2>&1 | Tee-Object -FilePath $LOG_FILE -Append
    Write-Host "DONE: $name"
}

function Run-Script($name, $script) {
    Write-Host ""
    Write-Host "STEP: $name"
    & $PYTHON $script 2>&1 | Tee-Object -FilePath $LOG_FILE -Append
    Write-Host "DONE: $name"
}

# =====================================================
# PHASE 1 - DOWNLOAD
# =====================================================
Run-Step "Download Equity"   "pipelines.equity.daily_download_equ_auto"
Run-Step "Download Futures"  "pipelines.futures.daily_download_fut_auto"
Run-Step "Download Options"  "pipelines.options.daily_download_opt_auto"

# =====================================================
# PHASE 2 - CLEAN
# =====================================================
Run-Step "Clean Equity"   "pipelines.equity.clean_daily_equ"
Run-Step "Clean Futures"  "pipelines.futures.clean_daily_fut"
Run-Step "Clean Options"  "pipelines.options.clean_daily_opt"

# =====================================================
# PHASE 3 - APPEND TO MASTER
# =====================================================
Run-Step "Append Equity Master"   "pipelines.equity.append_master_equ"
Run-Step "Append Futures Master"  "pipelines.futures.append_master_futures"
Run-Step "Append Options Master"  "pipelines.options.append_master_options"

# =====================================================
# PHASE 4 - SANITY
# =====================================================
Run-Step "Global Sanity Check" "pipelines.sanity_global_alignment"

# =====================================================
# PHASE 5 - DERIVED DATA
# =====================================================
Run-Script "Build Futures OI" "analytics/build_futures_oi_daily.py"
Run-Step   "Build PCR"        "pipelines.options.build_daily_pcr"

# =====================================================
# PHASE 6 - MARKET STRUCTURE
# =====================================================
Run-Step "Build Continuous NIFTY" "pipelines.historical.build_nifty_continuous"
Run-Step "Build Regime"           "pipelines.regime.build_nifty_regime_features"

# =====================================================
# PHASE 7 - ML INFERENCE
# =====================================================
Run-Step "Build Inference Features" "pipelines.ml.build_nifty_inference_features"
Run-Step "Predict ML Ensemble"      "pipelines.ml.predict_nifty_ensemble_calibrated"

# =====================================================
# PHASE 8 - FUTURES STRATEGY
# =====================================================
Run-Script "Final Futures Strategy" "strategies/final/nifty_ml_oi_pcr_final_strategy.py"

# =====================================================
# PHASE 9 - OPTIONS ENGINE
# =====================================================
Run-Script "Build Option Chain" "strategies/options/build_nifty_option_chain.py"
Run-Script "Options Execution"  "strategies/options/options_execution_engine.py"

# =====================================================
# END
# =====================================================
@"
-------------------------------------
PIPELINE COMPLETED SUCCESSFULLY
-------------------------------------
"@ | Tee-Object -FilePath $LOG_FILE -Append

Write-Host ""
Write-Host "ALL DONE - NO ERRORS"
