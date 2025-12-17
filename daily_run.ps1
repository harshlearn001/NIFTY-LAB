# =====================================
# NIFTY-LAB DAILY AUTO RUN (FINAL • PROD)
# =====================================

$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# -------- CONFIG --------
$PROJECT_DIR = "H:\NIFTY-LAB"
$PYTHON      = "C:\Users\Harshal\anaconda3\envs\TradeSense\python.exe"
$LOG_DIR     = "$PROJECT_DIR\logs"
$DATE        = Get-Date -Format "yyyy-MM-dd"
$LOG_FILE    = "$LOG_DIR\daily_run_$DATE.log"

if (!(Test-Path $LOG_DIR)) {
    New-Item -ItemType Directory -Path $LOG_DIR | Out-Null
}

Set-Location $PROJECT_DIR

# -------- HELPERS --------
function Run-Step($script) {
    $cmd = "$PYTHON $script"
    Write-Host "RUNNING: $cmd"
    Add-Content -Encoding UTF8 $LOG_FILE "`nRUNNING: $cmd"

    cmd /c "$cmd >> `"$LOG_FILE`" 2>&1"
    if ($LASTEXITCODE -ne 0) {
        Add-Content -Encoding UTF8 $LOG_FILE "FAILED: $script"
        throw "Pipeline stopped at: $script"
    }
}

function Run-Step-Soft($script) {
    $cmd = "$PYTHON $script"
    Write-Host "OPTIONAL: $cmd"
    Add-Content -Encoding UTF8 $LOG_FILE "`nOPTIONAL: $cmd"

    cmd /c "$cmd >> `"$LOG_FILE`" 2>&1"
}

# =================================================
# DATA DOWNLOAD (SOFT — NSE DELAYS ARE NORMAL)
# =================================================
Run-Step-Soft "pipelines/equity/daily_download_equ.py"
Run-Step-Soft "pipelines/futures/daily_download_fut.py"
Run-Step-Soft "pipelines/options/daily_download_opt.py"


# =================================================
# =================================================
# =================================================
# =================================================
# CLEAN (SAFE)
# =================================================
Run-Step-Soft "pipelines/equity/clean_daily_equ.py"
Run-Step-Soft "pipelines/futures/clean_daily_fut.py"
Run-Step-Soft "pipelines/options/clean_daily_opt.py"


# =================================================
# =================================================
# SANITY (NEVER FAIL PIPELINE)
# =================================================
Run-Step-Soft "pipelines/equity/sanity_daily_equity.py"
Run-Step-Soft "pipelines/futures/sanity_daily_futures.py"
Run-Step-Soft "pipelines/options/sanity_daily_opt.py"


# =================================================
# MASTER BUILD (HARD)
# =================================================
Run-Step "pipelines/equity/append_master_equ.py"
Run-Step "pipelines/futures/append_master.py"
Run-Step "pipelines/options/append_master_options.py"

# =================================================
# ANALYTICS (SOFT)
# =================================================
Run-Step-Soft "analytics/futures_oi_analysis.py"
Run-Step-Soft "pipelines/options/build_daily_pcr.py"

# =================================================
# ML INFERENCE (SOFT)
# =================================================
Run-Step-Soft "pipelines/ml/build_nifty_inference_features.py"
Run-Step-Soft "pipelines/ml/predict_nifty_xgb.py"

# =================================================
# FINAL STRATEGY (SOFT)
# =================================================
Run-Step-Soft "strategies/final/nifty_ml_oi_pcr_final_strategy.py"

Write-Host "DAILY RUN COMPLETED SUCCESSFULLY"
Add-Content -Encoding UTF8 $LOG_FILE "DAILY RUN COMPLETED SUCCESSFULLY"
