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

# -------- PRE-CHECKS --------
if (!(Test-Path $PYTHON)) {
    throw "Python executable not found: $PYTHON"
}

if (!(Test-Path $LOG_DIR)) {
    New-Item -ItemType Directory -Path $LOG_DIR | Out-Null
}

Set-Location $PROJECT_DIR

# Clear log header for clean run
"`n===== DAILY RUN STARTED ($DATE) =====`n" |
    Out-File -FilePath $LOG_FILE -Encoding UTF8

# -------- HELPERS --------
function Run-Hard($script) {
    $cmd = "$PYTHON $script"
    Write-Host "RUNNING (HARD): $cmd"
    Add-Content -Encoding UTF8 $LOG_FILE "RUNNING (HARD): $cmd"

    cmd /c "$cmd >> `"$LOG_FILE`" 2>&1"
    if ($LASTEXITCODE -ne 0) {
        Add-Content -Encoding UTF8 $LOG_FILE "FAILED (HARD): $script"
        throw "PIPELINE STOPPED AT: $script"
    }
}

function Run-Soft($script) {
    $cmd = "$PYTHON $script"
    Write-Host "RUNNING (SOFT): $cmd"
    Add-Content -Encoding UTF8 $LOG_FILE "RUNNING (SOFT): $cmd"

    cmd /c "$cmd >> `"$LOG_FILE`" 2>&1"
}

# =================================================
# STAGE 1 — DOWNLOAD (ALL • SOFT)
# =================================================
Run-Soft "pipelines/equity/daily_download_equ_auto.py"
Run-Soft "pipelines/futures/daily_download_fut_auto.py"
Run-Soft "pipelines/options/daily_download_opt_auto.py"

# =================================================
# STAGE 2 — CLEAN DAILY (ALL • SOFT)
# =================================================
Run-Soft "pipelines/equity/clean_daily_equ.py"
Run-Soft "pipelines/futures/clean_daily_fut.py"
Run-Soft "pipelines/options/clean_daily_opt.py"

# =================================================
# STAGE 3 — APPEND MASTER (ALL • HARD)
# =================================================
Run-Hard "pipelines/equity/append_master_equ.py"
Run-Hard "pipelines/futures/append_master_futures.py"
Run-Hard "pipelines/options/append_master_options.py"

# =================================================
# STAGE 4 — SANITY CHECKS (ALL • SOFT)
# =================================================
Run-Soft "pipelines/equity/sanity_daily_equity.py"
Run-Soft "pipelines/futures/sanity_daily_futures.py"
Run-Soft "pipelines/options/sanity_daily_opt.py"

# =================================================
# STAGE 4.5 — GLOBAL ALIGNMENT (SOFT)
# =================================================
Run-Soft "pipelines/sanity_global_alignment.py"

# =================================================
# STAGE 5 — ANALYTICS (SOFT)
# =================================================
Run-Soft "analytics/build_futures_oi_daily.py"
Run-Soft "pipelines/options/build_daily_pcr.py"

# =================================================
# STAGE 6 — ML INFERENCE (SOFT)
# =================================================
Run-Soft "pipelines/ml/build_nifty_inference_features.py"
Run-Soft "pipelines/ml/predict_nifty_xgb.py"

# =================================================
# STAGE 7 — FINAL STRATEGY (SOFT)
# =================================================
Run-Soft "strategies/final/nifty_ml_oi_pcr_final_strategy.py"

Add-Content -Encoding UTF8 $LOG_FILE "===== DAILY RUN COMPLETED SUCCESSFULLY ====="
Write-Host "DAILY RUN COMPLETED SUCCESSFULLY"
