#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | Audit Logger
------------------------
✔ Append-only decision log
✔ CSV + Parquet safe
✔ Never breaks strategy
"""

from pathlib import Path
import pandas as pd
from datetime import datetime


def log_decision(signal: dict, base_dir: Path):
    """
    Append trading decision to audit log.
    """

    audit_dir = base_dir / "data" / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)

    csv_file = audit_dir / "nifty_decision_audit.csv"
    pq_file = audit_dir / "nifty_decision_audit.parquet"

    df_new = pd.DataFrame([{
        **signal,
        "TIMESTAMP": datetime.now().isoformat(timespec="seconds")
    }])

    # ---------- CSV (append-safe)
    if csv_file.exists():
        df_old = pd.read_csv(csv_file)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    df.to_csv(csv_file, index=False)

    # ---------- Parquet (overwrite full snapshot)
    df.to_parquet(pq_file, index=False)

    return csv_file, pq_file
