#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess

STEPS = [
    "pipelines/equity/download.py",
    "pipelines/equity/clean_daily.py",
    "pipelines/equity/append_master.py",

    "pipelines/futures/download.py",
    "pipelines/futures/clean_daily.py",
    "pipelines/futures/append_master.py",

    "pipelines/options/download.py",
    "pipelines/options/clean_daily.py",
    "pipelines/options/append_master.py",

    "analytics/sanity_master.py",
]

for step in STEPS:
    print(f"\n▶️ Running: {step}")
    subprocess.run(["python", step], check=True)

print("\n🎉 DAILY PIPELINE COMPLETE ✅")
