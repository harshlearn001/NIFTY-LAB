#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-13.1 | NIFTY OPTIONS EXPIRY CALENDAR

✔ Weekly expiry: Tuesday
✔ Monthly expiry: Last Tuesday of month
✔ Holiday-safe (moves backward)
✔ Live + Backtest compatible
"""

from datetime import date, timedelta
import pandas as pd

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
WEEKLY_EXPIRY_WEEKDAY = 1  # Tuesday (Mon=0)
MONTHLY_EXPIRY_WEEKDAY = 1  # Tuesday

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def is_trading_day(d: date, holidays: set) -> bool:
    return d.weekday() < 5 and d not in holidays


def previous_trading_day(d: date, holidays: set) -> date:
    while not is_trading_day(d, holidays):
        d -= timedelta(days=1)
    return d


# --------------------------------------------------
# WEEKLY EXPIRY
# --------------------------------------------------
def get_weekly_expiry(trade_date: date, holidays: set = set()) -> date:
    """
    Returns weekly expiry date (Tuesday, holiday-adjusted)
    """
    days_ahead = (WEEKLY_EXPIRY_WEEKDAY - trade_date.weekday()) % 7
    expiry = trade_date + timedelta(days=days_ahead)

    return previous_trading_day(expiry, holidays)


# --------------------------------------------------
# MONTHLY EXPIRY
# --------------------------------------------------
def get_monthly_expiry(trade_date: date, holidays: set = set()) -> date:
    """
    Returns last Tuesday of the month (holiday-adjusted)
    """
    # move to last day of month
    next_month = trade_date.replace(day=28) + timedelta(days=4)
    last_day = next_month - timedelta(days=next_month.day)

    # move backward to Tuesday
    d = last_day
    while d.weekday() != MONTHLY_EXPIRY_WEEKDAY:
        d -= timedelta(days=1)

    return previous_trading_day(d, holidays)


# --------------------------------------------------
# EXPIRY CLASSIFIER
# --------------------------------------------------
def classify_expiry(trade_date: date, holidays: set = set()):
    """
    Returns:
        expiry_date,
        expiry_type -> WEEKLY | MONTHLY
    """
    weekly = get_weekly_expiry(trade_date, holidays)
    monthly = get_monthly_expiry(trade_date, holidays)

    if weekly == monthly:
        return monthly, "MONTHLY"
    else:
        return weekly, "WEEKLY"


# --------------------------------------------------
# SELF TEST
# --------------------------------------------------
if __name__ == "__main__":
    # Example NSE holidays (extend later)
    HOLIDAYS = {
        date(2025, 1, 26),   # Republic Day
        date(2025, 3, 29),   # Holi
    }

    test_dates = [
        date(2025, 1, 20),
        date(2025, 1, 27),
        date(2025, 2, 24),
        date(2025, 3, 24),
    ]

    for d in test_dates:
        exp, typ = classify_expiry(d, HOLIDAYS)
        print(f"{d} → {typ} expiry → {exp}")
