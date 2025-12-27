from datetime import date, timedelta

def select_option(action, confidence, spot_price):
    if action == "HOLD" or confidence < 0.60:
        return None

    opt_type = "CE" if action == "LONG" else "PE"

    step = 50  # NIFTY strike step
    atm = round(spot_price / step) * step

    if confidence > 0.75:
        strike = atm - step if opt_type == "CE" else atm + step
    else:
        strike = atm

    return {
        "option_type": opt_type,
        "strike": int(strike),
        "expiry": next_weekly_expiry()
    }

def next_weekly_expiry():
    d = date.today()
    while d.weekday() != 3:  # Thursday
        d += timedelta(days=1)
    return d
