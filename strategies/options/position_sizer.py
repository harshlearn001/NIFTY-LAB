import math

LOT_SIZE = 50

def size_position(capital, option_price):
    max_risk = min(capital * 0.01, 10_000)

    qty = math.floor(max_risk / (option_price * LOT_SIZE))
    return max(qty, 0)
