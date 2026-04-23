"""
BreezeAPI Options Chain Fetcher
================================
Logic:
  1. For a given stock + date, fetch the 9:20 AM 1-min candle to get ATM strike (close price)
  2. Round to nearest strike interval → ATM
  3. Build 5 strikes: OTM2, OTM1, ATM, ITM1, ITM2  (for both CE and PE)
  4. Fetch 1-min OHLC for 9:20 AM candle for each strike
  5. Save to CSV: ticker | expiry | option_type | moneyness | strike | open | high | low | close | volume | timestamp

Usage:
  pip install breeze-connect pandas
  Set your API credentials below, then run:
  python breeze_options_fetcher.py
"""

import pandas as pd
from datetime import datetime, date
from breeze_connect import BreezeConnect

# ─────────────────────────────────────────────
#  CONFIGURATION — fill these in
# ─────────────────────────────────────────────

API_KEY        = "YOUR_API_KEY"
API_SECRET     = "YOUR_API_SECRET"
SESSION_TOKEN  = "YOUR_SESSION_TOKEN"   # generate fresh each day from ICICI portal

# Stocks to process
# Format: { "ticker": (strike_interval, "NSE_symbol_for_underlying") }
STOCKS = {
    "ACC":     (10,  "ACC"),
    "RELIANCE": (20, "RELIANCE"),
    # Add more stocks here
}

# Expiry date (format: YYYY-MM-DD)  ← change to the contract expiry you want
EXPIRY_DATE = "2025-05-29"

# Date for which you want the 9:20 AM candle (format: YYYY-MM-DD)
TARGET_DATE = "2025-05-15"

# How many OTM/ITM steps on each side (default 2 → gives OTM2,OTM1,ATM,ITM1,ITM2)
STEPS = 2

# Output CSV path
OUTPUT_CSV = "options_chain_9_20.csv"

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def connect_breeze():
    breeze = BreezeConnect(api_key=API_KEY)
    breeze.generate_session(api_secret=API_SECRET, session_token=SESSION_TOKEN)
    return breeze


def get_920_candle(breeze, stock_code, date_str):
    """
    Fetch the 9:20 AM 1-minute candle for the underlying equity.
    Returns the close price (used as ATM reference).
    """
    from_dt = f"{date_str}T09:20:00.000Z"
    to_dt   = f"{date_str}T09:21:00.000Z"

    resp = breeze.get_historical_data_v2(
        interval        = "1minute",
        from_date       = from_dt,
        to_date         = to_dt,
        stock_code      = stock_code,
        exchange_code   = "NSE",
        product_type    = "cash",
    )

    if not resp or resp.get("Status") != 200 or not resp["Success"]:
        raise ValueError(f"Could not fetch underlying data for {stock_code} on {date_str}. "
                         f"Response: {resp}")

    candle = resp["Success"][0]
    return float(candle["close"])


def round_to_strike(price, interval):
    """Round price to nearest strike interval."""
    return round(price / interval) * interval


def label_moneyness(offset, steps):
    """
    offset: integer step from ATM (negative = below ATM, positive = above ATM)
    Returns moneyness label for CALL options
      (for CALL: above ATM = OTM, below ATM = ITM)
    """
    if offset == 0:
        return "ATM"
    elif offset > 0:
        return f"OTM{offset}"   # OTM1, OTM2
    else:
        return f"ITM{abs(offset)}"  # ITM1, ITM2


def get_option_920_candle(breeze, stock_code, strike, expiry_str, option_type, date_str):
    """
    Fetch 9:20 AM 1-min OHLC for one option strike.
    option_type: "call" or "put"
    expiry_str:  "YYYY-MM-DD"
    Returns dict with ohlcv fields, or None if no data.
    """
    # BreezeAPI expects expiry as "YYYY-MM-DDT06:00:00.000Z"
    expiry_dt = f"{expiry_str}T06:00:00.000Z"
    from_dt   = f"{date_str}T09:20:00.000Z"
    to_dt     = f"{date_str}T09:21:00.000Z"

    resp = breeze.get_historical_data_v2(
        interval        = "1minute",
        from_date       = from_dt,
        to_date         = to_dt,
        stock_code      = stock_code,
        exchange_code   = "NFO",
        product_type    = "options",
        expiry_date     = expiry_dt,
        right           = option_type,   # "call" or "put"
        strike_price    = str(int(strike)),
    )

    if not resp or resp.get("Status") != 200 or not resp.get("Success"):
        print(f"  [WARN] No data for {stock_code} {option_type.upper()} {strike} "
              f"expiry={expiry_str}")
        return None

    c = resp["Success"][0]
    return {
        "open":      float(c.get("open",   0)),
        "high":      float(c.get("high",   0)),
        "low":       float(c.get("low",    0)),
        "close":     float(c.get("close",  0)),
        "volume":    int(c.get("volume",   0)),
        "timestamp": c.get("datetime", f"{date_str} 09:20"),
    }


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    print("Connecting to BreezeAPI …")
    breeze = connect_breeze()
    print("Connected.\n")

    rows = []

    for ticker, (interval, nse_symbol) in STOCKS.items():
        print(f"── Processing {ticker} (interval={interval}) ──")

        # Step 1: Get ATM price from 9:20 candle
        try:
            close_920 = get_920_candle(breeze, nse_symbol, TARGET_DATE)
            print(f"  9:20 close price : {close_920}")
        except ValueError as e:
            print(f"  [ERROR] {e}")
            continue

        # Step 2: Round to ATM strike
        atm_strike = round_to_strike(close_920, interval)
        print(f"  ATM strike       : {atm_strike}")

        # Step 3: Build strike list
        # offsets: -STEPS … -1, 0, +1 … +STEPS
        offsets = list(range(-STEPS, STEPS + 1))  # e.g. [-2,-1,0,1,2]

        for option_type in ["call", "put"]:
            print(f"  Fetching {option_type.upper()} options …")

            for offset in offsets:
                strike = atm_strike + offset * interval

                # Moneyness label
                # For CALL: positive offset = OTM, negative = ITM
                # For PUT:  positive offset = ITM, negative = OTM  (flipped)
                if option_type == "call":
                    moneyness = label_moneyness(offset, STEPS)
                else:
                    moneyness = label_moneyness(-offset, STEPS)  # flip for PUT

                print(f"    {moneyness:5s} | strike={strike} | {option_type.upper()}", end=" … ")

                candle = get_option_920_candle(
                    breeze, nse_symbol, strike, EXPIRY_DATE, option_type, TARGET_DATE
                )

                if candle:
                    print(f"close={candle['close']}")
                    rows.append({
                        "ticker":      ticker,
                        "expiry":      EXPIRY_DATE,
                        "option_type": option_type.upper(),
                        "moneyness":   moneyness,
                        "strike":      int(strike),
                        "open":        candle["open"],
                        "high":        candle["high"],
                        "low":         candle["low"],
                        "close":       candle["close"],
                        "volume":      candle["volume"],
                        "timestamp":   candle["timestamp"],
                    })
                else:
                    print("no data")
                    rows.append({
                        "ticker":      ticker,
                        "expiry":      EXPIRY_DATE,
                        "option_type": option_type.upper(),
                        "moneyness":   moneyness,
                        "strike":      int(strike),
                        "open":        None,
                        "high":        None,
                        "low":         None,
                        "close":       None,
                        "volume":      None,
                        "timestamp":   None,
                    })

        print()

    # Step 4: Save to CSV
    if rows:
        df = pd.DataFrame(rows, columns=[
            "ticker", "expiry", "option_type", "moneyness",
            "strike", "open", "high", "low", "close", "volume", "timestamp"
        ])

        # Sort: ticker → option_type → moneyness order
        moneyness_order = (
            [f"OTM{i}" for i in range(STEPS, 0, -1)] +
            ["ATM"] +
            [f"ITM{i}" for i in range(1, STEPS + 1)]
        )
        df["moneyness"] = pd.Categorical(df["moneyness"], categories=moneyness_order, ordered=True)
        df = df.sort_values(["ticker", "option_type", "moneyness"]).reset_index(drop=True)

        df.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ Saved {len(df)} rows to '{OUTPUT_CSV}'")
        print(df.to_string(index=False))
    else:
        print("⚠️  No data collected. Check your credentials and date/expiry settings.")


if __name__ == "__main__":
    main()
