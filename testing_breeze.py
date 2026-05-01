"""
Scrape 1-minute historical OHLC for ALL SENSEX & BANKEX option contracts
with expiry 24-Apr-2026, using ICICI Breeze API.

Output:  ./output/SENSEX_24Apr2026_1min.csv
         ./output/BANKEX_24Apr2026_1min.csv

Usage:
    pip install breeze-connect pandas requests
    # set creds (env vars or edit below)
    python scrape_sensex_bankex_options.py
"""

import os
import io
import time
import zipfile
import datetime as dt
from typing import List, Dict, Any

import requests
import pandas as pd
from breeze_connect import BreezeConnect


# --------------------------------------------------------------------------- #
# CONFIG                                                                      #
# --------------------------------------------------------------------------- #
API_KEY        = os.getenv("BREEZE_API_KEY",       "536iWS@16)J67_e9717191`8Y1A1%I93")
API_SECRET     = os.getenv("BREEZE_API_SECRET",    "0757h4608s0551135488a47vp817093g")
SESSION_TOKEN  = os.getenv("BREEZE_SESSION_TOKEN", "55487884")


# Expiry we care about (Thursday weekly expiry)
EXPIRY_DDMMMYYYY = "23-Apr-2026"
EXPIRY_ISO       = "2026-04-23T07:00:00.000Z"   # Breeze historical accepts ISO

# Trading session window for the expiry day (IST 09:15 – 15:30 → UTC -5:30)
HIST_FROM = "2026-04-23T03:45:00.000Z"           # 09:15 IST
HIST_TO   = "2026-04-23T10:00:00.000Z"           # 15:30 IST
INTERVAL  = "1minute"

INDICES = [
    # underlying_in_master, breeze stock_code, friendly name
    {"underlying": "SENSEX", "stock_code": "BSESEN", "name": "SENSEX"},
    {"underlying": "BANKEX", "stock_code": "BSEBAN", "name": "BANKEX"},
]

OUT_DIR = "./output"
os.makedirs(OUT_DIR, exist_ok=True)

# Stay safely under 100 calls/min  (~ 85/min at 0.7s)
SLEEP_BETWEEN_CALLS = 0.7

# Where Breeze publishes its security master (refreshed daily ~08:00 IST)
SEC_MASTER_URL = "https://directlink.icicidirect.com/NewSecurityMaster/SecurityMaster.zip"


# --------------------------------------------------------------------------- #
# 1.  GET STRIKE LIST FROM SECURITY MASTER                                    #
# --------------------------------------------------------------------------- #
def download_security_master() -> Dict[str, pd.DataFrame]:
    """
    Returns {filename_inside_zip: DataFrame}. The zip contains per-exchange
    CSVs; column names have shifted across versions, so we just read all
    files and search them later.
    """
    print(f"Downloading security master from {SEC_MASTER_URL} ...")
    r = requests.get(SEC_MASTER_URL, timeout=60)
    r.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    out = {}
    for name in zf.namelist():
        with zf.open(name) as f:
            try:
                out[name] = pd.read_csv(f, low_memory=False)
            except Exception as e:
                print(f"  skip {name}: {e}")
    return out


def find_strikes_from_master(masters: Dict[str, pd.DataFrame],
                             underlying: str,
                             expiry: dt.date) -> List[float]:
    """Search every master CSV for SENSEX/BANKEX rows matching the expiry."""
    target_strikes: set = set()

    for fname, df in masters.items():
        if df.empty:
            continue
        cols = {c.lower(): c for c in df.columns}

        expiry_col = next((cols[c] for c in cols if "expiry" in c), None)
        strike_col = next((cols[c] for c in cols if "strike" in c), None)
        sym_col    = next((cols[c] for c in cols
                           if c in ("symbol", "underlying", "instrumentname",
                                    "shortname", "scripname", "stockcode")), None)
        if not (expiry_col and strike_col and sym_col):
            continue

        sub = df.copy()
        sub[expiry_col] = pd.to_datetime(sub[expiry_col], errors="coerce",
                                         dayfirst=True)
        sub = sub[sub[expiry_col].dt.date == expiry]
        if sub.empty:
            continue

        sub = sub[sub[sym_col].astype(str).str.upper().str.contains(underlying)]
        if sub.empty:
            continue

        for s in pd.to_numeric(sub[strike_col], errors="coerce").dropna().unique():
            target_strikes.add(float(s))

    return sorted(target_strikes)


def manual_strike_grid(center: float, step: int, n_each_side: int = 60) -> List[float]:
    """Fallback grid if the master can't be fetched."""
    return [float(center + i * step) for i in range(-n_each_side, n_each_side + 1)]


# --------------------------------------------------------------------------- #
# 2.  BREEZE HISTORICAL FETCH                                                 #
# --------------------------------------------------------------------------- #
def make_breeze() -> BreezeConnect:
    breeze = BreezeConnect(api_key=API_KEY)
    breeze.generate_session(api_secret=API_SECRET, session_token=SESSION_TOKEN)
    return breeze


def fetch_one(breeze: BreezeConnect, stock_code: str,
              strike: float, right: str) -> List[Dict[str, Any]]:
    try:
        resp = breeze.get_historical_data_v2(
            interval     = INTERVAL,
            from_date    = HIST_FROM,
            to_date      = HIST_TO,
            stock_code   = stock_code,
            exchange_code= "BFO",
            product_type = "options",
            expiry_date  = EXPIRY_ISO,
            right        = right,
            strike_price = str(strike),
        )
    except Exception as e:
        print(f"    [!] {stock_code} {int(strike)} {right}: {e}")
        return []

    if resp.get("Status") != 200:
        err = resp.get("Error")
        # Empty far-OTM strikes are normal — only log real errors
        if err and "no data" not in str(err).lower():
            print(f"    [!] {stock_code} {int(strike)} {right}: {err}")
        return []

    rows = resp.get("Success") or []
    for row in rows:
        row["index"]        = stock_code
        row["strike_price"] = strike
        row["right"]        = right
    return rows


def scrape_index(breeze: BreezeConnect, idx: Dict[str, str],
                 strikes: List[float]) -> None:
    """
    Writes one CSV per (strike, right). Filename pattern:
        {NAME}24APRIL2026{STRIKE}{CE|PE}.csv
    e.g. SENSEX24APRIL202680000CE.csv
    """
    name = idx["name"]
    code = idx["stock_code"]
    total = len(strikes) * 2
    print(f"\n=== {name} — {len(strikes)} strikes x 2 rights = {total} calls ===")

    done    = 0
    empty   = 0
    written = 0

    for strike in strikes:
        for right in ("call", "put"):
            rows = fetch_one(breeze, code, strike, right)
            done += 1

            if not rows:
                empty += 1
            else:
                df = pd.DataFrame(rows)
                if "datetime" in df.columns:
                    df = df.sort_values("datetime")

                suffix = "CE" if right == "call" else "PE"
                strike_str = str(int(strike)) if float(strike).is_integer() else str(strike)
                fname = f"{name}23APRIL2026{strike_str}{suffix}.csv"
                fpath = os.path.join(OUT_DIR, fname)
                df.to_csv(fpath, index=False)
                written += 1

            if done % 25 == 0:
                print(f"  {name}: {done}/{total}  written={written}  empty={empty}")
            time.sleep(SLEEP_BETWEEN_CALLS)

    print(f"  {name} done -> {written} csv files written, {empty} empty contracts skipped")


# --------------------------------------------------------------------------- #
# MAIN                                                                        #
# --------------------------------------------------------------------------- #
def main():
    breeze = make_breeze()

    # ---- 1) get strike list --------------------------------------------- #
    try:
        masters = download_security_master()
        print(f"loaded {len(masters)} master files: {list(masters)}")
    except Exception as e:
        print(f"[!] could not fetch security master ({e}); will use manual grids")
        masters = {}

    target_expiry = dt.date(2026, 4, 23)

    for idx in INDICES:
        strikes = find_strikes_from_master(masters, idx["underlying"], target_expiry) \
                  if masters else []

        if not strikes:
            # fallback — tweak centers to where each index was on 24-Apr-2026
            if idx["name"] == "SENSEX":
                strikes = manual_strike_grid(center=80000, step=100, n_each_side=60)
            else:  # BANKEX
                strikes = manual_strike_grid(center=63000, step=100, n_each_side=60)
            print(f"[{idx['name']}] using manual grid of {len(strikes)} strikes")
        else:
            print(f"[{idx['name']}] master gave {len(strikes)} strikes "
                  f"({strikes[0]} ... {strikes[-1]})")

        scrape_index(breeze, idx, strikes)

    print(f"\nAll done. CSVs are in: {os.path.abspath(OUT_DIR)}")


if __name__ == "__main__":
    main()