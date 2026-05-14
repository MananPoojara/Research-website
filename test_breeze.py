"""
╔══════════════════════════════════════════════════════════════════════╗
║     BREEZE API — TRADE ANALYSIS DATA DOWNLOADER                     ║
║     Downloads NIFTY + SENSEX spot & options OHLC for 13 May 2026   ║
║     Generates Excel with all sheets + annotated charts              ║
╚══════════════════════════════════════════════════════════════════════╝

HOW TO USE:
1.  pip install breeze-connect pandas openpyxl matplotlib
2.  Fill in your API_KEY, SECRET_KEY, SESSION_TOKEN below
3.  Get SESSION_TOKEN daily: login to https://api.icicidirect.com
    → Apps → Generate Session → copy the token from the URL
    (It looks like: https://api.icicidirect.com/apiuser/login?api_key=...&apisession=<TOKEN>)
4.  Run: python breeze_data_downloader.py
5.  Output: breeze_data_13May2026.xlsx  (all OHLC sheets + charts)
"""

import os
import time
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from openpyxl import load_workbook
from openpyxl.drawing.image import Image as XLImage
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ─────────────────────────────────────────────────────────────────────
# ★  FILL THESE IN BEFORE RUNNING
# ─────────────────────────────────────────────────────────────────────
API_KEY      = "536iWS@16)J67_e9717191`8Y1A1%I93"
SECRET_KEY   = "0757h4608s0551135488a47vp817093g"
SESSION_TOKEN = "55612123"   # generate fresh daily

# ─────────────────────────────────────────────────────────────────────
# CONFIG — matches your Excel trade data
# ─────────────────────────────────────────────────────────────────────
TRADE_DATE        = "2026-05-13"
MARKET_OPEN       = "2026-05-13T09:15:00.000Z"
MARKET_CLOSE      = "2026-05-13T15:30:00.000Z"
INTERVAL          = "5minute"          # options: "1minute", "5minute", "30minute"

NIFTY_EXPIRY      = "2026-05-19T06:00:00.000Z"
SENSEX_EXPIRY     = "2026-05-14T06:00:00.000Z"

# All strikes extracted from your trade log
NIFTY_STRIKES     = [23300, 23350, 23400, 23450, 23500, 23550, 23600, 24200, 24600]
SENSEX_STRIKES    = [74200, 74400, 74600, 74800, 74900, 75000, 75100, 75200, 75300,
                     76300, 76500, 77200, 77300, 77400, 77500, 77900]

OUTPUT_FILE       = "breeze_data_13May2026.xlsx"
CHARTS_DIR        = "charts"

# Key trade events from your Sensex sheet (for chart annotation)
# Format: (time_str, label, direction)  direction: "long" or "short"
NIFTY_TRADE_EVENTS = [
    ("09:36", "23300PE Buy", "long"),
    ("10:30", "23350CE+23450PE (Synth Short)", "short"),
    ("11:38", "23400CE+23500PE (Synth Short)", "short"),
    ("12:55", "23450CE Sell + 23450PE Buy", "short"),
    ("13:05", "23400CE Sell + 23450PE Buy", "short"),
    ("15:04", "23400CE Buy + 23500PE Sell\n(Synth Long)", "long"),
]

SENSEX_TRADE_EVENTS = [
    ("09:38", "74200PE+74400PE Buy", "long"),
    ("11:40", "74800PE Sell + 76300CE Buy", "short"),
    ("12:15", "74900PE Sell + 77900CE Buy\n(Square off)", "short"),
    ("12:55", "75000CE Buy (Day High BO)", "long"),
    ("12:56", "75000CE Add + 75100/75200CE Add\n(New High BO)", "long"),
    ("13:36", "75000CE Add ATM", "long"),
    ("14:46", "74800CE Sell + 74800PE Buy\n(5min fail → Synth Short)", "short"),
    ("15:05", "74600CE Sell + 74600PE+74800PE Buy\n(Break Low → Synth Short)", "short"),
    ("14:46", "75100/75200CE Sell (50% SL hit)", "short"),
]


# ─────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────
def time_to_dt(t_str, date="2026-05-13"):
    return datetime.datetime.strptime(f"{date} {t_str}", "%Y-%m-%d %H:%M")

def style_header(ws, row, cols, bg_hex, font_color="FFFFFF"):
    thin = Side(style="thin", color="CCCCCC")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    fill = PatternFill("solid", fgColor=bg_hex)
    for col in range(1, cols + 1):
        cell = ws.cell(row=row, column=col)
        cell.font = Font(bold=True, color=font_color, size=10)
        cell.fill = fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = border

def write_df_to_sheet(wb, sheet_name, df, header_color="1F4E79"):
    if sheet_name in wb.sheetnames:
        del wb[sheet_name]
    ws = wb.create_sheet(sheet_name)
    if df.empty:
        ws["A1"] = "No data returned — check API parameters or market was closed"
        return ws
    headers = list(df.columns)
    ws.append(headers)
    for row in df.itertuples(index=False):
        ws.append(list(row))
    style_header(ws, 1, len(headers), header_color)
    for i, col in enumerate(headers, 1):
        max_len = max(len(str(col)), df[col].astype(str).str.len().max() if not df.empty else 10)
        ws.column_dimensions[get_column_letter(i)].width = min(max_len + 2, 25)
    return ws


# ─────────────────────────────────────────────────────────────────────
# BREEZE CONNECTION
# ─────────────────────────────────────────────────────────────────────
def connect_breeze():
    try:
        from breeze_connect import BreezeConnect
    except ImportError:
        print("❌  breeze-connect not installed. Run: pip install breeze-connect")
        raise

    breeze = BreezeConnect(api_key=API_KEY)
    print("🔗  Connecting to Breeze API...")
    resp = breeze.generate_session(api_secret=SECRET_KEY, session_token=SESSION_TOKEN)
    if resp.get("Status") == 200:
        print("✅  Session established")
    else:
        print(f"⚠️   Session response: {resp}")
    return breeze


# ─────────────────────────────────────────────────────────────────────
# DATA FETCHERS
# ─────────────────────────────────────────────────────────────────────
def fetch_index_ohlc(breeze, symbol, exchange):
    """Download NIFTY or SENSEX spot intraday OHLC."""
    print(f"  📥  {symbol} ({exchange}) spot {INTERVAL} candles...")
    try:
        resp = breeze.get_historical_data_v2(
            interval=INTERVAL,
            from_date=MARKET_OPEN,
            to_date=MARKET_CLOSE,
            stock_code=symbol,
            exchange_code=exchange,
            product_type="cash",
        )
        if resp.get("Status") == 200 and resp.get("Success"):
            df = pd.DataFrame(resp["Success"])
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.sort_values("datetime").reset_index(drop=True)
            for col in ["open", "high", "low", "close", "volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            return df
        else:
            print(f"     ⚠️  No data: {resp.get('Error', 'Unknown error')}")
            return pd.DataFrame()
    except Exception as e:
        print(f"     ❌  Error: {e}")
        return pd.DataFrame()


def fetch_option_ohlc(breeze, symbol, exchange, strike, right, expiry):
    """Download a single options strike OHLC."""
    right_label = "call" if right.upper() in ["CE", "CALL"] else "put"
    right_display = "CE" if right_label == "call" else "PE"
    label = f"{symbol} {strike}{right_display}"
    try:
        resp = breeze.get_historical_data_v2(
            interval=INTERVAL,
            from_date=MARKET_OPEN,
            to_date=MARKET_CLOSE,
            stock_code=symbol,
            exchange_code=exchange,
            product_type="options",
            expiry_date=expiry,
            right=right_label,
            strike_price=str(strike),
        )
        if resp.get("Status") == 200 and resp.get("Success"):
            df = pd.DataFrame(resp["Success"])
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.sort_values("datetime").reset_index(drop=True)
            for col in ["open", "high", "low", "close", "volume", "open_interest"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            df["strike"] = strike
            df["right"] = right_display
            df["symbol"] = symbol
            return label, df
        else:
            return label, pd.DataFrame()
    except Exception as e:
        print(f"     ❌  {label}: {e}")
        return label, pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────
# CHART GENERATORS
# ─────────────────────────────────────────────────────────────────────
def make_intraday_chart(df_ohlc, symbol, events, filename):
    """Generate annotated intraday candlestick-style chart."""
    if df_ohlc.empty:
        return None

    os.makedirs(CHARTS_DIR, exist_ok=True)
    fig, axes = plt.subplots(2, 1, figsize=(16, 10),
                              gridspec_kw={"height_ratios": [3, 1]},
                              facecolor="#0d1117")
    ax, ax_vol = axes

    for a in axes:
        a.set_facecolor("#0d1117")
        a.tick_params(colors="#adbac7", labelsize=8)
        for spine in a.spines.values():
            spine.set_color("#2d333b")

    df = df_ohlc.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    x = range(len(df))
    x_list = list(x)

    # Candles
    for i, row in df.iterrows():
        color = "#3fb950" if row["close"] >= row["open"] else "#f85149"
        ax.plot([i, i], [row["low"], row["high"]], color=color, linewidth=0.8, zorder=2)
        body_lo = min(row["open"], row["close"])
        body_hi = max(row["open"], row["close"])
        body_h  = max(body_hi - body_lo, 0.5)
        ax.bar(i, body_h, bottom=body_lo, color=color, width=0.7, zorder=3)

    # Volume bars
    if "volume" in df.columns:
        v_colors = ["#3fb950" if r["close"] >= r["open"] else "#f85149"
                    for _, r in df.iterrows()]
        ax_vol.bar(x_list, df["volume"], color=v_colors, width=0.7, alpha=0.7)
        ax_vol.set_ylabel("Volume", color="#adbac7", fontsize=8)

    # Day high / low lines
    day_high = df["high"].max()
    day_low  = df["low"].min()
    ax.axhline(day_high, color="#e3b341", linewidth=0.8, linestyle="--", alpha=0.8,
               label=f"Day High {day_high:,.0f}")
    ax.axhline(day_low,  color="#f85149", linewidth=0.8, linestyle="--", alpha=0.8,
               label=f"Day Low {day_low:,.0f}")

    # Trade event annotations
    color_map = {"long": "#3fb950", "short": "#f85149"}
    marker_map = {"long": "▲", "short": "▼"}
    annotated_count = 0
    for t_str, label, direction in events:
        try:
            target_dt = time_to_dt(t_str)
            diffs = abs(df["datetime"] - target_dt)
            idx = diffs.idxmin()
            price = df.loc[idx, "close"]
            offset = (day_high - day_low) * 0.03
            y_pos = price + offset if direction == "long" else price - offset
            ax.annotate(
                f"{marker_map[direction]} {t_str}",
                xy=(idx, price),
                xytext=(idx, y_pos),
                color=color_map[direction],
                fontsize=7.5,
                fontweight="bold",
                ha="center",
                arrowprops=dict(arrowstyle="-", color=color_map[direction],
                                linewidth=0.8),
            )
            annotated_count += 1
        except Exception:
            pass

    # X-axis ticks (every 30 min)
    tick_idx = [i for i, dt in enumerate(df["datetime"])
                if dt.minute % 30 == 0 and dt.minute != 0 or i == 0]
    tick_labels = [df["datetime"].iloc[i].strftime("%H:%M") for i in tick_idx]
    ax.set_xticks(tick_idx)
    ax.set_xticklabels(tick_labels, color="#adbac7", fontsize=8)
    ax_vol.set_xticks(tick_idx)
    ax_vol.set_xticklabels(tick_labels, color="#adbac7", fontsize=8)

    ax.set_title(f"{symbol} — 13 May 2026 | {INTERVAL} | Trade Events Annotated",
                 color="#e6edf3", fontsize=13, pad=12, fontweight="bold")
    ax.set_ylabel("Price", color="#adbac7", fontsize=9)
    ax.legend(loc="upper left", fontsize=8, facecolor="#161b22", labelcolor="#adbac7",
              framealpha=0.8)
    ax.grid(color="#2d333b", linewidth=0.4, alpha=0.5)
    ax_vol.grid(color="#2d333b", linewidth=0.3, alpha=0.4)
    ax.set_xlim(-0.5, len(df) - 0.5)
    ax_vol.set_xlim(-0.5, len(df) - 0.5)

    plt.tight_layout()
    filepath = os.path.join(CHARTS_DIR, filename)
    plt.savefig(filepath, dpi=150, bbox_inches="tight", facecolor="#0d1117")
    plt.close()
    print(f"     📊  Chart saved: {filepath} ({annotated_count} annotations)")
    return filepath


def make_option_chain_snapshot(nifty_dfs, sensex_dfs, filename):
    """Side-by-side ATM options chart (CE vs PE premium over the day)."""
    os.makedirs(CHARTS_DIR, exist_ok=True)
    fig, axes = plt.subplots(1, 2, figsize=(16, 6), facecolor="#0d1117")

    datasets = [
        ("NIFTY ATM Options (23400/23450)", nifty_dfs, [("23400", "CE"), ("23400", "PE"),
                                                          ("23450", "CE"), ("23450", "PE")]),
        ("SENSEX ATM Options (74800/74900)", sensex_dfs, [("74800", "CE"), ("74800", "PE"),
                                                            ("74900", "CE"), ("74900", "PE")]),
    ]
    colors = {"CE": "#58a6ff", "PE": "#f85149"}
    styles = {"23400": "-", "23450": "--", "74800": "-", "74900": "--"}

    for ax, (title, dfs_dict, pairs) in zip(axes, datasets):
        ax.set_facecolor("#0d1117")
        ax.tick_params(colors="#adbac7", labelsize=8)
        for spine in ax.spines.values():
            spine.set_color("#2d333b")

        for strike_str, right in pairs:
            key = f"{list(dfs_dict.keys())[0].split()[0]} {strike_str}{right}"
            for k, df in dfs_dict.items():
                if strike_str in k and right in k and not df.empty:
                    ax.plot(df["datetime"], df["close"],
                            label=f"{strike_str}{right}",
                            color=colors[right],
                            linestyle=styles.get(strike_str, "-"),
                            linewidth=1.4, alpha=0.9)
                    break

        ax.set_title(title, color="#e6edf3", fontsize=11, pad=10, fontweight="bold")
        ax.set_ylabel("Premium (₹)", color="#adbac7", fontsize=9)
        ax.legend(loc="upper right", fontsize=8, facecolor="#161b22",
                  labelcolor="#adbac7", framealpha=0.8)
        ax.grid(color="#2d333b", linewidth=0.4, alpha=0.5)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 30]))
        plt.setp(ax.get_xticklabels(), rotation=45, color="#adbac7")

    plt.suptitle("Options Premium Movement — 13 May 2026",
                 color="#e6edf3", fontsize=13, fontweight="bold", y=1.01)
    plt.tight_layout()
    filepath = os.path.join(CHARTS_DIR, filename)
    plt.savefig(filepath, dpi=150, bbox_inches="tight", facecolor="#0d1117")
    plt.close()
    print(f"     📊  Chart saved: {filepath}")
    return filepath


# ─────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────
def main():
    print("\n" + "═" * 60)
    print("  BREEZE DATA DOWNLOADER — 13 May 2026")
    print("═" * 60 + "\n")

    breeze = connect_breeze()
    wb = load_workbook_or_new()

    # ── 1. NIFTY SPOT ────────────────────────────────────────────────
    print("\n[1/4] Downloading NIFTY spot data...")
    df_nifty_spot = fetch_index_ohlc(breeze, "NIFTY", "NSE")
    write_df_to_sheet(wb, "NIFTY_Spot", df_nifty_spot, "1F4E79")
    time.sleep(0.5)

    # ── 2. SENSEX SPOT ───────────────────────────────────────────────
    print("\n[2/4] Downloading SENSEX spot data...")
    df_sensex_spot = fetch_index_ohlc(breeze, "SENSEX", "BSE")
    write_df_to_sheet(wb, "SENSEX_Spot", df_sensex_spot, "833C11")
    time.sleep(0.5)

    # ── 3. NIFTY OPTIONS ─────────────────────────────────────────────
    print(f"\n[3/4] Downloading NIFTY options ({len(NIFTY_STRIKES)} strikes × 2 sides)...")
    nifty_option_dfs = {}
    nifty_all_rows = []

    for strike in NIFTY_STRIKES:
        for right in ["CE", "PE"]:
            label, df = fetch_option_ohlc(
                breeze, "NIFTY", "NFO", strike, right, NIFTY_EXPIRY
            )
            print(f"     {'✅' if not df.empty else '⚠️ '} {label}: "
                  f"{len(df)} rows")
            nifty_option_dfs[label] = df
            if not df.empty:
                nifty_all_rows.append(df)
            time.sleep(0.3)      # be gentle with rate limits

    if nifty_all_rows:
        df_nifty_combined = pd.concat(nifty_all_rows, ignore_index=True)
        df_nifty_combined = df_nifty_combined.sort_values(
            ["strike", "right", "datetime"]).reset_index(drop=True)
        write_df_to_sheet(wb, "NIFTY_Options_All", df_nifty_combined, "1F4E79")

        # Pivot: each strike-right as a column for easy comparison
        try:
            df_pivot = df_nifty_combined.pivot_table(
                index="datetime", columns=["strike", "right"],
                values="close", aggfunc="last"
            )
            df_pivot.columns = [f"{s}{r}" for s, r in df_pivot.columns]
            df_pivot = df_pivot.reset_index()
            write_df_to_sheet(wb, "NIFTY_Close_Pivot", df_pivot, "375623")
        except Exception as e:
            print(f"     ⚠️  Pivot failed: {e}")

    # ── 4. SENSEX OPTIONS ────────────────────────────────────────────
    print(f"\n[4/4] Downloading SENSEX options ({len(SENSEX_STRIKES)} strikes × 2 sides)...")
    sensex_option_dfs = {}
    sensex_all_rows = []

    for strike in SENSEX_STRIKES:
        for right in ["CE", "PE"]:
            label, df = fetch_option_ohlc(
                breeze, "SENSEX", "BSE", strike, right, SENSEX_EXPIRY
            )
            print(f"     {'✅' if not df.empty else '⚠️ '} {label}: "
                  f"{len(df)} rows")
            sensex_option_dfs[label] = df
            if not df.empty:
                sensex_all_rows.append(df)
            time.sleep(0.3)

    if sensex_all_rows:
        df_sensex_combined = pd.concat(sensex_all_rows, ignore_index=True)
        df_sensex_combined = df_sensex_combined.sort_values(
            ["strike", "right", "datetime"]).reset_index(drop=True)
        write_df_to_sheet(wb, "SENSEX_Options_All", df_sensex_combined, "833C11")

        try:
            df_pivot_s = df_sensex_combined.pivot_table(
                index="datetime", columns=["strike", "right"],
                values="close", aggfunc="last"
            )
            df_pivot_s.columns = [f"{s}{r}" for s, r in df_pivot_s.columns]
            df_pivot_s = df_pivot_s.reset_index()
            write_df_to_sheet(wb, "SENSEX_Close_Pivot", df_pivot_s, "833C11")
        except Exception as e:
            print(f"     ⚠️  Pivot failed: {e}")

    # ── CHARTS ───────────────────────────────────────────────────────
    print("\n[Charts] Generating annotated charts...")
    chart_paths = []

    nifty_chart = make_intraday_chart(
        df_nifty_spot, "NIFTY 50", NIFTY_TRADE_EVENTS, "nifty_intraday.png"
    )
    if nifty_chart:
        chart_paths.append(("NIFTY_Spot", nifty_chart))

    sensex_chart = make_intraday_chart(
        df_sensex_spot, "SENSEX", SENSEX_TRADE_EVENTS, "sensex_intraday.png"
    )
    if sensex_chart:
        chart_paths.append(("SENSEX_Spot", sensex_chart))

    options_chart = make_option_chain_snapshot(
        nifty_option_dfs, sensex_option_dfs, "atm_options_premium.png"
    )
    if options_chart:
        chart_paths.append(("NIFTY_Options_All", options_chart))

    # Embed charts into Excel sheets
    for sheet_name, img_path in chart_paths:
        if sheet_name in wb.sheetnames and os.path.exists(img_path):
            ws = wb[sheet_name]
            img = XLImage(img_path)
            img.width  = 900
            img.height = 450
            # Place image below the data (row 50 approx)
            ws.add_image(img, "A50")

    # ── ADD SUMMARY SHEET ────────────────────────────────────────────
    add_summary_sheet(wb, df_nifty_spot, df_sensex_spot, nifty_option_dfs, sensex_option_dfs)

    # ── SAVE ─────────────────────────────────────────────────────────
    wb.save(OUTPUT_FILE)
    print(f"\n✅  All done! Saved to: {OUTPUT_FILE}")
    print(f"    Sheets: {wb.sheetnames}")
    print(f"    Charts: {CHARTS_DIR}/\n")


def load_workbook_or_new():
    """Create fresh workbook, remove default Sheet."""
    from openpyxl import Workbook
    wb = Workbook()
    if "Sheet" in wb.sheetnames:
        del wb["Sheet"]
    return wb


def add_summary_sheet(wb, df_nifty, df_sensex, nifty_opts, sensex_opts):
    """Dashboard-style summary of the day's market data."""
    ws = wb.create_sheet("📊 Summary", 0)
    ws.sheet_view.showGridLines = False

    # Title
    ws["A1"] = "TRADE ANALYSIS — MARKET DATA SUMMARY"
    ws["A1"].font = Font(bold=True, size=14, color="FFFFFF")
    ws["A1"].fill = PatternFill("solid", fgColor="1F4E79")
    ws["A1"].alignment = Alignment(horizontal="center")
    ws.merge_cells("A1:H1")

    ws["A2"] = f"Date: 13 May 2026  |  Downloaded: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
    ws["A2"].font = Font(italic=True, size=10, color="595959")
    ws.merge_cells("A2:H2")

    row = 4
    def add_section(title, color):
        nonlocal row
        ws.cell(row, 1, title).font = Font(bold=True, size=11, color="FFFFFF")
        ws.cell(row, 1).fill = PatternFill("solid", fgColor=color)
        ws.cell(row, 1).alignment = Alignment(horizontal="left")
        ws.merge_cells(f"A{row}:H{row}")
        row += 1

    def add_kv(label, value):
        nonlocal row
        ws.cell(row, 1, label).font = Font(size=10, color="595959")
        ws.cell(row, 2, value).font = Font(bold=True, size=10)
        row += 1

    # NIFTY summary
    add_section("NIFTY 50 — Spot", "1F4E79")
    if not df_nifty.empty:
        add_kv("Day Open",  f"₹{df_nifty['open'].iloc[0]:,.2f}")
        add_kv("Day High",  f"₹{df_nifty['high'].max():,.2f}")
        add_kv("Day Low",   f"₹{df_nifty['low'].min():,.2f}")
        add_kv("Day Close", f"₹{df_nifty['close'].iloc[-1]:,.2f}")
        chg = df_nifty['close'].iloc[-1] - df_nifty['open'].iloc[0]
        pct = chg / df_nifty['open'].iloc[0] * 100
        add_kv("Day Change", f"₹{chg:+,.2f}  ({pct:+.2f}%)")
        add_kv("Total Candles", len(df_nifty))
    else:
        add_kv("Status", "No data fetched")

    row += 1
    # SENSEX summary
    add_section("SENSEX — Spot", "833C11")
    if not df_sensex.empty:
        add_kv("Day Open",  f"₹{df_sensex['open'].iloc[0]:,.2f}")
        add_kv("Day High",  f"₹{df_sensex['high'].max():,.2f}")
        add_kv("Day Low",   f"₹{df_sensex['low'].min():,.2f}")
        add_kv("Day Close", f"₹{df_sensex['close'].iloc[-1]:,.2f}")
        chg = df_sensex['close'].iloc[-1] - df_sensex['open'].iloc[0]
        pct = chg / df_sensex['open'].iloc[0] * 100
        add_kv("Day Change", f"₹{chg:+,.2f}  ({pct:+.2f}%)")
    else:
        add_kv("Status", "No data fetched")

    row += 1
    # Options summary
    add_section("NIFTY Options — Key Strikes", "375623")
    add_kv("Strikes downloaded", ", ".join(str(s) for s in NIFTY_STRIKES))
    add_kv("Types", "CE + PE for each strike")
    filled = sum(1 for df in nifty_opts.values() if not df.empty)
    add_kv("Successful fetches", f"{filled} / {len(nifty_opts)}")

    row += 1
    add_section("SENSEX Options — Key Strikes", "7F3F00")
    add_kv("Strikes downloaded", ", ".join(str(s) for s in SENSEX_STRIKES))
    filled_s = sum(1 for df in sensex_opts.values() if not df.empty)
    add_kv("Successful fetches", f"{filled_s} / {len(sensex_opts)}")

    row += 2
    ws.cell(row, 1, "💡  Tip: Use NIFTY_Close_Pivot and SENSEX_Close_Pivot sheets "
                    "to cross-reference option premiums with trade entry times.").font = Font(
        italic=True, size=9, color="595959")
    ws.merge_cells(f"A{row}:H{row}")

    ws.column_dimensions["A"].width = 22
    ws.column_dimensions["B"].width = 30


# ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
