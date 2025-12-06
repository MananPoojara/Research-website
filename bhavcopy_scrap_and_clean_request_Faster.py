

import os
import csv
import time
import random
import zipfile
import requests
from requests.cookies import create_cookie
from html import unescape
from urllib.parse import unquote_plus

from datetime import datetime, timedelta
import pandas as pd

# ---------------- FOLDERS ----------------
clean_folder = r"C:/Users/AFF31/Desktop/Manan Pujara/cleaned_csvs"
os.makedirs(clean_folder, exist_ok=True)
WAIT_BETWEEN_REQUESTS = 4
  

cut_off = datetime(2024, 7, 8)

print("\nCLEANING OLD BHAVCOPY FILES\n")

# ---------------- CLEANING FUNCTIONS ----------------
def fixing_appended_rows(file_path):
    with open(file_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        raw = list(reader)

    if not raw:
        print("Empty file:", file_path)
        return pd.DataFrame()

    header = [c for c in raw[0] if c.strip() != ""]
    exp_cols = len(header)
    new_rows = []

    for row in raw[1:]:
        non_empty = [c for c in row if c.strip() != ""]

        if len(non_empty) > exp_cols:
            first = non_empty[:exp_cols]
            second = non_empty[exp_cols:]
            new_rows.append(first)
            new_rows.append(second)
        else:
            padded = row + [""] * (exp_cols - len(row))
            new_rows.append(padded[:exp_cols])

    df = pd.DataFrame(new_rows, columns=header)
    df = df[~(df.astype(str).eq(df.columns)).all(axis=1)]
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df = df[~df.replace("", pd.NA).isna().all(axis=1)]

    return df


def main_cleaning_workflow(file_path):
    logFile = []
    date_part = os.path.basename(file_path)
    print(date_part)

    try:
        try:
            file_date = datetime.strptime(date_part.replace(".csv", ""), "%d-%b-%y")
        except:
            file_date = datetime.strptime(date_part.replace(".csv", ""), "%Y-%m-%d")
    except:
        print("Skipping invalid:", date_part)
        logFile.append(file_path)
        return

    output_path = os.path.join(clean_folder, date_part)

    try:
        df = fixing_appended_rows(file_path)
    except Exception as e:
        print(f"Fixing error: {e}")
        return

    try:
        if file_date < cut_off:
            # Old bhavcopy format
            if "OPTION_TYP" in df.columns:
                df["OPTION_TYPE"] = df["OPTION_TYP"]
            elif "OPTIONTYPE" in df.columns:
                df["OPTION_TYPE"] = df["OPTIONTYPE"]

            df["OPTION_TYPE"] = df["OPTION_TYPE"].str.replace("XX", "FUT")
            cleaned = pd.DataFrame()
            fmt = ["%d-%b-%Y", "%d-%b-%y", "%d-%m-%y", "%d-%m-%Y", "%Y-%m-%d"]

            cleaned["Date"] = df["TIMESTAMP"]
            cleaned["ExpiryDate"] = df["EXPIRY_DT"]

            for f in fmt:
                try:
                    cleaned["Date"] = pd.to_datetime(cleaned["Date"], format=f)
                    break
                except:
                    pass

            for f in fmt:
                try:
                    cleaned["ExpiryDate"] = pd.to_datetime(cleaned["ExpiryDate"], format=f)
                    break
                except:
                    pass

            cleaned = cleaned.dropna(subset=["Date", "ExpiryDate"])
            cleaned["Instrument"] = df["INSTRUMENT"]
            cleaned["Symbol"] = df["SYMBOL"]
            cleaned["StrikePrice"] = df["STRIKE_PR"]
            cleaned["OptionType"] = df["OPTION_TYPE"]
            cleaned["Open"] = df["OPEN"]
            cleaned["High"] = df["HIGH"]
            cleaned["Low"] = df["LOW"]
            cleaned["Close"] = df["CLOSE"]
            cleaned["SettledPrice"] = df["SETTLE_PR"]
            cleaned["Contracts"] = df["CONTRACTS"]
            cleaned["TurnOver"] = df["VAL_INLAKH"]
            cleaned["OpenInterest"] = df["OPEN_INT"]

        else:
            # New bhavcopy format
            mapping = {"IDF": "FUTIDX", "IDO": "OPTIDX", "STF": "FUTSTK", "STO": "OPTSTK"}
            cleaned = pd.DataFrame()
            fmt = ["%Y-%m-%d", "%d-%m-%Y", "%d-%b-%Y"]

            cleaned["Date"] = df["TradDt"]
            cleaned["ExpiryDate"] = df["XpryDt"]

            for f in fmt:
                try:
                    cleaned["Date"] = pd.to_datetime(cleaned["Date"], format=f)
                    break
                except:
                    pass

            for f in fmt:
                try:
                    cleaned["ExpiryDate"] = pd.to_datetime(cleaned["ExpiryDate"], format=f)
                    break
                except:
                    pass

            cleaned = cleaned.dropna(subset=["Date", "ExpiryDate"])
            cleaned["Instrument"] = df["FinInstrmTp"].replace(mapping)
            cleaned["Symbol"] = df["TckrSymb"]
            cleaned["StrikePrice"] = df["StrkPric"]
            cleaned["OptionType"] = df["OptnTp"]
            cleaned["Open"] = df["OpnPric"]
            cleaned["High"] = df["HghPric"]
            cleaned["Low"] = df["LwPric"]
            cleaned["Close"] = df["ClsPric"]
            cleaned["SettledPrice"] = df["SttlmPric"]
            cleaned["Contracts"] = df["TtlTradgVol"]
            cleaned["TurnOver"] = df["TtlTrfVal"]
            cleaned["OpenInterest"] = df["OpnIntrst"]

        cleaned.to_csv(output_path, index=False)
        print(f"Cleaned: {clean_folder}\{date_part}")

    except Exception as e:
        print("Cleaning error:", e)
        logFile.append(file_path)

    if logFile:
        pd.DataFrame(logFile, columns=["FilePath"]).to_csv("./to_be_processed.csv", index=False)


# ---------------- SCRAPER ----------------
API_URL = "https://www.nseindia.com/api/reports"
cookie_string = ""   # paste cookie if needed

BASE_HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/all-reports-derivatives",
    "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}

def session_from_cookie_string_or_homepage(cookie_string, domain="nseindia.com"):
    """
    If cookie_string is provided, parse and set cookies explicitly.
    Otherwise, return a requests.Session() primed by fetching the homepage (best-effort).
    """
    s = requests.Session()
    s.headers.update(BASE_HEADERS)

    if cookie_string:
        cookie_pairs = cookie_string.split("&")
        cookie_dict = {}
        for pair in cookie_pairs:
            if not pair:
                continue
            if "=" in pair:
                k, v = pair.split("=", 1)
                cookie_dict[k] = unquote_plus(v)
            else:
                cookie_dict[pair] = ""
        for k, v in cookie_dict.items():
            c = create_cookie(name=k, value=v, domain=domain, path="/")
            s.cookies.set_cookie(c)
        print("Session created using provided cookie string.")
        # still try to prime homepage to get any server-side cookies too
        try:
            s.get("https://www.nseindia.com/", timeout=12)
        except Exception:
            pass
    else:
        # No cookie string provided: use homepage GET to obtain cookies dynamically
        try:
            r = s.get("https://www.nseindia.com/", timeout=15)
            print("Homepage fetch status (no cookie string):", r.status_code)
            time.sleep(1)
        except Exception as e:
            print("Warning: homepage GET failed (no cookie string):", e)
    return s

session = session_from_cookie_string_or_homepage(cookie_string)


def looks_like_zip(resp):
    if resp.headers.get("Content-Type", "").lower().__contains__("zip"):
        return True
    return resp.content[:4] == b'PK\x03\x04'

def debug_response(resp):
    print("HTTP:", resp.status_code)
    print("Content-Type:", resp.headers.get("Content-Type"))
    snippet = resp.content[:800]
    try:
        print("First bytes (decoded):\n", snippet.decode("utf-8", errors="replace"))
    except Exception:
        print("First bytes (raw):", snippet)


def extract_csv_from_zip(zip_path, dest_path):
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as z:
        members = [m for m in z.namelist() if m.lower().endswith(".csv")]
        if not members:
            print("NO CSV FOUND IN ZIP:", zip_path)
            return False
        csv_inside = members[0]
        with z.open(csv_inside) as f:
            data = f.read()
        with open(dest_path, "wb") as out:
            out.write(data)
    return True


def scrape_and_clean():
    output_folder = "bhavcopy_downloads"
    zips_folder = os.path.join(output_folder, "zips")
    csvs_folder = os.path.join(output_folder, "csvs_new")

    os.makedirs(zips_folder, exist_ok=True)
    os.makedirs(csvs_folder, exist_ok=True)

    cleaned_files = os.listdir(clean_folder)
    if cleaned_files:
        parsed = [pd.to_datetime(f.replace(".csv", "")) for f in cleaned_files]
        start_date = (max(parsed) + timedelta(days=1)).date()
    else:
        start_date = datetime(2000, 1, 1).date()

    now = datetime.now()
    if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
        end_date = now.date()
    else:
        end_date = (now - timedelta(days=1)).date()

    print(f"\nSCRAPING from {start_date} → {end_date}")

    current = end_date
    while current >= start_date:
        date_api = current.strftime("%d-%b-%Y")
        iso_date = current.strftime("%Y-%m-%d")
        print(f"\nProcessing {date_api}")

        archive_name = (
            "F&O - Bhavcopy(csv)" if current < cut_off.date()
            else "F&O - UDiFF Common Bhavcopy Final (zip)"
        )

        params = {
            "archives": f'[{{"name":"{archive_name}","type":"archives","category":"derivatives","section":"equity"}}]',
            "date": date_api,
            "type": "equity",
            "mode": "single"
        }

        try:
            resp = session.get(API_URL, params=params, timeout=25)

            if resp.status_code != 200 or not looks_like_zip(resp):
                print("Failed download / holiday")

            # if resp.status_code == 404:
            #     print("   Not available (404).")
            #     return

            # if resp.status_code != 200:
            #     print("   Non-200 response from server.")
            #     debug_response(resp)
            #     return

            # if not looks_like_zip(resp):
            #     print("   Response not a ZIP. (Maybe blocked / HTML challenge).")
            #     debug_response(resp)
            #     return

            else:
                zip_path = os.path.join(zips_folder, f"{iso_date}.zip")
                with open(zip_path, "wb") as z:
                    z.write(resp.content)
                print("Downloaded ZIP:", zip_path)

                csv_path = os.path.join(csvs_folder, f"{iso_date}.csv")
                if extract_csv_from_zip(zip_path, csv_path):
                    print("EXTRACTED CSV:", csv_path)
                    # Directly clean
                    main_cleaning_workflow(csv_path)

                os.remove(zip_path)

        except Exception as e:
            print("Error:", e)

        time.sleep(WAIT_BETWEEN_REQUESTS)
        current -= timedelta(days=1)


if __name__ == "__main__":
    scrape_and_clean()
