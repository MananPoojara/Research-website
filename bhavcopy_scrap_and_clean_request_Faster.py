import os
import csv
import time
import zipfile
import requests
from requests.cookies import create_cookie
from urllib.parse import unquote_plus
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    when,
    lit,
    to_date,
    regexp_replace,
    concat_ws,
    isnan,
    isnull,
    length,
)
from pyspark.sql.types import StringType, DoubleType, IntegerType

# ---------------- SPARK INITIALIZATION ----------------
spark = (
    SparkSession.builder.appName("BhavcopyProcessor")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

# ---------------- FOLDERS ----------------
clean_folder = r"C:/Users/AFF31/Desktop/Manan Pujara/cleaned_csvs"
os.makedirs(clean_folder, exist_ok=True)
WAIT_BETWEEN_REQUESTS = 4

cut_off = datetime(2024, 7, 8)

print("\nCLEANING OLD BHAVCOPY FILES WITH PYSPARK\n")


# ---------------- CLEANING FUNCTIONS ----------------
def fixing_appended_rows(file_path):
    """Read CSV and fix appended rows, return cleaned CSV path"""
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        raw = list(reader)

    if not raw:
        print("Empty file:", file_path)
        return None

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

    # Write to temporary file for Spark to read
    temp_path = file_path.replace(".csv", "_temp.csv")
    with open(temp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(new_rows)

    return temp_path


def clean_dataframe_spark(df):
    """Remove header rows and empty rows using Spark"""
    # Get column names
    columns = df.columns

    # Remove rows where all values match column names (header duplicates)
    condition = None
    for col_name in columns:
        if condition is None:
            condition = col(col_name) == lit(col_name)
        else:
            condition = condition & (col(col_name) == lit(col_name))

    df = df.filter(~condition)

    # Trim all string columns
    for col_name in df.columns:
        df = df.withColumn(col_name, trim(col(col_name)))

    # Remove rows where all values are empty or null
    non_empty_condition = None
    for col_name in columns:
        col_condition = (col(col_name).isNotNull()) & (length(col(col_name)) > 0)
        if non_empty_condition is None:
            non_empty_condition = col_condition
        else:
            non_empty_condition = non_empty_condition | col_condition

    df = df.filter(non_empty_condition)

    return df


def parse_date_spark(df, col_name, new_col_name):
    """Try multiple date formats using Spark"""
    formats = ["%d-%b-%Y", "%d-%b-%y", "%d-%m-%y", "%d-%m-%Y", "%Y-%m-%d"]

    result_col = None
    for fmt in formats:
        parsed = to_date(col(col_name), fmt)
        if result_col is None:
            result_col = parsed
        else:
            result_col = when(result_col.isNull(), parsed).otherwise(result_col)

    return df.withColumn(new_col_name, result_col)


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
        # Fix appended rows first
        temp_path = fixing_appended_rows(file_path)
        if not temp_path:
            return

        # Read with Spark
        df = spark.read.csv(temp_path, header=True, inferSchema=False)

        # Clean dataframe
        df = clean_dataframe_spark(df)

        if file_date < cut_off:
            # Old bhavcopy format
            if "OPTION_TYP" in df.columns:
                df = df.withColumn("OPTION_TYPE", col("OPTION_TYP"))
            elif "OPTIONTYPE" in df.columns:
                df = df.withColumn("OPTION_TYPE", col("OPTIONTYPE"))

            df = df.withColumn(
                "OPTION_TYPE", regexp_replace(col("OPTION_TYPE"), "XX", "FUT")
            )

            # Parse dates
            df = parse_date_spark(df, "TIMESTAMP", "Date_parsed")
            df = parse_date_spark(df, "EXPIRY_DT", "ExpiryDate_parsed")

            # Drop rows with null dates
            df = df.filter(
                col("Date_parsed").isNotNull() & col("ExpiryDate_parsed").isNotNull()
            )

            # Select and rename columns
            cleaned = df.select(
                col("Date_parsed").alias("Date"),
                col("ExpiryDate_parsed").alias("ExpiryDate"),
                col("INSTRUMENT").alias("Instrument"),
                col("SYMBOL").alias("Symbol"),
                col("STRIKE_PR").alias("StrikePrice"),
                col("OPTION_TYPE").alias("OptionType"),
                col("OPEN").alias("Open"),
                col("HIGH").alias("High"),
                col("LOW").alias("Low"),
                col("CLOSE").alias("Close"),
                col("SETTLE_PR").alias("SettledPrice"),
                col("CONTRACTS").alias("Contracts"),
                col("VAL_INLAKH").alias("TurnOver"),
                col("OPEN_INT").alias("OpenInterest"),
            )

        else:
            # New bhavcopy format
            mapping = {
                "IDF": "FUTIDX",
                "IDO": "OPTIDX",
                "STF": "FUTSTK",
                "STO": "OPTSTK",
            }

            # Parse dates
            df = parse_date_spark(df, "TradDt", "Date_parsed")
            df = parse_date_spark(df, "XpryDt", "ExpiryDate_parsed")

            # Drop rows with null dates
            df = df.filter(
                col("Date_parsed").isNotNull() & col("ExpiryDate_parsed").isNotNull()
            )

            # Replace instrument types
            replace_expr = col("FinInstrmTp")
            for old_val, new_val in mapping.items():
                replace_expr = when(replace_expr == old_val, new_val).otherwise(
                    replace_expr
                )

            df = df.withColumn("Instrument_mapped", replace_expr)

            # Select and rename columns
            cleaned = df.select(
                col("Date_parsed").alias("Date"),
                col("ExpiryDate_parsed").alias("ExpiryDate"),
                col("Instrument_mapped").alias("Instrument"),
                col("TckrSymb").alias("Symbol"),
                col("StrkPric").alias("StrikePrice"),
                col("OptnTp").alias("OptionType"),
                col("OpnPric").alias("Open"),
                col("HghPric").alias("High"),
                col("LwPric").alias("Low"),
                col("ClsPric").alias("Close"),
                col("SttlmPric").alias("SettledPrice"),
                col("TtlTradgVol").alias("Contracts"),
                col("TtlTrfVal").alias("TurnOver"),
                col("OpnIntrst").alias("OpenInterest"),
            )

        # Write to CSV with coalesce(1) to create single file
        cleaned.coalesce(1).write.mode("overwrite").option("header", True).csv(
            output_path + "_temp"
        )

        # Rename the part file to final name
        temp_dir = output_path + "_temp"
        part_files = [
            f
            for f in os.listdir(temp_dir)
            if f.startswith("part-") and f.endswith(".csv")
        ]
        if part_files:
            os.rename(os.path.join(temp_dir, part_files[0]), output_path)
            # Clean up temp directory
            for f in os.listdir(temp_dir):
                os.remove(os.path.join(temp_dir, f))
            os.rmdir(temp_dir)

        # Remove temporary file
        if os.path.exists(temp_path):
            os.remove(temp_path)

        print(f"Cleaned: {output_path}")

    except Exception as e:
        print("Cleaning error:", e)
        import traceback

        traceback.print_exc()
        logFile.append(file_path)

    if logFile:
        log_df = spark.createDataFrame([(f,) for f in logFile], ["FilePath"])
        log_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            "./to_be_processed_temp"
        )


# ---------------- SCRAPER ----------------
API_URL = "https://www.nseindia.com/api/reports"
cookie_string = ""  # paste cookie if needed

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
        try:
            s.get("https://www.nseindia.com/", timeout=12)
        except Exception:
            pass
    else:
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
    return resp.content[:4] == b"PK\x03\x04"


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
    with zipfile.ZipFile(zip_path, "r") as z:
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
        # Filter only CSV files
        csv_files = [f for f in cleaned_files if f.endswith(".csv")]
        if csv_files:
            parsed = [
                datetime.strptime(f.replace(".csv", ""), "%Y-%m-%d") for f in csv_files
            ]
            start_date = (max(parsed) + timedelta(days=1)).date()
        else:
            start_date = datetime(2000, 1, 1).date()
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
            "F&O - Bhavcopy(csv)"
            if current < cut_off.date()
            else "F&O - UDiFF Common Bhavcopy Final (zip)"
        )

        params = {
            "archives": f'[{{"name":"{archive_name}","type":"archives","category":"derivatives","section":"equity"}}]',
            "date": date_api,
            "type": "equity",
            "mode": "single",
        }

        try:
            resp = session.get(API_URL, params=params, timeout=25)

            if resp.status_code != 200 or not looks_like_zip(resp):
                print("Failed download / holiday")
            else:
                zip_path = os.path.join(zips_folder, f"{iso_date}.zip")
                with open(zip_path, "wb") as z:
                    z.write(resp.content)
                print("Downloaded ZIP:", zip_path)

                csv_path = os.path.join(csvs_folder, f"{iso_date}.csv")
                if extract_csv_from_zip(zip_path, csv_path):
                    print("EXTRACTED CSV:", csv_path)
                    # Directly clean with PySpark
                    main_cleaning_workflow(csv_path)

                os.remove(zip_path)

        except Exception as e:
            print("Error:", e)

        time.sleep(WAIT_BETWEEN_REQUESTS)
        current -= timedelta(days=1)


if __name__ == "__main__":
    try:
        scrape_and_clean()
    finally:
        spark.stop()
        print("\nSpark session stopped.")
