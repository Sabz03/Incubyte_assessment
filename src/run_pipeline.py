import json
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

IST = ZoneInfo("Asia/Kolkata")


def get_connection():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "SKYPOINTS_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "SKYPOINTS_DB"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def generate_mock_files(
    run_date: date, num_members: int = 120, num_txns: int = 250
) -> tuple[Path, Path]:
    """Generate member profile CSV and redemption JSONL with <filename><YYYYMMDD> naming."""
    flat_dir = Path(os.getenv("FLAT_DATA_FOLDER", "data/generated"))
    json_dir = Path(os.getenv("JSON_DATA_FOLDER", "data/generated"))
    flat_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)

    yyyymmdd = run_date.strftime("%Y%m%d")

    member_file = flat_dir / f"skypoints_members{yyyymmdd}.csv"
    redemption_file = json_dir / f"skypoints_redemptions{yyyymmdd}.jsonl"

    rng = random.Random(int(yyyymmdd))

    first_names = [
        "Arjun", "Priya", "Karthik", "Divya", "Vikram", "Ananya", "Rohan", "Meera",
        "Liam", "Olivia", "Noah", "Emma", "Jack", "Charlotte", "Mason", "Amelia",
    ]
    last_names = [
        "Nair", "Sharma", "Iyer", "Reddy", "Kulkarni", "Menon",
        "Smith", "Johnson", "Brown", "Taylor", "Wilson", "Anderson",
    ]
    tiers = ["SILVER", "GOLD", "PLATINUM"]
    agents = ["WEB", "MOBILE", "CALL_CENTER", "AIRPORT_DESK"]
    statuses = ["COMPLETED", "PENDING", "REVERSED"]
    partners = ["Taj Hotels", "Uber", "Amazon", "Apple", "Marriott", "Swiggy"]

    region_map = {
        "IND": [("TN", "641601"), ("KA", "560001"), ("MH", "400001"), ("TS", "500001")],
        "USA": [("CA", "90001"), ("NY", "10001"), ("TX", "73301"), ("WA", "98101")],
        "AUS": [("NSW", "2000"), ("VIC", "3000"), ("QLD", "4000"), ("WA", "6000")],
    }

    member_rows = []
    member_ids = []

    for i in range(1, num_members + 1):
        member_id = f"MBR{1000 + i}"
        member_ids.append(member_id)

        full_name = f"{rng.choice(first_names)} {rng.choice(last_names)}"
        country = rng.choice(["IND", "USA", "AUS"])
        state, post_code = rng.choice(region_map[country])

        enroll_dt = run_date - timedelta(days=rng.randint(180, 2200))
        last_flight_dt = run_date - timedelta(days=rng.randint(5, 240))
        dob_dt = run_date - timedelta(days=rng.randint(21 * 365, 62 * 365))

        member_rows.append(
            {
                "MEMBER_NAME": full_name,
                "MEMBER_ID": member_id,
                "ENROLLMENT_DATE": enroll_dt.strftime("%Y%m%d"),
                "LAST_FLIGHT_DATE": last_flight_dt.strftime("%Y%m%d"),
                "TIER_CODE": rng.choice(tiers),
                "AGENT_NAME": rng.choice(agents),
                "STATE": state,
                "COUNTRY": country,
                "POST_CODE": post_code,
                "DATE_OF_BIRTH": dob_dt.strftime("%Y%m%d"),
                "ACTIVE_MEMBER": rng.choices(["A", "I"], weights=[0.9, 0.1])[0],
            }
        )

    if len(member_ids) >= 4:
        member_rows[1]["COUNTRY"] = "USA"
        member_rows[1]["STATE"], member_rows[1]["POST_CODE"] = ("CA", "94102")
        member_rows[2]["COUNTRY"] = "AUS"
        member_rows[2]["STATE"], member_rows[2]["POST_CODE"] = ("VIC", "3000")

    pd.DataFrame(member_rows).to_csv(member_file, index=False)

    with open(redemption_file, "w", encoding="utf-8") as f:
        for j in range(1, num_txns + 1):
            txn_dt = run_date - timedelta(days=rng.randint(0, 60))
            record = {
                "TXN_ID": f"TXN{yyyymmdd}_{j:04d}",
                "MEMBER_ID": rng.choice(member_ids),
                "FEED_DATE_RAW": yyyymmdd,
                "TXN_DATE_RAW": txn_dt.strftime("%Y%m%d"),
                "PARTNER": rng.choice(partners),
                "MILES_RAW": str(round(rng.uniform(500, 15000), 2)),
                "STATUS": rng.choices(statuses, weights=[0.75, 0.15, 0.10])[0],
                "SOURCE_FILE": redemption_file.name,
            }
            f.write(json.dumps(record) + "\n")

    return member_file, redemption_file


def ingest_to_bronze(
    conn, member_file: Path, redemption_file: Path, batch_id: str, load_ts: datetime
):
    """Upload generated files to Snowflake temp stage and load into BRONZE tables."""
    cur = conn.cursor()
    try:
        cur.execute("USE WAREHOUSE SKYPOINTS_WH")
        cur.execute("USE DATABASE SKYPOINTS_DB")
        cur.execute("CREATE TEMPORARY STAGE IF NOT EXISTS TEMP_SKYPOINTS_STAGE")
        cur.execute(
            """
            CREATE OR REPLACE TEMPORARY FILE FORMAT TEMP_CSV_FF
                TYPE = CSV
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE TEMPORARY FILE FORMAT TEMP_JSON_FF
                TYPE = JSON
            """
        )

        cur.execute(
            f"PUT 'file://{member_file.resolve().as_posix()}' @TEMP_SKYPOINTS_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )
        cur.execute(
            f"PUT 'file://{redemption_file.resolve().as_posix()}' @TEMP_SKYPOINTS_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )

        cur.execute(
            f"""
            INSERT INTO BRONZE.RAW_MEMBER_PROFILES (
                MEMBER_NAME, MEMBER_ID, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
                TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DATE_OF_BIRTH,
                ACTIVE_MEMBER, BATCH_ID, BIW_LOAD_DATE
            )
            SELECT
                $1::VARCHAR,
                $2::VARCHAR,
                $3::VARCHAR,
                $4::VARCHAR,
                $5::VARCHAR,
                $6::VARCHAR,
                $7::VARCHAR,
                $8::VARCHAR,
                $9::VARCHAR,
                $10::VARCHAR,
                $11::VARCHAR,
                '{batch_id}',
                '{load_ts.strftime("%Y-%m-%d %H:%M:%S")}'::TIMESTAMP_NTZ
            FROM @TEMP_SKYPOINTS_STAGE/{member_file.name}.gz
            (FILE_FORMAT => TEMP_CSV_FF)
            """
        )

        cur.execute(
            f"""
            INSERT INTO BRONZE.REDEMPTION_JSON_FLAT (
                TXN_ID, MEMBER_ID, FEED_DATE_RAW, TXN_DATE_RAW,
                PARTNER, MILES_RAW, STATUS, SOURCE_FILE, BATCH_ID, BIW_LOAD_DATE
            )
            SELECT
                $1:TXN_ID::VARCHAR,
                $1:MEMBER_ID::VARCHAR,
                $1:FEED_DATE_RAW::VARCHAR,
                $1:TXN_DATE_RAW::VARCHAR,
                $1:PARTNER::VARCHAR,
                $1:MILES_RAW::VARCHAR,
                $1:STATUS::VARCHAR,
                COALESCE($1:SOURCE_FILE::VARCHAR, '{redemption_file.name}'),
                '{batch_id}',
                '{load_ts.strftime("%Y-%m-%d %H:%M:%S")}'::TIMESTAMP_NTZ
            FROM @TEMP_SKYPOINTS_STAGE/{redemption_file.name}.gz
            (FILE_FORMAT => TEMP_JSON_FF)
            """
        )
    finally:
        cur.close()


def call_silver_sp(conn) -> str:
    """Execute SILVER.SP_RAW_TO_STG() and return its status message."""
    cur = conn.cursor()
    try:
        cur.execute("USE WAREHOUSE SKYPOINTS_WH")
        cur.execute("USE DATABASE SKYPOINTS_DB")
        cur.execute("CALL SILVER.SP_RAW_TO_STG()")
        row = cur.fetchone()
        return str(row[0]) if row else "SP_RAW_TO_STG executed"
    finally:
        cur.close()


def main():
    now_ist = datetime.now(IST)
    run_date = now_ist.date()
    batch_id = now_ist.strftime("%Y%m%d_%H%M%S")

    print(f"[1/3] Generating mock files for {run_date.strftime('%Y%m%d')}...")
    member_file, redemption_file = generate_mock_files(run_date=run_date)
    print(f"      Created: {member_file} and {redemption_file}")

    conn = get_connection()
    try:
        print("[2/3] Ingesting files into BRONZE...")
        ingest_to_bronze(
            conn,
            member_file,
            redemption_file,
            batch_id=batch_id,
            load_ts=now_ist.replace(tzinfo=None),
        )
        print("      Bronze load complete.")

        print("[3/3] Calling SILVER.SP_RAW_TO_STG()...")
        sp_result = call_silver_sp(conn)
        print(f"      {sp_result}")
        print(
            "Done! GOLD.MEMBER_LOYALTY_SUMMARY view is live, and Streamlit will refresh automatically."
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()