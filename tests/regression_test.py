import os
from datetime import datetime
from zoneinfo import ZoneInfo

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

IST = ZoneInfo("Asia/Kolkata")
AUDIT_SCHEMA = "AUDIT"
TEST_AUDIT_TABLE = "AUDIT.TEST_AUDIT"


def get_connection():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "SKYPOINTS_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "SKYPOINTS_DB"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def ensure_test_audit_table(cur) -> None:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_SCHEMA}")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TEST_AUDIT_TABLE} (
            TEST_RUN_ID   VARCHAR NOT NULL,
            LAYER         VARCHAR NOT NULL,
            TEST_SCENARIO VARCHAR NOT NULL,
            STATUS        VARCHAR NOT NULL,
            DETAILS       VARCHAR,
            EXECUTED_AT   TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )


def record_check(
    cur, run_id: str, layer: str, scenario: str, status: str, details: str
) -> None:
    safe_layer = layer.replace("'", "''")
    safe_scenario = scenario.replace("'", "''")
    safe_details = str(details).replace("'", "''")
    cur.execute(
        f"""
        INSERT INTO {TEST_AUDIT_TABLE} (
            TEST_RUN_ID, LAYER, TEST_SCENARIO, STATUS, DETAILS, EXECUTED_AT
        )
        VALUES (
            '{run_id}',
            '{safe_layer}',
            '{safe_scenario}',
            '{status}',
            '{safe_details}',
            CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
        )
        """
    )
    print(f"[{status}] [{layer}] {scenario} -> {details}")


def scalar(cur, sql: str):
    cur.execute(sql)
    row = cur.fetchone()
    return row[0] if row else None


def run_checks() -> None:
    run_id = f"CHK_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}"
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("USE WAREHOUSE SKYPOINTS_WH")
        cur.execute("USE DATABASE SKYPOINTS_DB")
        ensure_test_audit_table(cur)

        # --- 1. BRONZE CHECKS ---
        raw_mem_cnt = scalar(cur, "SELECT COUNT(*) FROM BRONZE.RAW_MEMBER_PROFILES")
        raw_batches = scalar(cur, "SELECT COUNT(DISTINCT BATCH_ID) FROM BRONZE.RAW_MEMBER_PROFILES")
        record_check(
            cur, run_id, "BRONZE", "RAW_MEMBER_PROFILES_COUNT_AND_BATCHES",
            "INFO", f"rows={raw_mem_cnt}, distinct_batches={raw_batches}"
        )

        null_id_cnt = scalar(
            cur,
            """
            SELECT COUNT(*)
            FROM BRONZE.RAW_MEMBER_PROFILES
            WHERE MEMBER_ID IS NULL OR TRIM(MEMBER_ID) = ''
            """,
        )
        record_check(
            cur, run_id, "BRONZE", "RAW_MEMBER_NULL_OR_BLANK_ID",
            "WARN" if null_id_cnt > 0 else "PASS", f"count={null_id_cnt}"
        )

        null_name_cnt = scalar(
            cur,
            """
            SELECT COUNT(*)
            FROM BRONZE.RAW_MEMBER_PROFILES
            WHERE MEMBER_NAME IS NULL OR TRIM(MEMBER_NAME) = ''
            """,
        )
        record_check(
            cur, run_id, "BRONZE", "RAW_MEMBER_NULL_OR_BLANK_NAME",
            "WARN" if null_name_cnt > 0 else "PASS", f"count={null_name_cnt}"
        )

        dup_raw_ids = scalar(
            cur,
            """
            SELECT COUNT(*) FROM (
                SELECT MEMBER_ID
                FROM BRONZE.RAW_MEMBER_PROFILES
                WHERE MEMBER_ID IS NOT NULL AND TRIM(MEMBER_ID) <> ''
                GROUP BY MEMBER_ID
                HAVING COUNT(*) > 1
            )
            """,
        )
        record_check(
            cur, run_id, "BRONZE", "RAW_MEMBER_DUPLICATE_IDS",
            "INFO", f"member_ids_appearing_multiple_times={dup_raw_ids}"
        )

        non_std_countries = scalar(
            cur,
            """
            SELECT COUNT(*)
            FROM BRONZE.RAW_MEMBER_PROFILES
            WHERE COUNTRY IS NULL OR UPPER(TRIM(COUNTRY)) NOT IN ('IND', 'USA', 'AUS')
            """,
        )
        record_check(
            cur, run_id, "BRONZE", "RAW_MEMBER_NON_STANDARD_COUNTRIES",
            "INFO", f"rows_outside_IND_USA_AUS={non_std_countries}"
        )

        q_mem_cnt = scalar(cur, "SELECT COUNT(*) FROM BRONZE.QUARANTINE_MEMBER_PROFILES")
        raw_json_cnt = scalar(cur, "SELECT COUNT(*) FROM BRONZE.RAW_REDEMPTION_JSON")
        flat_json_cnt = scalar(cur, "SELECT COUNT(*) FROM BRONZE.REDEMPTION_JSON_FLAT")
        q_json_cnt = scalar(cur, "SELECT COUNT(*) FROM BRONZE.QUARANTINE_REDEMPTION_JSON")
        record_check(
            cur, run_id, "BRONZE", "BRONZE_QUARANTINE_AND_REDEMPTION_COUNTS",
            "INFO",
            f"quarantined_members={q_mem_cnt}, raw_json={raw_json_cnt}, flat_json={flat_json_cnt}, quarantined_json={q_json_cnt}"
        )

        # --- 2. SILVER CHECKS ---
        ind_cnt = scalar(cur, "SELECT COUNT(*) FROM SILVER.TABLE_INDIA")
        usa_cnt = scalar(cur, "SELECT COUNT(*) FROM SILVER.TABLE_USA")
        aus_cnt = scalar(cur, "SELECT COUNT(*) FROM SILVER.TABLE_AUS")
        record_check(
            cur, run_id, "SILVER", "SILVER_COUNTRY_TABLE_ROW_COUNTS",
            "INFO", f"TABLE_INDIA={ind_cnt}, TABLE_USA={usa_cnt}, TABLE_AUS={aus_cnt}"
        )

        silver_nulls = scalar(
            cur,
            """
            SELECT COUNT(*) FROM (
                SELECT MEMBER_ID, MEMBER_NAME FROM SILVER.TABLE_INDIA
                UNION ALL
                SELECT MEMBER_ID, MEMBER_NAME FROM SILVER.TABLE_USA
                UNION ALL
                SELECT MEMBER_ID, MEMBER_NAME FROM SILVER.TABLE_AUS
            )
            WHERE MEMBER_ID IS NULL OR TRIM(MEMBER_ID) = ''
               OR MEMBER_NAME IS NULL OR TRIM(MEMBER_NAME) = ''
            """,
        )
        record_check(
            cur, run_id, "SILVER", "SILVER_NO_NULL_OR_BLANK_ID_OR_NAME",
            "PASS" if silver_nulls == 0 else "FAIL", f"invalid_null_rows={silver_nulls}"
        )

        cross_country_dups = scalar(
            cur,
            """
            SELECT COUNT(*) FROM (
                SELECT MEMBER_ID
                FROM (
                    SELECT MEMBER_ID FROM SILVER.TABLE_INDIA
                    UNION ALL
                    SELECT MEMBER_ID FROM SILVER.TABLE_USA
                    UNION ALL
                    SELECT MEMBER_ID FROM SILVER.TABLE_AUS
                )
                GROUP BY MEMBER_ID
                HAVING COUNT(*) > 1
            )
            """,
        )
        record_check(
            cur, run_id, "SILVER", "SILVER_NO_DUPLICATE_MEMBER_ID_ACROSS_COUNTRIES",
            "PASS" if cross_country_dups == 0 else "FAIL", f"duplicate_member_ids={cross_country_dups}"
        )

        cur.execute(
            """
            SELECT STALE_MEMBER, COUNT(*)
            FROM (
                SELECT STALE_MEMBER FROM SILVER.TABLE_INDIA
                UNION ALL
                SELECT STALE_MEMBER FROM SILVER.TABLE_USA
                UNION ALL
                SELECT STALE_MEMBER FROM SILVER.TABLE_AUS
            )
            GROUP BY STALE_MEMBER
            ORDER BY STALE_MEMBER
            """
        )
        stale_dist = cur.fetchall()
        record_check(
            cur, run_id, "SILVER", "SILVER_STALE_MEMBER_DISTRIBUTION",
            "INFO", f"stale_counts={stale_dist}"
        )

        redemp_cnt = scalar(cur, "SELECT COUNT(*) FROM SILVER.MEMBER_REDEMPTIONS")
        dup_txns = scalar(
            cur,
            """
            SELECT COUNT(*) FROM (
                SELECT TXN_ID
                FROM SILVER.MEMBER_REDEMPTIONS
                GROUP BY TXN_ID
                HAVING COUNT(*) > 1
            )
            """,
        )
        record_check(
            cur, run_id, "SILVER", "SILVER_REDEMPTIONS_COUNT_AND_UNIQUENESS",
            "PASS" if dup_txns == 0 else "FAIL", f"rows={redemp_cnt}, duplicate_txn_ids={dup_txns}"
        )

        # --- 3. GOLD & AUDIT SUMMARY CHECKS ---
        gold_cnt = scalar(cur, "SELECT COUNT(*) FROM GOLD.MEMBER_LOYALTY_SUMMARY")
        gold_dups = scalar(
            cur,
            """
            SELECT COUNT(*) FROM (
                SELECT MEMBER_ID
                FROM GOLD.MEMBER_LOYALTY_SUMMARY
                GROUP BY MEMBER_ID
                HAVING COUNT(*) > 1
            )
            """,
        )
        record_check(
            cur, run_id, "GOLD", "GOLD_LOYALTY_SUMMARY_UNIQUENESS",
            "PASS" if gold_dups == 0 else "FAIL", f"gold_rows={gold_cnt}, duplicate_member_ids={gold_dups}"
        )

        cur.execute(
            """
            SELECT PIPELINE_RUN_ID, STAGE, STATUS, ROWS_READ, ROWS_WRITTEN, ROWS_QUARANTINED
            FROM AUDIT.PIPELINE_AUDIT
            ORDER BY STARTED_AT DESC
            LIMIT 5
            """
        )
        recent_audit = cur.fetchall()
        record_check(
            cur, run_id, "AUDIT", "RECENT_PIPELINE_AUDIT_RUNS",
            "INFO", f"latest_5_runs={recent_audit}"
        )
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run_checks()