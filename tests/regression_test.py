import os
from datetime import datetime
from zoneinfo import ZoneInfo

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

IST = ZoneInfo("Asia/Kolkata")
AUDIT_SCHEMA = "AUDIT"
TEST_AUDIT_TABLE = "AUDIT.TEST_AUDIT"
WAREHOUSE = (os.getenv("SNOWFLAKE_WAREHOUSE") or "SKYPOINTS_WH").strip()
DATABASE = (os.getenv("SNOWFLAKE_DATABASE") or "SKYPOINTS_DB").strip()


def get_connection():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=WAREHOUSE,
        database=DATABASE,
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
    details_str = str(details)
    cur.execute(
        f"""
        INSERT INTO {TEST_AUDIT_TABLE} (
            TEST_RUN_ID, LAYER, TEST_SCENARIO, STATUS, DETAILS, EXECUTED_AT
        )
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
        """,
        (run_id, layer, scenario, status, details_str),
    )
    print(f"[{status}] [{layer}] {scenario} -> {details_str}")


def scalar(cur, sql: str, params=None):
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None


def run_checks() -> None:
    run_id = f"CHK_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}"
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"USE WAREHOUSE {WAREHOUSE}")
        cur.execute(f"USE DATABASE {DATABASE}")
        ensure_test_audit_table(cur)

        # --- 1. BRONZE CHECKS ---
        raw_mem_cnt = scalar(cur, "SELECT COUNT(*) FROM BRONZE.RAW_MEMBER_PROFILES") or 0
        raw_batches = (
            scalar(cur, "SELECT COUNT(DISTINCT BATCH_ID) FROM BRONZE.RAW_MEMBER_PROFILES") or 0
        )
        record_check(
            cur,
            run_id,
            "BRONZE",
            "RAW_MEMBER_PROFILES_COUNT_AND_BATCHES",
            "INFO",
            f"rows={raw_mem_cnt}, distinct_batches={raw_batches}",
        )

        null_id_cnt = (
            scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM BRONZE.RAW_MEMBER_PROFILES
                WHERE MEMBER_ID IS NULL OR TRIM(MEMBER_ID) = ''
                """,
            )
            or 0
        )
        record_check(
            cur,
            run_id,
            "BRONZE",
            "RAW_MEMBER_NULL_OR_BLANK_ID",
            "WARN" if null_id_cnt > 0 else "PASS",
            f"count={null_id_cnt}",
        )

        null_name_cnt = (
            scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM BRONZE.RAW_MEMBER_PROFILES
                WHERE MEMBER_NAME IS NULL OR TRIM(MEMBER_NAME) = ''
                """,
            )
            or 0
        )
        record_check(
            cur,
            run_id,
            "BRONZE",
            "RAW_MEMBER_NULL_OR_BLANK_NAME",
            "WARN" if null_name_cnt > 0 else "PASS",
            f"count={null_name_cnt}",
        )

        dup_raw_ids = (
            scalar(
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
            or 0
        )
        record_check(
            cur,
            run_id,
            "BRONZE",
            "RAW_MEMBER_DUPLICATE_IDS",
            "INFO",
            f"member_ids_appearing_multiple_times={dup_raw_ids}",
        )

        non_std_countries = (
            scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM BRONZE.RAW_MEMBER_PROFILES
                WHERE COUNTRY IS NULL OR UPPER(TRIM(COUNTRY)) NOT IN ('IND', 'USA', 'AUS')
                """,
            )
            or 0
        )
        record_check(
            cur,
            run_id,
            "BRONZE",
            "RAW_MEMBER_NON_STANDARD_COUNTRIES",
            "INFO",
            f"rows_outside_IND_USA_AUS={non_std_countries}",
        )

        q_mem_cnt = scalar(cur, "SELECT COUNT(*) FROM BRONZE.QUARANTINE_MEMBER_PROFILES") or 0
        raw_json_cnt = scalar(cur, "SELECT COUNT(*) FROM BRONZE.RAW_REDEMPTION_JSON") or 0
        flat_json_cnt = scalar(cur, "SELECT COUNT(*) FROM BRONZE.REDEMPTION_JSON_FLAT") or 0
        q_json_cnt = scalar(cur, "SELECT COUNT(*) FROM BRONZE.QUARANTINE_REDEMPTION_JSON") or 0
        record_check(
            cur,
            run_id,
            "BRONZE",
            "BRONZE_QUARANTINE_AND_REDEMPTION_COUNTS",
            "INFO",
            (
                f"quarantined_members={q_mem_cnt}, raw_json={raw_json_cnt}, "
                f"flat_json={flat_json_cnt}, quarantined_json={q_json_cnt}"
            ),
        )

        # --- 2. SILVER CHECKS ---
        ind_cnt = scalar(cur, "SELECT COUNT(*) FROM SILVER.TABLE_INDIA") or 0
        usa_cnt = scalar(cur, "SELECT COUNT(*) FROM SILVER.TABLE_USA") or 0
        aus_cnt = scalar(cur, "SELECT COUNT(*) FROM SILVER.TABLE_AUS") or 0
        record_check(
            cur,
            run_id,
            "SILVER",
            "SILVER_COUNTRY_TABLE_ROW_COUNTS",
            "INFO",
            f"TABLE_INDIA={ind_cnt}, TABLE_USA={usa_cnt}, TABLE_AUS={aus_cnt}",
        )

        silver_nulls = (
            scalar(
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
            or 0
        )
        record_check(
            cur,
            run_id,
            "SILVER",
            "SILVER_NO_NULL_OR_BLANK_ID_OR_NAME",
            "PASS" if silver_nulls == 0 else "FAIL",
            f"invalid_null_rows={silver_nulls}",
        )

        active_cross_country_dups = (
            scalar(
                cur,
                """
                SELECT COUNT(*) FROM (
                    SELECT MEMBER_ID
                    FROM (
                        SELECT MEMBER_ID FROM SILVER.TABLE_INDIA WHERE IS_ACTIVE = 'Y'
                        UNION ALL
                        SELECT MEMBER_ID FROM SILVER.TABLE_USA WHERE IS_ACTIVE = 'Y'
                        UNION ALL
                        SELECT MEMBER_ID FROM SILVER.TABLE_AUS WHERE IS_ACTIVE = 'Y'
                    )
                    GROUP BY MEMBER_ID
                    HAVING COUNT(*) > 1
                )
                """,
            )
            or 0
        )
        record_check(
            cur,
            run_id,
            "SILVER",
            "SILVER_ACTIVE_NO_DUPLICATE_MEMBER_ID_ACROSS_COUNTRIES",
            "PASS" if active_cross_country_dups == 0 else "FAIL",
            f"active_duplicate_member_ids={active_cross_country_dups}",
        )

        stale_active_rows = (
            scalar(
                cur,
                """
                WITH unioned AS (
                    SELECT MEMBER_ID, IS_ACTIVE, BIW_LOAD_DATE FROM SILVER.TABLE_INDIA
                    UNION ALL
                    SELECT MEMBER_ID, IS_ACTIVE, BIW_LOAD_DATE FROM SILVER.TABLE_USA
                    UNION ALL
                    SELECT MEMBER_ID, IS_ACTIVE, BIW_LOAD_DATE FROM SILVER.TABLE_AUS
                ),
                latest_per_member AS (
                    SELECT
                        MEMBER_ID,
                        MAX(CASE WHEN IS_ACTIVE = 'Y' THEN BIW_LOAD_DATE END) AS ACTIVE_LOAD_DT,
                        MAX(CASE WHEN IS_ACTIVE = 'N' THEN BIW_LOAD_DATE END) AS INACTIVE_LOAD_DT
                    FROM unioned
                    GROUP BY MEMBER_ID
                )
                SELECT COUNT(*)
                FROM latest_per_member
                WHERE ACTIVE_LOAD_DT IS NOT NULL
                  AND INACTIVE_LOAD_DT IS NOT NULL
                  AND ACTIVE_LOAD_DT < INACTIVE_LOAD_DT
                """,
            )
            or 0
        )
        record_check(
            cur,
            run_id,
            "SILVER",
            "SILVER_ACTIVE_ROW_HAS_LATEST_LOAD_DATE",
            "PASS" if stale_active_rows == 0 else "FAIL",
            f"out_of_order_swap_members={stale_active_rows}",
        )

        cur.execute(
            """
            SELECT STALE_MEMBER, COUNT(*)
            FROM (
                SELECT STALE_MEMBER FROM SILVER.TABLE_INDIA WHERE IS_ACTIVE = 'Y'
                UNION ALL
                SELECT STALE_MEMBER FROM SILVER.TABLE_USA WHERE IS_ACTIVE = 'Y'
                UNION ALL
                SELECT STALE_MEMBER FROM SILVER.TABLE_AUS WHERE IS_ACTIVE = 'Y'
            )
            GROUP BY STALE_MEMBER
            ORDER BY STALE_MEMBER
            """
        )
        stale_dist = cur.fetchall()
        record_check(
            cur,
            run_id,
            "SILVER",
            "SILVER_ACTIVE_STALE_MEMBER_DISTRIBUTION",
            "INFO",
            f"stale_counts={stale_dist}",
        )

        redemp_cnt = scalar(cur, "SELECT COUNT(*) FROM SILVER.MEMBER_REDEMPTIONS") or 0
        dup_txns = (
            scalar(
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
            or 0
        )
        record_check(
            cur,
            run_id,
            "SILVER",
            "SILVER_REDEMPTIONS_COUNT_AND_UNIQUENESS",
            "PASS" if dup_txns == 0 else "FAIL",
            f"rows={redemp_cnt}, duplicate_txn_ids={dup_txns}",
        )

        # --- 3. GOLD & AUDIT SUMMARY CHECKS ---
        gold_cnt = scalar(cur, "SELECT COUNT(*) FROM GOLD.MEMBER_LOYALTY_SUMMARY") or 0
        gold_dups = (
            scalar(
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
            or 0
        )
        record_check(
            cur,
            run_id,
            "GOLD",
            "GOLD_LOYALTY_SUMMARY_UNIQUENESS",
            "PASS" if gold_dups == 0 else "FAIL",
            f"gold_rows={gold_cnt}, duplicate_member_ids={gold_dups}",
        )

        negative_gold_miles = (
            scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM GOLD.MEMBER_LOYALTY_SUMMARY
                WHERE TOTAL_REDEMPTIONS < 0
                   OR TOTAL_MILES_REDEEMED < 0
                   OR PENDING_MILES < 0
                   OR REVERSED_MILES < 0
                """,
            )
            or 0
        )
        record_check(
            cur,
            run_id,
            "GOLD",
            "GOLD_NON_NEGATIVE_METRICS",
            "PASS" if negative_gold_miles == 0 else "FAIL",
            f"negative_metric_rows={negative_gold_miles}",
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
            cur,
            run_id,
            "AUDIT",
            "RECENT_PIPELINE_AUDIT_RUNS",
            "INFO",
            f"latest_5_runs={recent_audit}",
        )
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run_checks()