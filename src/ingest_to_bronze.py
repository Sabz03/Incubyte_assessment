import argparse
import json
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import snowflake.connector
from dotenv import load_dotenv


MEMBER_COLUMNS = [
    "MEMBER_NAME",
    "MEMBER_ID",
    "ENROLLMENT_DATE",
    "LAST_FLIGHT_DATE",
    "TIER_CODE",
    "AGENT_NAME",
    "STATE",
    "COUNTRY",
    "POST_CODE",
    "DOB",
    "IS_ACTIVE",
]

VALID_COUNTRIES = {"IND", "AUS", "USA"}
CHUNK_SIZE = 1000


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    load_dotenv()
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "SKYPOINTS_WH")
    database = os.getenv("SNOWFLAKE_DATABASE", "SKYPOINTS_DB")
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=warehouse,
        database=database,
    )
    with conn.cursor() as cur:
        cur.execute(f"USE WAREHOUSE {warehouse}")
        cur.execute(f"USE DATABASE {database}")
    return conn


def make_pipeline_run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def feed_date_from_filename(file_path: Path) -> date:
    match = re.search(r"(\d{8})$", file_path.stem)
    if not match:
        raise ValueError(
            f"Feed filename must end with a YYYYMMDD date before its extension: {file_path.name}"
        )
    try:
        return datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid YYYYMMDD date in feed filename: {file_path.name}") from exc


def sort_feed_files(file_paths: list[Path]) -> list[Path]:
    return sorted(file_paths, key=lambda path: (feed_date_from_filename(path), path.name.lower()))


def unprocessed_feed_files(file_paths: list[Path], loaded_files: set[str]) -> list[Path]:
    return [path for path in file_paths if path.name not in loaded_files]


def loaded_source_files(conn: Any) -> tuple[set[str], set[str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT SOURCE_FILE
            FROM (
                SELECT SOURCE_FILE FROM BRONZE.RAW_MEMBER_PROFILES
                UNION ALL
                SELECT SOURCE_FILE FROM BRONZE.QUARANTINE_MEMBER_PROFILES
            )
            WHERE SOURCE_FILE IS NOT NULL
            """
        )
        member_files = {row[0] for row in cur.fetchall()}

        cur.execute(
            """
            SELECT DISTINCT SOURCE_FILE
            FROM (
                SELECT SOURCE_FILE FROM BRONZE.RAW_REDEMPTION_JSON
                UNION ALL
                SELECT SOURCE_FILE FROM BRONZE.REDEMPTION_JSON_FLAT
                UNION ALL
                SELECT SOURCE_FILE FROM BRONZE.QUARANTINE_REDEMPTION_JSON
            )
            WHERE SOURCE_FILE IS NOT NULL
            """
        )
        redemption_files = {row[0] for row in cur.fetchall()}
    return member_files, redemption_files


def normalize_and_parse_date(raw_val: str | None) -> tuple[date | None, str | None]:
    """
    Handles YYYYMMDD and MMDDYYYY (including 7-digit strings missing a leading zero,
    such as '3051985' -> '03051985'). Rejects malformed or future dates.
    """
    if raw_val is None:
        return None, None
    val = raw_val.strip()
    if val == "":
        return None, None

    if not val.isdigit():
        return None, f"Invalid date format: '{val}'"

    if len(val) == 7:
        val = val.zfill(8)

    if len(val) != 8:
        return None, f"Unexpected date length: '{raw_val}'"

    candidates: list[date] = []
    for fmt in ("%Y%m%d", "%m%d%Y"):
        try:
            dt = datetime.strptime(val, fmt).date()
            if 1900 <= dt.year <= 2100:
                candidates.append(dt)
        except ValueError:
            pass

    if not candidates:
        return None, f"Unparseable calendar date: '{raw_val}'"

    parsed = candidates[0]
    if parsed > date.today():
        return None, f"Future date not allowed: '{parsed.isoformat()}'"

    return parsed, None


def parse_member_file(file_path: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            raw_line = line.strip()
            if not raw_line:
                continue

            trimmed = raw_line.strip("|")
            parts = [p.strip() for p in trimmed.split("|")]
            if not parts:
                continue

            record_type = parts[0].upper()
            if record_type == "H":
                continue

            if record_type == "D":
                data_cols = parts[1:]
                if len(data_cols) < len(MEMBER_COLUMNS):
                    data_cols = data_cols + [""] * (len(MEMBER_COLUMNS) - len(data_cols))
                elif len(data_cols) > len(MEMBER_COLUMNS):
                    data_cols = data_cols[: len(MEMBER_COLUMNS)]
                records.append(dict(zip(MEMBER_COLUMNS, data_cols)))
    return records


def validate_member_row(row_dict: dict[str, str], seen_ids_in_batch: set[str]) -> str | None:
    reasons: list[str] = []

    member_name = (row_dict.get("MEMBER_NAME") or "").strip()
    member_id = (row_dict.get("MEMBER_ID") or "").strip()
    enroll_raw = (row_dict.get("ENROLLMENT_DATE") or "").strip()
    last_flight_raw = (row_dict.get("LAST_FLIGHT_DATE") or "").strip()
    tier_code = (row_dict.get("TIER_CODE") or "").strip().upper()
    dob_raw = (row_dict.get("DOB") or "").strip()
    country = (row_dict.get("COUNTRY") or "").strip().upper()

    if not member_name:
        reasons.append("Missing mandatory MEMBER_NAME")

    if not member_id:
        reasons.append("Missing mandatory MEMBER_ID")
    elif not member_id.isdigit():
        reasons.append(f"Non-numeric MEMBER_ID: '{member_id}'")
    elif len(member_id) < 5:
        reasons.append(f"Short MEMBER_ID: '{member_id}'")
    elif member_id in seen_ids_in_batch:
        reasons.append(f"Duplicate MEMBER_ID in batch: '{member_id}'")

    if not enroll_raw:
        reasons.append("Missing mandatory ENROLLMENT_DATE")
    else:
        _, err = normalize_and_parse_date(enroll_raw)
        if err:
            reasons.append(f"ENROLLMENT_DATE -> {err}")

    if last_flight_raw:
        _, err = normalize_and_parse_date(last_flight_raw)
        if err:
            reasons.append(f"LAST_FLIGHT_DATE -> {err}")

    if tier_code and tier_code not in {"GLD", "SLV", "PLT", "BASE", "PLAT", "GOLD", "SLVR"}:
        reasons.append(f"Unexpected TIER_CODE: '{tier_code}'")

    if dob_raw:
        _, err = normalize_and_parse_date(dob_raw)
        if err:
            reasons.append(f"DOB -> {err}")

    if country not in VALID_COUNTRIES:
        reasons.append(f"Invalid COUNTRY: '{country}' (must be IND, AUS, or USA)")

    if member_id and not reasons:
        seen_ids_in_batch.add(member_id)

    return "; ".join(reasons) if reasons else None


def bulk_insert_variant_rows(
    cur: Any,
    table_name: str,
    columns: list[str],
    rows: list[tuple],
    variant_col_index: int = 0,
) -> None:
    if not rows:
        return

    payload_items: list[dict[str, Any]] = []
    for r in rows:
        item: dict[str, Any] = {}
        for idx, col_name in enumerate(columns):
            val = r[idx]
            if idx == variant_col_index and isinstance(val, str):
                try:
                    val = json.loads(val)
                except json.JSONDecodeError:
                    pass
            item[col_name.lower()] = val
        payload_items.append(item)

    select_cols: list[str] = []
    for col_name in columns:
        key = col_name.lower()
        if col_name.upper() == columns[variant_col_index].upper():
            select_cols.append(f"f.value:{key}")
        else:
            select_cols.append(f"f.value:{key}::VARCHAR")
    select_cols.append("CURRENT_TIMESTAMP()")

    col_list = ", ".join(columns)
    for start in range(0, len(payload_items), CHUNK_SIZE):
        chunk = payload_items[start : start + CHUNK_SIZE]
        sql = (
            f"INSERT INTO {table_name} ({col_list}, BIW_LOAD_DATE) "
            f"SELECT {', '.join(select_cols)} "
            f"FROM LATERAL FLATTEN(input => PARSE_JSON(%s)) f"
        )
        cur.execute(sql, (json.dumps(chunk),))


def insert_audit_start(cur: Any, pipeline_run_id: str, stage: str) -> None:
    cur.execute(
        """
        INSERT INTO AUDIT.PIPELINE_AUDIT (
            PIPELINE_RUN_ID, STAGE, STATUS, ROWS_READ, ROWS_WRITTEN, ROWS_QUARANTINED,
            ERROR_MESSAGE, STARTED_AT, BIW_LOAD_DATE
        )
        VALUES (%s, %s, 'RUNNING', 0, 0, 0, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """,
        (pipeline_run_id, stage),
    )


def insert_audit_finish(
    cur: Any,
    pipeline_run_id: str,
    stage: str,
    status: str,
    rows_read: int,
    rows_written: int,
    rows_quarantined: int,
    error_message: str | None = None,
) -> None:
    cur.execute(
        """
        INSERT INTO AUDIT.PIPELINE_AUDIT (
            PIPELINE_RUN_ID, STAGE, STATUS, ROWS_READ, ROWS_WRITTEN, ROWS_QUARANTINED,
            ERROR_MESSAGE, STARTED_AT, ENDED_AT, BIW_LOAD_DATE
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """,
        (
            pipeline_run_id,
            stage,
            status,
            rows_read,
            rows_written,
            rows_quarantined,
            error_message[:3900] if error_message else None,
        ),
    )


def ingest_member_files(conn: Any, member_files: list[Path], pipeline_run_id: str) -> tuple[int, int, int]:
    rows_read = 0
    rows_valid = 0
    rows_quarantined = 0
    seen_ids: set[str] = set()

    valid_raw_rows: list[tuple] = []
    quarantine_rows: list[tuple] = []

    for fp in member_files:
        records = parse_member_file(fp)
        for rec in records:
            rows_read += 1
            country_norm = (rec["COUNTRY"] or "").strip().upper()
            tier_norm = (rec["TIER_CODE"] or "").strip().upper()

            reject_reason = validate_member_row(rec, seen_ids)
            if reject_reason:
                rows_quarantined += 1
                quarantine_rows.append(
                    (
                        rec["MEMBER_NAME"],
                        rec["MEMBER_ID"],
                        rec["ENROLLMENT_DATE"],
                        rec["LAST_FLIGHT_DATE"],
                        tier_norm,
                        rec["AGENT_NAME"],
                        rec["STATE"],
                        country_norm,
                        rec["POST_CODE"],
                        rec["DOB"],
                        rec["IS_ACTIVE"],
                        reject_reason,
                        fp.name,
                        pipeline_run_id,
                    )
                )
            else:
                rows_valid += 1
                valid_raw_rows.append(
                    (
                        rec["MEMBER_NAME"],
                        rec["MEMBER_ID"],
                        rec["ENROLLMENT_DATE"],
                        rec["LAST_FLIGHT_DATE"],
                        tier_norm,
                        rec["AGENT_NAME"],
                        rec["STATE"],
                        country_norm,
                        rec["POST_CODE"],
                        rec["DOB"],
                        rec["IS_ACTIVE"],
                        fp.name,
                        pipeline_run_id,
                    )
                )

    with conn.cursor() as cur:
        for i in range(0, len(valid_raw_rows), CHUNK_SIZE):
            cur.executemany(
                """
                INSERT INTO BRONZE.RAW_MEMBER_PROFILES (
                    MEMBER_NAME, MEMBER_ID, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
                    TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DATE_OF_BIRTH,
                    ACTIVE_MEMBER, SOURCE_FILE, BATCH_ID, BIW_LOAD_DATE
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                """,
                valid_raw_rows[i : i + CHUNK_SIZE],
            )

        for i in range(0, len(quarantine_rows), CHUNK_SIZE):
            cur.executemany(
                """
                INSERT INTO BRONZE.QUARANTINE_MEMBER_PROFILES (
                    MEMBER_NAME, MEMBER_ID, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
                    TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DATE_OF_BIRTH,
                    ACTIVE_MEMBER, REJECT_REASON, SOURCE_FILE, BATCH_ID, BIW_LOAD_DATE
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                """,
                quarantine_rows[i : i + CHUNK_SIZE],
            )

    return rows_read, rows_valid, rows_quarantined


def ingest_json_files(conn: Any, json_files: list[Path], pipeline_run_id: str) -> tuple[int, int, int]:
    payloads_read = 0
    flat_written = 0
    items_quarantined = 0

    valid_raw_json_rows: list[tuple] = []
    flat_rows: list[tuple] = []
    quarantine_json_rows: list[tuple] = []

    for fp in json_files:
        text = fp.read_text(encoding="utf-8")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            payloads_read += 1
            items_quarantined += 1
            quarantine_json_rows.append(
                (
                    json.dumps({"raw_text": text[:2000]}),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    f"Malformed JSON file: {exc}",
                    fp.name,
                    pipeline_run_id,
                )
            )
            continue

        records = parsed if isinstance(parsed, list) else [parsed]
        for rec in records:
            payloads_read += 1
            raw_json_str = json.dumps(rec)

            if not isinstance(rec, dict):
                items_quarantined += 1
                quarantine_json_rows.append(
                    (
                        raw_json_str,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        "Payload is not a JSON object",
                        fp.name,
                        pipeline_run_id,
                    )
                )
                continue

            member_id = str(rec.get("member_id") or "").strip()
            feed_date_raw = str(rec.get("feed_date") or "").strip()
            redemptions = rec.get("redemptions")

            if not member_id:
                items_quarantined += 1
                quarantine_json_rows.append(
                    (
                        raw_json_str,
                        None,
                        feed_date_raw or None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        "Missing mandatory member_id",
                        fp.name,
                        pipeline_run_id,
                    )
                )
                continue

            if not member_id.isdigit() or len(member_id) < 5:
                items_quarantined += 1
                quarantine_json_rows.append(
                    (
                        raw_json_str,
                        member_id,
                        feed_date_raw or None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        f"Invalid member_id: '{member_id}'",
                        fp.name,
                        pipeline_run_id,
                    )
                )
                continue

            if not isinstance(redemptions, list):
                items_quarantined += 1
                quarantine_json_rows.append(
                    (
                        raw_json_str,
                        member_id,
                        feed_date_raw or None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        "Missing or non-list redemptions array",
                        fp.name,
                        pipeline_run_id,
                    )
                )
                continue

            record_has_valid_item = False
            for item in redemptions:
                if not isinstance(item, dict):
                    items_quarantined += 1
                    quarantine_json_rows.append(
                        (
                            raw_json_str,
                            member_id,
                            feed_date_raw or None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            "Redemption item is not an object",
                            fp.name,
                            pipeline_run_id,
                        )
                    )
                    continue

                txn_id = str(item.get("txn_id") or "").strip()
                txn_date_raw = str(item.get("txn_date") or "").strip()
                partner = str(item.get("partner") or "").strip()
                miles_raw_val = item.get("miles_redeemed")
                miles_raw_str = "" if miles_raw_val is None else str(miles_raw_val).strip()
                status = str(item.get("status") or "").strip()

                item_errors: list[str] = []
                if not txn_id:
                    item_errors.append("Missing txn_id")
                if txn_date_raw:
                    _, dt_err = normalize_and_parse_date(txn_date_raw)
                    if dt_err:
                        item_errors.append(f"txn_date -> {dt_err}")

                try:
                    miles_num = float(miles_raw_str)
                    if miles_num <= 0:
                        item_errors.append("miles_redeemed must be > 0")
                except ValueError:
                    item_errors.append(f"Invalid miles_redeemed: '{miles_raw_str}'")

                if item_errors:
                    items_quarantined += 1
                    quarantine_json_rows.append(
                        (
                            raw_json_str,
                            member_id,
                            feed_date_raw or None,
                            txn_id or None,
                            txn_date_raw or None,
                            partner or None,
                            miles_raw_str or None,
                            status or None,
                            "; ".join(item_errors),
                            fp.name,
                            pipeline_run_id,
                        )
                    )
                else:
                    flat_written += 1
                    record_has_valid_item = True
                    flat_rows.append(
                        (
                            txn_id,
                            member_id,
                            feed_date_raw or None,
                            txn_date_raw or None,
                            partner or None,
                            miles_raw_str or None,
                            status or None,
                            fp.name,
                            pipeline_run_id,
                        )
                    )

            if record_has_valid_item:
                valid_raw_json_rows.append((raw_json_str, fp.name, pipeline_run_id))

    with conn.cursor() as cur:
        bulk_insert_variant_rows(
            cur=cur,
            table_name="BRONZE.RAW_REDEMPTION_JSON",
            columns=["PAYLOAD", "SOURCE_FILE", "BATCH_ID"],
            rows=valid_raw_json_rows,
            variant_col_index=0,
        )

        for i in range(0, len(flat_rows), CHUNK_SIZE):
            cur.executemany(
                """
                INSERT INTO BRONZE.REDEMPTION_JSON_FLAT (
                    TXN_ID, MEMBER_ID, FEED_DATE_RAW, TXN_DATE_RAW,
                    PARTNER, MILES_RAW, STATUS, SOURCE_FILE, BATCH_ID, BIW_LOAD_DATE
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                """,
                flat_rows[i : i + CHUNK_SIZE],
            )

        bulk_insert_variant_rows(
            cur=cur,
            table_name="BRONZE.QUARANTINE_REDEMPTION_JSON",
            columns=[
                "RAW_PAYLOAD",
                "MEMBER_ID",
                "FEED_DATE_RAW",
                "TXN_ID",
                "TXN_DATE_RAW",
                "PARTNER",
                "MILES_RAW",
                "STATUS",
                "REJECT_REASON",
                "SOURCE_FILE",
                "BATCH_ID",
            ],
            rows=quarantine_json_rows,
            variant_col_index=0,
        )

    return payloads_read, flat_written, items_quarantined


def run_bronze_ingestion(flat_dir: Path, json_dir: Path) -> None:
    pipeline_run_id = make_pipeline_run_id()
    member_files = (
        [
            p
            for p in flat_dir.iterdir()
            if p.is_file() and p.suffix.lower() in {".csv", ".txt", ".dat", ".psv"}
        ]
        if flat_dir.exists()
        else []
    )
    json_files = (
        [p for p in json_dir.iterdir() if p.is_file() and p.suffix.lower() == ".json"]
        if json_dir.exists()
        else []
    )
    member_files = sort_feed_files(member_files)
    json_files = sort_feed_files(json_files)

    print(f"[{pipeline_run_id}] Connecting to Snowflake...")
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            insert_audit_start(cur, pipeline_run_id, "BRONZE_INGEST")
        conn.commit()

        loaded_members, loaded_redemptions = loaded_source_files(conn)
        pending_member_files = unprocessed_feed_files(member_files, loaded_members)
        pending_json_files = unprocessed_feed_files(json_files, loaded_redemptions)
        skipped_members = len(member_files) - len(pending_member_files)
        skipped_json = len(json_files) - len(pending_json_files)

        print(
            f"[{pipeline_run_id}] Skipping already loaded files: "
            f"{skipped_members} member, {skipped_json} redemption."
        )
        print(f"[{pipeline_run_id}] Processing {len(pending_member_files)} member file(s)...")
        m_read, m_valid, m_quar = ingest_member_files(conn, pending_member_files, pipeline_run_id)

        print(f"[{pipeline_run_id}] Processing {len(pending_json_files)} JSON file(s)...")
        j_read, j_flat, j_quar = ingest_json_files(conn, pending_json_files, pipeline_run_id)

        total_read = m_read + j_read
        total_written = m_valid + j_flat
        total_quarantined = m_quar + j_quar

        with conn.cursor() as cur:
            insert_audit_finish(
                cur,
                pipeline_run_id,
                "BRONZE_INGEST",
                "SUCCESS",
                total_read,
                total_written,
                total_quarantined,
            )
        conn.commit()

        print(f"Pipeline Run ID: {pipeline_run_id} completed successfully.")
        print(f"Flat CSV ({flat_dir}) -> read: {m_read}, valid: {m_valid}, quarantined: {m_quar}")
        print(f"JSON ({json_dir}) -> payloads read: {j_read}, flat written: {j_flat}, quarantined items/errors: {j_quar}")

    except KeyboardInterrupt:
        print(f"\n[{pipeline_run_id}] Interrupted by user. Rolling back...")
        conn.rollback()
        try:
            with conn.cursor() as cur:
                insert_audit_finish(
                    cur,
                    pipeline_run_id,
                    "BRONZE_INGEST",
                    "CANCELLED",
                    0,
                    0,
                    0,
                    error_message="Interrupted by user",
                )
            conn.commit()
        except Exception:
            pass
        sys.exit(1)

    except Exception as exc:
        conn.rollback()
        try:
            with conn.cursor() as cur:
                insert_audit_finish(
                    cur,
                    pipeline_run_id,
                    "BRONZE_INGEST",
                    "FAILED",
                    0,
                    0,
                    0,
                    error_message=str(exc),
                )
            conn.commit()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def main() -> None:
    load_dotenv()
    default_flat = Path(os.getenv("FLAT_DATA_FOLDER", "data/flat"))
    default_json = Path(os.getenv("JSON_DATA_FOLDER", "data/json"))

    parser = argparse.ArgumentParser(description="Fetch CSV member and JSON redemption feeds into Snowflake Bronze.")
    parser.add_argument(
        "--flat-dir",
        type=Path,
        default=default_flat,
        help="Folder to fetch member CSV files from (defaults to FLAT_DATA_FOLDER in .env).",
    )
    parser.add_argument(
        "--json-dir",
        type=Path,
        default=default_json,
        help="Folder to fetch JSON redemption files from (defaults to JSON_DATA_FOLDER in .env).",
    )
    args = parser.parse_args()
    run_bronze_ingestion(args.flat_dir, args.json_dir)


if __name__ == "__main__":
    main()