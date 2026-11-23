from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import re
import sys
import tempfile
import uuid
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

RAW_MEMBER_COLUMNS = [
    "MEMBER_NAME",
    "MEMBER_ID",
    "ENROLLMENT_DATE",
    "LAST_FLIGHT_DATE",
    "TIER_CODE",
    "AGENT_NAME",
    "STATE",
    "COUNTRY",
    "POST_CODE",
    "DATE_OF_BIRTH",
    "ACTIVE_MEMBER",
    "SOURCE_FILE",
    "BATCH_ID",
]

QUARANTINE_MEMBER_COLUMNS = [
    "MEMBER_NAME",
    "MEMBER_ID",
    "ENROLLMENT_DATE",
    "LAST_FLIGHT_DATE",
    "TIER_CODE",
    "AGENT_NAME",
    "STATE",
    "COUNTRY",
    "POST_CODE",
    "DATE_OF_BIRTH",
    "ACTIVE_MEMBER",
    "REJECT_REASON",
    "SOURCE_FILE",
    "BATCH_ID",
]

VALID_COUNTRIES = {"IND", "AUS", "USA"}


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
                if len(data_cols) == 10:
                    ten_field_columns = [
                        "MEMBER_NAME",
                        "MEMBER_ID",
                        "ENROLLMENT_DATE",
                        "LAST_FLIGHT_DATE",
                        "TIER_CODE",
                        "AGENT_NAME",
                        "STATE",
                        "COUNTRY",
                        "DOB",
                        "IS_ACTIVE",
                    ]
                    row = dict(zip(ten_field_columns, data_cols))
                    row["POST_CODE"] = ""
                    records.append(row)
                else:
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


def _put_and_copy_csv(
    cur: Any,
    stage: str,
    file_format: str,
    target_table: str,
    columns: list[str],
    local_gz: Path,
    subpath_prefix: str,
) -> None:
    stage_subpath = f"{stage}/{subpath_prefix}"
    cur.execute(
        f"PUT 'file://{local_gz.as_posix()}' {stage_subpath} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    col_list = ", ".join(columns)
    select_list = ", ".join(f"${idx}" for idx in range(1, len(columns) + 1))
    cur.execute(
        f"""
        COPY INTO {target_table} ({col_list}, BIW_LOAD_DATE)
        FROM (
            SELECT {select_list}, CURRENT_TIMESTAMP()
            FROM {stage_subpath}/{local_gz.name}
        )
        FILE_FORMAT = (FORMAT_NAME = {file_format})
        PURGE = TRUE
        """
    )


def _put_and_copy_raw_json(
    cur: Any,
    stage: str,
    file_format: str,
    payload_rows: list[tuple[str, str, str]],
    subpath_prefix: str,
) -> None:
    """Stages NDJSON rows and COPYs raw JSON payloads into BRONZE.RAW_REDEMPTION_JSON."""
    if not payload_rows:
        return

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_gz = Path(tmp_dir) / f"raw_json_{uuid.uuid4().hex[:8]}.ndjson.gz"
        with gzip.open(local_gz, "wt", encoding="utf-8") as gz_f:
            for payload_str, source_file, batch_id in payload_rows:
                envelope = {
                    "payload": json.loads(payload_str),
                    "source_file": source_file,
                    "batch_id": batch_id,
                }
                gz_f.write(json.dumps(envelope) + "\n")

        stage_subpath = f"{stage}/{subpath_prefix}"
        cur.execute(
            f"PUT 'file://{local_gz.as_posix()}' {stage_subpath} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        cur.execute(
            f"""
            COPY INTO BRONZE.RAW_REDEMPTION_JSON (
                PAYLOAD, SOURCE_FILE, BATCH_ID, BIW_LOAD_DATE
            )
            FROM (
                SELECT
                    $1:payload,
                    $1:source_file::VARCHAR,
                    $1:batch_id::VARCHAR,
                    CURRENT_TIMESTAMP()
                FROM {stage_subpath}/{local_gz.name}
            )
            FILE_FORMAT = (FORMAT_NAME = {file_format})
            PURGE = TRUE
            """
        )


def _put_and_copy_quarantine_json(
    cur: Any,
    stage: str,
    file_format: str,
    quar_rows: list[tuple],
    subpath_prefix: str,
) -> None:
    """Stages NDJSON rows and COPYs into BRONZE.QUARANTINE_REDEMPTION_JSON."""
    if not quar_rows:
        return

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_gz = Path(tmp_dir) / f"quar_json_{uuid.uuid4().hex[:8]}.ndjson.gz"
        with gzip.open(local_gz, "wt", encoding="utf-8") as gz_f:
            for r in quar_rows:
                (
                    raw_json_str,
                    member_id,
                    feed_date_raw,
                    txn_id,
                    txn_date_raw,
                    partner,
                    miles_raw_str,
                    status,
                    reject_reason,
                    source_file,
                    batch_id,
                ) = r
                try:
                    payload_obj: Any = json.loads(raw_json_str)
                except json.JSONDecodeError:
                    payload_obj = {"raw_text": raw_json_str}
                doc = {
                    "raw_payload": payload_obj,
                    "member_id": member_id,
                    "feed_date_raw": feed_date_raw,
                    "txn_id": txn_id,
                    "txn_date_raw": txn_date_raw,
                    "partner": partner,
                    "miles_raw": miles_raw_str,
                    "status": status,
                    "reject_reason": reject_reason,
                    "source_file": source_file,
                    "batch_id": batch_id,
                }
                gz_f.write(json.dumps(doc) + "\n")

        stage_subpath = f"{stage}/{subpath_prefix}"
        cur.execute(
            f"PUT 'file://{local_gz.as_posix()}' {stage_subpath} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        cur.execute(
            f"""
            COPY INTO BRONZE.QUARANTINE_REDEMPTION_JSON (
                RAW_PAYLOAD, MEMBER_ID, FEED_DATE_RAW, TXN_ID, TXN_DATE_RAW,
                PARTNER, MILES_RAW, STATUS, REJECT_REASON, SOURCE_FILE, BATCH_ID, BIW_LOAD_DATE
            )
            FROM (
                SELECT
                    $1:raw_payload,
                    $1:member_id::VARCHAR,
                    $1:feed_date_raw::VARCHAR,
                    $1:txn_id::VARCHAR,
                    $1:txn_date_raw::VARCHAR,
                    $1:partner::VARCHAR,
                    $1:miles_raw::VARCHAR,
                    $1:status::VARCHAR,
                    $1:reject_reason::VARCHAR,
                    $1:source_file::VARCHAR,
                    $1:batch_id::VARCHAR,
                    CURRENT_TIMESTAMP()
                FROM {stage_subpath}/{local_gz.name}
            )
            FILE_FORMAT = (FORMAT_NAME = {file_format})
            PURGE = TRUE
            """
        )


def _flatten_raw_json_to_flat(cur: Any, pipeline_run_id: str) -> int:
    """Flattens newly loaded BRONZE.RAW_REDEMPTION_JSON rows for pipeline_run_id into BRONZE.REDEMPTION_JSON_FLAT."""
    cur.execute(
        """
        INSERT INTO BRONZE.REDEMPTION_JSON_FLAT (
            TXN_ID, MEMBER_ID, FEED_DATE_RAW, TXN_DATE_RAW,
            PARTNER, MILES_RAW, STATUS, SOURCE_FILE, BATCH_ID, BIW_LOAD_DATE
        )
        SELECT
            NULLIF(TRIM(f.value:txn_id::VARCHAR), '')         AS TXN_ID,
            NULLIF(TRIM(r.PAYLOAD:member_id::VARCHAR), '')    AS MEMBER_ID,
            NULLIF(TRIM(r.PAYLOAD:feed_date::VARCHAR), '')    AS FEED_DATE_RAW,
            NULLIF(TRIM(f.value:txn_date::VARCHAR), '')       AS TXN_DATE_RAW,
            NULLIF(TRIM(f.value:partner::VARCHAR), '')        AS PARTNER,
            NULLIF(TRIM(f.value:miles_redeemed::VARCHAR), '') AS MILES_RAW,
            NULLIF(UPPER(TRIM(f.value:status::VARCHAR)), '')  AS STATUS,
            r.SOURCE_FILE,
            r.BATCH_ID,
            CURRENT_TIMESTAMP()
        FROM BRONZE.RAW_REDEMPTION_JSON r,
             LATERAL FLATTEN(input => r.PAYLOAD:redemptions) f
        WHERE r.BATCH_ID = %s
        """,
        (pipeline_run_id,),
    )
    return cur.rowcount or 0


def write_audit_entry(
    conn: Any,
    pipeline_run_id: str,
    stage: str,
    status: str,
    rows_read: int,
    rows_written: int,
    rows_quarantined: int,
    started_at: datetime,
    error_message: str | None = None,
) -> None:
    """Writes a single audit record per pipeline run."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO AUDIT.PIPELINE_AUDIT (
                PIPELINE_RUN_ID, STAGE, STATUS, ROWS_READ, ROWS_WRITTEN, ROWS_QUARANTINED,
                ERROR_MESSAGE, STARTED_AT, ENDED_AT, BIW_LOAD_DATE
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
            """,
            (
                pipeline_run_id,
                stage,
                status,
                rows_read,
                rows_written,
                rows_quarantined,
                error_message[:3900] if error_message else None,
                started_at,
            ),
        )
    conn.commit()


def ingest_member_files(conn: Any, member_files: list[Path], pipeline_run_id: str) -> tuple[int, int, int]:
    if not member_files:
        return 0, 0, 0

    rows_read = 0
    rows_valid = 0
    rows_quarantined = 0
    seen_ids: set[str] = set()

    with tempfile.TemporaryDirectory() as tmp_dir:
        valid_gz = Path(tmp_dir) / f"valid_members_{uuid.uuid4().hex[:8]}.csv.gz"
        quar_gz = Path(tmp_dir) / f"quar_members_{uuid.uuid4().hex[:8]}.csv.gz"

        with (
            gzip.open(valid_gz, "wt", encoding="utf-8", newline="") as f_valid,
            gzip.open(quar_gz, "wt", encoding="utf-8", newline="") as f_quar,
        ):
            w_valid = csv.writer(f_valid)
            w_quar = csv.writer(f_quar)
            w_valid.writerow(RAW_MEMBER_COLUMNS)
            w_quar.writerow(QUARANTINE_MEMBER_COLUMNS)

            for fp in member_files:
                records = parse_member_file(fp)
                for rec in records:
                    rows_read += 1
                    country_norm = (rec["COUNTRY"] or "").strip().upper()
                    tier_norm = (rec["TIER_CODE"] or "").strip().upper()

                    reject_reason = validate_member_row(rec, seen_ids)
                    if reject_reason:
                        rows_quarantined += 1
                        w_quar.writerow(
                            [
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
                            ]
                        )
                    else:
                        rows_valid += 1
                        w_valid.writerow(
                            [
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
                            ]
                        )

        with conn.cursor() as cur:
            if rows_valid > 0:
                _put_and_copy_csv(
                    cur=cur,
                    stage="@BRONZE.MEMBER_STAGE",
                    file_format="BRONZE.CSV_STAGE_FMT",
                    target_table="BRONZE.RAW_MEMBER_PROFILES",
                    columns=RAW_MEMBER_COLUMNS,
                    local_gz=valid_gz,
                    subpath_prefix=f"valid_{pipeline_run_id}",
                )
            if rows_quarantined > 0:
                _put_and_copy_csv(
                    cur=cur,
                    stage="@BRONZE.MEMBER_STAGE",
                    file_format="BRONZE.CSV_STAGE_FMT",
                    target_table="BRONZE.QUARANTINE_MEMBER_PROFILES",
                    columns=QUARANTINE_MEMBER_COLUMNS,
                    local_gz=quar_gz,
                    subpath_prefix=f"quar_{pipeline_run_id}",
                )

    return rows_read, rows_valid, rows_quarantined


def ingest_json_files(conn: Any, json_files: list[Path], pipeline_run_id: str) -> tuple[int, int, int]:
    if not json_files:
        return 0, 0, 0

    payloads_read = 0
    flat_written = 0
    items_quarantined = 0

    valid_raw_json_rows: list[tuple[str, str, str]] = []
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

            valid_items: list[dict[str, Any]] = []
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
                    valid_items.append(item)

            if valid_items:
                clean_payload = dict(rec)
                clean_payload["redemptions"] = valid_items
                valid_raw_json_rows.append((json.dumps(clean_payload), fp.name, pipeline_run_id))

    with conn.cursor() as cur:
        # 1. Stage & load raw JSON into BRONZE.RAW_REDEMPTION_JSON
        _put_and_copy_raw_json(
            cur=cur,
            stage="@BRONZE.REDEMPTION_JSON_STAGE",
            file_format="BRONZE.JSON_STAGE_FMT",
            payload_rows=valid_raw_json_rows,
            subpath_prefix=f"raw_{pipeline_run_id}",
        )

        # 2. Flatten from BRONZE.RAW_REDEMPTION_JSON into BRONZE.REDEMPTION_JSON_FLAT
        if valid_raw_json_rows:
            flat_written = _flatten_raw_json_to_flat(cur, pipeline_run_id)

        # 3. Stage & load quarantined JSON records/items
        _put_and_copy_quarantine_json(
            cur=cur,
            stage="@BRONZE.REDEMPTION_JSON_STAGE",
            file_format="BRONZE.JSON_STAGE_FMT",
            quar_rows=quarantine_json_rows,
            subpath_prefix=f"quar_{pipeline_run_id}",
        )

    return payloads_read, flat_written, items_quarantined


def run_bronze_ingestion(flat_dir: Path, json_dir: Path) -> None:
    started_at = datetime.now()
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

        conn.commit()

        total_read = m_read + j_read
        total_written = m_valid + j_flat
        total_quarantined = m_quar + j_quar

        write_audit_entry(
            conn=conn,
            pipeline_run_id=pipeline_run_id,
            stage="BRONZE_INGEST",
            status="SUCCESS",
            rows_read=total_read,
            rows_written=total_written,
            rows_quarantined=total_quarantined,
            started_at=started_at,
        )

        print(f"Pipeline Run ID: {pipeline_run_id} completed successfully.")
        print(f"Flat CSV ({flat_dir}) -> read: {m_read}, valid: {m_valid}, quarantined: {m_quar}")
        print(
            f"JSON ({json_dir}) -> payloads read: {j_read}, flat written: {j_flat}, "
            f"quarantined items/errors: {j_quar}"
        )

    except KeyboardInterrupt:
        print(f"\n[{pipeline_run_id}] Interrupted by user. Rolling back...")
        conn.rollback()
        try:
            write_audit_entry(
                conn=conn,
                pipeline_run_id=pipeline_run_id,
                stage="BRONZE_INGEST",
                status="CANCELLED",
                rows_read=0,
                rows_written=0,
                rows_quarantined=0,
                started_at=started_at,
                error_message="Interrupted by user",
            )
        except Exception:
            pass
        sys.exit(1)

    except Exception as exc:
        conn.rollback()
        try:
            write_audit_entry(
                conn=conn,
                pipeline_run_id=pipeline_run_id,
                stage="BRONZE_INGEST",
                status="FAILED",
                rows_read=0,
                rows_written=0,
                rows_quarantined=0,
                started_at=started_at,
                error_message=str(exc),
            )
        except Exception:
            pass
        raise
    finally:
        conn.close()


def main() -> None:
    load_dotenv()
    default_flat = Path(os.getenv("FLAT_DATA_FOLDER", "data/flat"))
    default_json = Path(os.getenv("JSON_DATA_FOLDER", "data/json"))

    parser = argparse.ArgumentParser(
        description="Stage and bulk-load CSV member and JSON redemption feeds into Snowflake Bronze."
    )
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