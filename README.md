# SkyPoints Loyalty Data Pipeline

End-to-end Snowflake data warehouse pipeline for the **SkyPoints** airline loyalty program, using a medallion architecture (`BRONZE`, `SILVER`, `GOLD`) plus an `AUDIT` schema. It includes local mock data generation, staged batch ingestion, a stored-procedure transformation, data quality / regression checks, and a Streamlit dashboard.

**Tech stack:** Python, Snowflake (Snowpark & Connector), Streamlit

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture & Layers](#architecture--layers)
3. [Framework Diagram](#framework-diagram)
4. [Project Structure](#project-structure)
5. [Run Order](#run-order)
6. [Setup & Run](#setup--run)
7. [Deliverables Map](#deliverables-map)

---

## Overview

Every day the source system delivers two feeds with a `YYYYMMDD` date at the end of the filename stem (for example, `member_profiles20261121.csv` or `member_profiles_20261121.csv`):

| Feed | Format | Contents |
|------|--------|----------|
| Member profile feed | Pipe-delimited, `\|H\|` header / `\|D\|` detail records, 11 columns | Member profile data |
| Partner airline redemption feed | JSON | `member_id`, `feed_date`, `redemptions[]` |

The pipeline stages compressed files into Snowflake internal stages, loads and validates them in Bronze, routes members into **country-specific Silver tables**, applies **"latest record wins"** and **CDC Type-1 country-swap handling** (`IS_ACTIVE = 'Y'` / `'N'`), flattens and deduplicates JSON redemptions, and exposes a Gold summary view joining active members with their redemption metrics.

### Daily volume and mock data

- The mock generator produces **5,000 records per feed per day** by default (5,000 member profile rows and 5,000 JSON redemption records).
- All generated member IDs are **numeric only** (no alphabetic or mixed IDs).
- This assessment implementation has not been benchmarked for billions of records per day; see [Scale considerations](#scale-considerations).

### Input-layout assumption

The implementation follows the field-position table and expects 11 detail fields, including `Post_Code`. The assessment's abbreviated sample detail lines omit `Post_Code`. The current parser does not automatically distinguish these layouts, so use the 11-field format or adapt the parser before loading the abbreviated sample. `MEMBER_ID` is used as the member key; names are not unique identifiers. `POST_CODE` is stored as text to preserve leading zeroes and support alphanumeric postal codes.

---

## Architecture & Layers

- **Warehouse & Database:** `SKYPOINTS_WH` (`XSMALL`, auto-suspend `60s`, auto-resume enabled) and `SKYPOINTS_DB`.
- **Service Role:** `SKYPOINTS_ETL_ROLE` (with database, schema, stage, table, view, and audit privileges; `ACCOUNTADMIN` / app owner role also granted `USAGE` and `SELECT` as needed for SQL and Streamlit execution).
- **Schemas:** `BRONZE`, `SILVER`, `GOLD`, and `AUDIT`.

| Layer | Objects | Purpose |
|-------|---------|---------|
| **Bronze Stages & Formats** | `BRONZE.MEMBER_STAGE`, `BRONZE.REDEMPTION_JSON_STAGE`, `BRONZE.CSV_STAGE_FMT`, `BRONZE.JSON_STAGE_FMT` | Internal Snowflake stages and file formats for staged `PUT` + `COPY INTO ... PURGE = TRUE` ingestion |
| **Bronze** | `BRONZE.RAW_MEMBER_PROFILES`, `BRONZE.QUARANTINE_MEMBER_PROFILES` | Raw member rows (append-only) and rejected rows with a reject reason |
| **Bronze** | `BRONZE.RAW_REDEMPTION_JSON`, `BRONZE.REDEMPTION_JSON_FLAT`, `BRONZE.QUARANTINE_REDEMPTION_JSON` | Raw JSON payloads, flattened redemption rows per batch, and malformed JSON / invalid items |
| **Silver** | `SILVER.TABLE_INDIA`, `SILVER.TABLE_USA`, `SILVER.TABLE_AUS` | Country-partitioned member tables with active/inactive tracking (`IS_ACTIVE`) |
| **Silver** | `SILVER.MEMBER_REDEMPTIONS` | Typed, deduplicated redemption transactions (on `TXN_ID`) |
| **Silver** | `SILVER.SP_RAW_TO_STG()` | Transformation stored procedure |
| **Gold** | `GOLD.MEMBER_LOYALTY_SUMMARY` (view) | Active members (`IS_ACTIVE = 'Y'`) across country tables joined with redemption counts and completed, pending, and reversed miles (`TOTAL_REDEMPTIONS`, `TOTAL_MILES_REDEEMED`, `PENDING_MILES`, `REVERSED_MILES`) |
| **Audit** | `AUDIT.PIPELINE_AUDIT` | Single-entry pipeline stage completion log (`SUCCESS`, `FAILED`, or `CANCELLED`) |
| **Audit** | `AUDIT.TEST_AUDIT` | Data quality & regression test run results |

Every Bronze table carries `SOURCE_FILE`, `BATCH_ID`, and `BIW_LOAD_DATE` (`CURRENT_TIMESTAMP()` on arrival), so each downstream row can be traced to its file and run.

### Key processing rules

- **Staged ingestion:** `ingest_to_bronze.py` writes local gzipped files, uploads via `PUT` to `@BRONZE.MEMBER_STAGE` and `@BRONZE.REDEMPTION_JSON_STAGE`, loads via `COPY INTO ... PURGE = TRUE`, flattens JSON via `LATERAL FLATTEN` into `BRONZE.REDEMPTION_JSON_FLAT`, and routes invalid/malformed JSON items to `BRONZE.QUARANTINE_REDEMPTION_JSON`.
- **Folder date check:** validate each filename's trailing `YYYYMMDD`, skip files already recorded by `SOURCE_FILE` in the corresponding Bronze raw/flat/quarantine tables, and process new files chronologically within each feed. Feed filenames should be immutable after ingestion; use a new filename when delivering replacement data.
- **Single-entry audit logging:** each pipeline stage writes a single summary row (`SUCCESS`, `FAILED`, or `CANCELLED`) to `AUDIT.PIPELINE_AUDIT` upon completion.
- **Data quality:** mandatory fields, key uniqueness, invalid dates (for example `2021-13-13`, bad `MMDDYYYY`, future dates), null DOB, and schema / country / JSON anomalies. Bad rows go to quarantine with a reason.
- **Derived columns:** `AGE` (from date of birth) and `STALE_MEMBER` (more than 90 days since last flight).
- **Latest record wins:** pending files are ordered by the date in their filename, while the Silver procedure ranks records by `BIW_LOAD_DATE` and `BATCH_ID`. A late-arriving older feed can therefore be treated as the latest update; source-effective-date handling is a production enhancement.
- **CDC Type-1 MERGE:** on a country move, the prior country row is marked `IS_ACTIVE = 'N'` and the new country row is active (`IS_ACTIVE = 'Y'`) with the latest `BIW_LOAD_DATE`.
- **Redemptions:** JSON flattened, typed, and deduplicated on `TXN_ID`.

---

## Scale considerations

The current implementation uses useful Snowflake bulk-loading primitives: compressed files, internal stages, `PUT`, `COPY INTO`, and Snowflake-side JSON flattening. That is a solid prototype direction.

- The member parser materializes a file's records in Python, and the JSON loader reads and parses each whole JSON file in memory.
- The Silver procedure ranks rows from Bronze history on each run instead of processing only the new batch.
- The mock data is 5,000 rows per feed; no billion-row throughput, concurrency, or cost benchmark is included.

A production-scale next step would be to use immutable, appropriately sized dated files and Snowflake-managed ingestion; record per-file/batch completion for safe replay; transform only new batches using streams/tasks or explicit batch watermarks; define late-arriving-data rules; and benchmark throughput, concurrency, and cost with representative volumes. These are **design recommendations, not implemented or benchmarked features**.

---

## Framework Diagram

```text
+------------------------------------------------------------------------------------+
|                    DAILY SOURCE FEEDS (filename ends in YYYYMMDD)                   |
|  1. Member Profile Feed       (pipe-delimited |H|/|D|, 11 columns)                  |
|  2. Partner Redemption Feed   (JSON: member_id, feed_date, redemptions[])           |
+-----------------------------------------+------------------------------------------+
                                          |
                                          v
+------------------------------------------------------------------------------------+
|         STAGED INGESTION, FOLDER DATE CHECK & SINGLE-ENTRY PIPELINE AUDIT           |
|  - Validate feed dates, skip loaded SOURCE_FILEs, sort pending files chronologically|
|  - Local gzip -> PUT to @BRONZE.MEMBER_STAGE / @BRONZE.REDEMPTION_JSON_STAGE       |
|  - COPY INTO ... PURGE = TRUE  |  Role: SKYPOINTS_ETL_ROLE (SKYPOINTS_WH/DB)      |
|  - AUDIT.PIPELINE_AUDIT: single completion row (SUCCESS / FAILED / CANCELLED)      |
+-----------------------------------------+------------------------------------------+
                                          |
                                          v
+------------------------------------------------------------------------------------+
|                     BRONZE / RAW LAYER  (append-only)                              |
|  - BRONZE.RAW_MEMBER_PROFILES  (11 raw cols + SOURCE_FILE + BATCH_ID + BIW_LOAD_DATE)|
|  - BRONZE.RAW_REDEMPTION_JSON  (VARIANT payload + SOURCE_FILE + BATCH_ID + ...)    |
+---------------------+--------------------------------------+-----------------------+
                      |                                      |
                      v                                      v
+--------------------------------------+    +----------------------------------------+
|   DATA QUALITY  (members)            |    |   JSON PROCESSING  (redemptions)       |
|  - Mandatory fields, key uniqueness  |    |  - BRONZE.REDEMPTION_JSON_FLAT         |
|  - Invalid / future dates, null DOB  |    |    (LATERAL FLATTEN per batch_id)      |
|  - Schema / country anomalies        |    |  - BRONZE.QUARANTINE_REDEMPTION_JSON   |
+-------------+------------------------+    |    (malformed JSON / invalid items)    |
              |                             +--------------------+-------------------+
     bad rows | valid rows                                       |
              v                                                  |
+--------------------------------------+                         |
|  BRONZE.QUARANTINE_MEMBER_PROFILES   |                         |
|  (raw cols + REJECT_REASON + BATCH_ID|                         |
|   + BIW_LOAD_DATE)                   |                         |
+--------------------------------------+                         |
              | valid rows                                       | valid rows
              v                                                  v
+------------------------------------------------------------------------------------+
|                SILVER:  STANDARDIZED MEMBERS & REDEMPTIONS                         |
|  - Normalize schema; parse dates / types safely                                    |
|  - Derive AGE (from DATE_OF_BIRTH) and STALE_MEMBER (> 90 days since last flight)  |
|  - Rank per key ("latest record wins") using BATCH_ID + BIW_LOAD_DATE              |
|  - CDC Type-1 MERGE; mark prior country row IS_ACTIVE = 'N' on country move        |
|  - SILVER.MEMBER_REDEMPTIONS: typed and deduplicated on TXN_ID                     |
+-----------------------------------------+------------------------------------------+
                                          |
                                          v
+------------------------------------------------------------------------------------+
|                  SILVER COUNTRY TARGET TABLES  (routed by country)                 |
|        SILVER.TABLE_INDIA   |   SILVER.TABLE_USA   |   SILVER.TABLE_AUS           |
+-----------------------------------------+------------------------------------------+
                                          |
                                          v
+------------------------------------------------------------------------------------+
|                  GOLD LAYER, QUALITY CHECKS & STREAMLIT DEMO                       |
|  - GOLD.MEMBER_LOYALTY_SUMMARY: active members (IS_ACTIVE = 'Y') + redemption stats|
|  - Metrics: AGE, STALE_MEMBER, TOTAL_REDEMPTIONS, TOTAL_MILES_REDEEMED,            |
|    PENDING_MILES, REVERSED_MILES                                                   |
|  - Data quality check script & unit tests logging to AUDIT.TEST_AUDIT              |
|  - Streamlit dashboard (Snowpark SiS + local connector fallback)                   |
+------------------------------------------------------------------------------------+
```

---

## Project Structure

```text
Incubyte_assessment/
├── data/                           # Generated mock feed files
├── logs/                           # Pipeline run logs
├── sql/
│   ├── ddl_tables.sql              # Warehouse, role/grants, schemas, stages, file formats, tables, and view
│   └── sp_raw_to_stg.sql           # SILVER.SP_RAW_TO_STG() transformation procedure
├── src/
│   ├── generate_mock_data.py       # Generates pipe-delimited member CSVs and JSON redemption feeds
│   ├── ingest_to_bronze.py         # Gzips feeds, PUTs to Bronze stages, COPY INTO, flattens/quarantines
│   ├── run_pipeline.py             # Generates mock files, ingests Bronze, transforms Silver, runs checks
│   └── streamlit_app.py            # Streamlit dashboard (Snowpark + local fallback, Bronze & STG status)
├── tests/
│   ├── regression_test.py          # Bronze/Silver/Gold/Audit checks (IS_ACTIVE='Y' & swap order); logs to audit
│   └── test_feed_file_selection.py # Unit tests for filename dates, ordering, and skipping loaded files
├── .env                            # Local Snowflake credentials (not committed)
├── .env.example                    # Template for required environment variables
├── .gitignore
└── README.md
```

---

## Run Order

```text
generate_mock_data.py  ->  ingest_to_bronze.py  ->  SILVER.SP_RAW_TO_STG()  ->  regression_test.py  ->  streamlit_app.py
        (run_pipeline.py chains the first four steps)
```

---

## Setup & Run

```bash
# 1. Configure Snowflake credentials and SKYPOINTS_ETL_ROLE privileges
cp .env.example .env              # fill in SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE=SKYPOINTS_ETL_ROLE

# 2. Prepare Snowflake objects and privileges
#    Role/grant statements are in sql/ddl_tables.sql; there is no sql/grants.sql.
#    On a fresh account, create SKYPOINTS_DB and its schemas before running the grant statements,
#    because those statements refer to the database/schemas before the DDL creates them.
#    Replace the <USER_NAME> placeholder and run the DDL with a role allowed to create roles/grant privileges.
#    Ensure the role used by the Python jobs has the required table and stage privileges.
#    ddl_tables.sql preserves existing tables, but replaces stage, file-format, and view definitions.
#    Then run sql/sp_raw_to_stg.sql to create/update the procedure.

# 3. Run the end-to-end pipeline: generate files, stage & ingest Bronze, transform Silver, and run checks
python src/run_pipeline.py

# 4. Run the focused local unit tests
python -m unittest discover -s tests -p "test_*.py"

# Optional: rerun only the Snowflake data quality / regression checks (results go to AUDIT.TEST_AUDIT)
python tests/regression_test.py

# 5. Run or deploy src/streamlit_app.py
#    Supports Snowpark get_active_session() in Streamlit in Snowflake with local connector fallback,
#    explicitly targeting SKYPOINTS_WH and SKYPOINTS_DB.
```

The local unit tests cover filename-date parsing, chronological ordering, and skipping previously loaded files. The Snowflake regression script checks warehouse data and records `PASS`/`FAIL` statuses, but currently does not make a recorded `FAIL` return a failing process exit code; review its output and `AUDIT.TEST_AUDIT`.

---

## Deliverables Map

| # | Deliverable | Where it lives |
|---|-------------|----------------|
| 1 | Service role & DDL for stages, file formats, raw, temporary staging, country, and audit tables | `sql/ddl_tables.sql` (`SKYPOINTS_ETL_ROLE`; grant statements included) |
| 2 | Staged Bronze ingestion (`PUT` + `COPY INTO ... PURGE = TRUE`) & JSON flattening/quarantine | `src/ingest_to_bronze.py` |
| 3 | Staging load with derived `AGE`, `STALE_MEMBER`, latest-record-wins & CDC Type-1 country swaps | `sql/sp_raw_to_stg.sql` |
| 4 | Gold summary view (`GOLD.MEMBER_LOYALTY_SUMMARY`) with completed, pending & reversed miles | `sql/ddl_tables.sql` |
| 5 | Validations, quarantine, unit tests & live data quality / regression audit checks | `tests/test_feed_file_selection.py`, `tests/regression_test.py` (`AUDIT.TEST_AUDIT`) |
| 6 | Interactive dashboard (Snowpark + local connector, Bronze/STG status, Bronze quarantine view) | `src/streamlit_app.py` |
