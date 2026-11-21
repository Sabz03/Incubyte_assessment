# SkyPoints Loyalty Data Pipeline

End-to-end Snowflake data warehouse pipeline for the **SkyPoints** airline loyalty program, using a medallion architecture (`BRONZE`, `SILVER`, `GOLD`) plus an `AUDIT` schema. It includes local mock data generation, batch ingestion, a stored-procedure transformation, regression checks, and a Streamlit dashboard.

**Tech stack:** Python, Snowflake, Streamlit

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture & Layers](#architecture--layers)
3. [Framework Diagram](#framework-diagram)
4. [Project Structure](#project-structure)
5. [Setup & Run](#setup--run)
6. [Deliverables Map](#deliverables-map)

---

## Overview

Every day the source system delivers two feeds with a `YYYYMMDD` date at the end of the filename stem (for example, `member_profiles20261121.csv` or `member_profiles_20261121.csv`):

| Feed | Format | Contents |
|------|--------|----------|
| Member profile feed | Pipe-delimited, `\|H\|` header / `\|D\|` detail records, 11 columns | Member profile data |
| Partner airline redemption feed | JSON | `member_id`, `feed_date`, `redemptions[]` |

The pipeline loads both feeds into Snowflake, validates them, routes members into **country-specific tables**, applies **"latest record wins"** when a member moves countries, flattens the JSON redemptions, and exposes a Gold summary joining members with their redemptions.

---

## Architecture & Layers

- **Warehouse & Database:** `SKYPOINTS_WH` (`XSMALL`, auto-suspend `60s`, auto-resume enabled) and `SKYPOINTS_DB`.
- **Schemas:** `BRONZE`, `SILVER`, `GOLD`, and `AUDIT`.

| Layer | Objects | Purpose |
|-------|---------|---------|
| **Bronze** | `BRONZE.RAW_MEMBER_PROFILES`, `BRONZE.QUARANTINE_MEMBER_PROFILES` | Raw member rows (append-only) and rejected rows with a reject reason |
| **Bronze** | `BRONZE.RAW_REDEMPTION_JSON`, `BRONZE.REDEMPTION_JSON_FLAT`, `BRONZE.QUARANTINE_REDEMPTION_JSON` | Raw JSON payloads, flattened redemption rows, and bad JSON / invalid items |
| **Silver** | `SILVER.TABLE_INDIA`, `SILVER.TABLE_USA`, `SILVER.TABLE_AUS` | Country-partitioned member tables |
| **Silver** | `SILVER.MEMBER_REDEMPTIONS` | Typed, deduplicated redemption transactions (on `TXN_ID`) |
| **Silver** | `SILVER.SP_RAW_TO_STG()` | Transformation stored procedure |
| **Gold** | `GOLD.MEMBER_LOYALTY_SUMMARY` (view) | Active members (`IS_ACTIVE = 'Y'`) from the three country tables, with `TOTAL_REDEMPTIONS` and `TOTAL_MILES_REDEEMED` aggregated from `SILVER.MEMBER_REDEMPTIONS` |
| **Audit** | `AUDIT.PIPELINE_AUDIT` | Pipeline stage execution tracking |
| **Audit** | `AUDIT.TEST_AUDIT` | Regression / test run results |

Every Bronze table carries `SOURCE_FILE`, `BATCH_ID` and `BIW_LOAD_DATE` (`CURRENT_TIMESTAMP()` on arrival), so each downstream row can be traced to its file and run.

### Key processing rules

- **Folder date check:** validate each filename's trailing `YYYYMMDD`, skip files already recorded by `SOURCE_FILE` in the corresponding Bronze raw/flat/quarantine tables, and process new files chronologically within each feed. Feed filenames should be immutable after ingestion; use a new filename when delivering replacement data.
- **Data quality:** mandatory fields, key uniqueness, invalid dates (for example `2021-13-13`, bad `MMDDYYYY`, future dates), null DOB, and schema / country / JSON anomalies. Bad rows go to quarantine with a reason.
- **Derived columns:** `AGE` (from date of birth) and `STALE_MEMBER` (more than 90 days since last flight).
- **Latest record wins:** rank per key using `BATCH_ID` and `BIW_LOAD_DATE`.
- **CDC Type-1 MERGE:** on a country move, the prior country row is marked `IS_ACTIVE = 'N'`.
- **Redemptions:** JSON flattened, typed, and deduplicated on `TXN_ID`.

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
|              FOLDER DATE CHECK  &  PIPELINE AUDIT  (SKYPOINTS_WH / SKYPOINTS_DB)    |
|  - Validate feed dates, skip loaded SOURCE_FILEs, sort pending files chronologically|
|  - AUDIT.PIPELINE_AUDIT: BATCH_ID, BIW_LOAD_DATE, stage, status                    |
|    (RUNNING / SUCCESS / FAILED), rows_read, rows_written, rows_quarantined,        |
|    started_at, ended_at                                                            |
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
|  - Invalid / future dates, null DOB  |    |    (txn_id, member_id, feed_date_raw,  |
|  - Schema / country anomalies        |    |     txn_date_raw, partner, miles_raw,  |
+-------------+------------------------+    |     status, BATCH_ID, BIW_LOAD_DATE)   |
              |                             |  - BRONZE.QUARANTINE_REDEMPTION_JSON   |
     bad rows | valid rows                  |    (bad JSON / invalid items + reason) |
              v                             +--------------------+-------------------+
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
|                         GOLD LAYER  &  STREAMLIT DEMO                              |
|  - GOLD.MEMBER_LOYALTY_SUMMARY: country member profiles + MEMBER_REDEMPTIONS       |
|  - Metrics: AGE, STALE_MEMBER, IS_ACTIVE, TOTAL_REDEMPTIONS, TOTAL_MILES_REDEEMED  |
|  - Streamlit dashboard for the interactive live demo                               |
+------------------------------------------------------------------------------------+
```

---

## Project Structure

```text
Incubyte_assessment/
├── data/                          # Generated mock feed files
├── logs/                          
├── sql/
│   ├── ddl_tables.sql             # Warehouse, schemas, tables, view, and audit DDL
│   └── sp_raw_to_stg.sql          # SILVER.SP_RAW_TO_STG() transformation procedure
├── src/
│   ├── generate_mock_data.py      # Generates pipe-delimited member CSVs and JSON redemption feeds
│   ├── ingest_to_bronze.py        # Bronze ingestion logic
│   ├── run_pipeline.py            # Generates mock files, ingests to BRONZE, transforms Silver, runs checks
│   └── streamlit_app.py           # Streamlit dashboard querying GOLD.MEMBER_LOYALTY_SUMMARY
├── tests/
│   ├── regression_test.py         # Bronze/Silver/Gold/Audit checks; writes results to AUDIT.TEST_AUDIT
│   └── test_feed_file_selection.py # Unit tests for filename dates, ordering, and skipping loaded files
├── .env                           # Local Snowflake credentials (not committed)
├── .env.example                   # Template for required environment variables
├── .gitignore
└── README.md
```

### Run order at a glance

```text
generate_mock_data.py  ->  ingest_to_bronze.py  ->  SILVER.SP_RAW_TO_STG()  ->  regression_test.py  ->  streamlit_app.py
        (run_pipeline.py chains the first four steps)
```

---

## Setup & Run

> Commands assume the file names in the structure above. Adjust if your entry points take arguments.

```bash
# 1. Configure Snowflake credentials
cp .env.example .env              # then fill in your account, user, password, role

# 2. Create warehouse, schemas, tables, view and audit tables
#    Run sql/ddl_tables.sql and sql/sp_raw_to_stg.sql in a Snowflake worksheet (or via SnowSQL)
#    ddl_tables.sql uses IF NOT EXISTS, so rerunning it preserves existing tables.
#    It will not apply schema changes to tables/views that already exist.

# 3. Run the pipeline: generate files, ingest Bronze, transform Silver, and run regression checks
python src/run_pipeline.py

# 4. Run the focused local unit tests
python -m unittest discover -s tests -p "test_*.py"

# Optional: rerun only the Snowflake regression checks (results go to AUDIT.TEST_AUDIT)
python tests/regression_test.py

# 5. Deploy src/streamlit_app.py as a Streamlit in Snowflake app.
#    It uses get_active_session() and is not intended for ordinary local `streamlit run`.
```

The focused unit tests check supported filename date formats, reject missing or invalid dates, and verify chronological ordering and skipping previously loaded files. They run locally without Snowflake. The separate `regression_test.py` checks live warehouse tables and requires Snowflake credentials.

---

## Deliverables Map

| # | Deliverable | Where it lives |
|---|-------------|----------------|
| 1 | DDL for raw, staging and country target tables | `sql/ddl_tables.sql` |
| 2 | Staging load with derived `AGE` and `STALE_MEMBER` | `sql/sp_raw_to_stg.sql` |
| 3 | Country split with "latest record wins" | `sql/sp_raw_to_stg.sql` |
| 4 | JSON flattening and join to members | `sql/sp_raw_to_stg.sql`, `GOLD.MEMBER_LOYALTY_SUMMARY` |
| 5 | Validations and quarantine | `sql/sp_raw_to_stg.sql`, `src/regression_test.py` |
| 6 | Live demo | `src/streamlit_app.py` |
