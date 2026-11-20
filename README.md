# SkyPoints Loyalty Data Pipeline

A data pipeline for the **SkyPoints** airline loyalty program built with **Python** and **Snowflake** using a **Medallion Architecture** (`BRONZE`, `SILVER`, `GOLD`) and run tracking (`AUDIT.PIPELINE_AUDIT`).

## Architecture Diagram

```text
+------------------------------------------------------------------------------------+
|                           DAILY SOURCE FEEDS (<name>_<YYYYMMDD>)                    |
|  1. Member Profile Feed (Pipe-delimited |H|/|D| - 11 cols)                         |
|  2. Partner Airline Redemption Feed (JSON: member_id, feed_date, redemptions[])    |
+-----------------------------------------+------------------------------------------+
                                          |
                                          v
+------------------------------------------------------------------------------------+
| FOLDER DATE CHECK & PIPELINE AUDIT TABLE (SNOWFLAKE : SKYPOINTS_WH / SKYPOINTS_DB) |
|  - Compare folder file dates vs. loaded batches; sort chronologically and load     |
|  - AUDIT.PIPELINE_AUDIT: tracks BATCH_ID, BIW_LOAD_DATE, stage, status (RUNNING/   |
|    SUCCESS/FAILED), rows_read, rows_written, rows_quarantined, started_at, ended_at|
+-----------------------------------------+------------------------------------------+
                                          |
                                          v
+------------------------------------------------------------------------------------+
| BRONZE / RAW LAYER (Append-only; BIW_LOAD_DATE = CURRENT_TIMESTAMP() on arrival)   |
|  - BRONZE.RAW_MEMBER_PROFILES (11 raw cols + SOURCE_FILE + BATCH_ID + BIW_LOAD_DATE)|
|  - BRONZE.RAW_REDEMPTION_JSON (VARIANT payload + SOURCE_FILE + BATCH_ID + BIW_LOAD_DATE)|
+--------------------+-------------------------------------------+-------------------+
                     |                                           |
                     | (Chronological BATCH_ID / arrival order)  |
                     v                                           v
+----------------------------------------+    +--------------------------------------+
| DATA QUALITY & BRONZE JSON PROCESSING  |    | BRONZE: FLATTENED & QUARANTINED JSON |
| - Mandatory fields & key uniqueness    |    | - BRONZE.REDEMPTION_JSON_FLAT        |
| - Invalid dates (2021-13-13, bad       |    |   (txn_id, member_id, feed_date_raw, |
|   MMDDYYYY, future dates), null DOB    |    |    txn_date_raw, partner, miles_raw, |
| - Schema / country / JSON anomalies    |    |    status, BATCH_ID, BIW_LOAD_DATE)  |
+-------------------+--------------------+    | - BRONZE.QUARANTINE_REDEMPTION_JSON  |
                    |                         |   (bad JSON / invalid items + reason)|
         (Bad Rows) | (Valid Rows)            +------------------+-------------------+
                    v                                            |
+----------------------------------------+                       |
| BRONZE QUARANTINE TABLE                |                       |
| - BRONZE.QUARANTINE_MEMBER_PROFILES    |                       |
|   (raw cols, REJECT_REASON, BATCH_ID,  |                       |
|    BIW_LOAD_DATE)                      |                       |
+----------------------------------------+                       |
                    |                                            |
                    | (Valid Rows)                               |
                    v                                            |
+----------------------------------------------------------------+                   |
| SILVER: STANDARDIZED MEMBERS & REDEMPTIONS                                         |
| - Normalize schema & parse dates/types safely                                      |
| - Derive AGE (from DATE_OF_BIRTH) & STALE_MEMBER (>90 days since last flight)      |
| - Rank per key ("latest record wins") + BATCH_ID + BIW_LOAD_DATE                   |
| - CDC Type-1 MERGE; mark prior country row IS_ACTIVE='N' on country move           |
| - SILVER.MEMBER_REDEMPTIONS: typed & deduplicated redemptions (on TXN_ID)          |
+-----------------------------------------+------------------------------------------+
                                          |
                                          v
+------------------------------------------------------------------------------------+
| SILVER COUNTRY TARGET TABLES (Routed by Country)                                   |
|  - SILVER.TABLE_INDIA   |   SILVER.TABLE_USA   |   SILVER.TABLE_AUS                 |
+-----------------------------------------+------------------------------------------+
                                          |
                                          v
+------------------------------------------------------------------------------------+
| GOLD LAYER & STREAMLIT DEMO                                                        |
|  - GOLD.MEMBER_LOYALTY_SUMMARY: joins country member profiles + MEMBER_REDEMPTIONS |
|  - Metrics: AGE, STALE_MEMBER, IS_ACTIVE, TOTAL_REDEMPTIONS, TOTAL_MILES_REDEEMED |
|  - Streamlit dashboard for interactive live demo walkthrough                      |
+------------------------------------------------------------------------------------+