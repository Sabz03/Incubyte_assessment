USE WAREHOUSE SKYPOINTS_WH;
USE DATABASE SKYPOINTS_DB;

CREATE OR REPLACE PROCEDURE SILVER.SP_RAW_TO_STG()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id          VARCHAR;
    v_rows_read       NUMBER(38, 0) := 0;
    v_rows_written    NUMBER(38, 0) := 0;
    v_ind_count       NUMBER(38, 0) := 0;
    v_usa_count       NUMBER(38, 0) := 0;
    v_aus_count       NUMBER(38, 0) := 0;
    v_redemp_count    NUMBER(38, 0) := 0;
BEGIN
    v_run_id := TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');

    INSERT INTO AUDIT.PIPELINE_AUDIT (
        PIPELINE_RUN_ID, STAGE, STATUS, ROWS_READ, ROWS_WRITTEN, ROWS_QUARANTINED,
        ERROR_MESSAGE, STARTED_AT, BIW_LOAD_DATE
    )
    VALUES (
        :v_run_id, 'RAW_TO_STG', 'RUNNING', 0, 0, 0,
        NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

    v_rows_read :=
        (SELECT COUNT(*) FROM BRONZE.RAW_MEMBER_PROFILES) +
        (SELECT COUNT(*) FROM BRONZE.REDEMPTION_JSON_FLAT);

    -- ------------------------------------------------------------------------
    -- 1. FULL SCAN & DEDUPLICATE MEMBER PROFILES (AGE + STALE_MEMBER > 90 DAYS)
    -- ------------------------------------------------------------------------
    CREATE OR REPLACE TEMPORARY TABLE TEMP_CLEAN_MEMBERS AS
    WITH parsed_dates AS (
        SELECT
            TRIM(MEMBER_NAME) AS MEMBER_NAME,
            TRIM(MEMBER_ID)   AS MEMBER_ID,
            COALESCE(
                TRY_TO_DATE(TRIM(ENROLLMENT_DATE), 'YYYYMMDD'),
                TRY_TO_DATE(LPAD(TRIM(ENROLLMENT_DATE), 8, '0'), 'MMDDYYYY')
            ) AS ENROLLMENT_DATE,
            COALESCE(
                TRY_TO_DATE(TRIM(LAST_FLIGHT_DATE), 'YYYYMMDD'),
                TRY_TO_DATE(LPAD(TRIM(LAST_FLIGHT_DATE), 8, '0'), 'MMDDYYYY')
            ) AS LAST_FLIGHT_DATE,
            UPPER(TRIM(TIER_CODE)) AS TIER_CODE,
            TRIM(AGENT_NAME)       AS AGENT_NAME,
            UPPER(TRIM(STATE))     AS STATE,
            UPPER(TRIM(COUNTRY))   AS COUNTRY,
            TRIM(POST_CODE)        AS POST_CODE,
            COALESCE(
                TRY_TO_DATE(TRIM(DATE_OF_BIRTH), 'YYYYMMDD'),
                TRY_TO_DATE(LPAD(TRIM(DATE_OF_BIRTH), 8, '0'), 'MMDDYYYY')
            ) AS DATE_OF_BIRTH,
            UPPER(TRIM(ACTIVE_MEMBER)) AS ACTIVE_MEMBER,
            BATCH_ID,
            BIW_LOAD_DATE,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(MEMBER_ID)
                ORDER BY BIW_LOAD_DATE DESC, BATCH_ID DESC
            ) AS rn
        FROM BRONZE.RAW_MEMBER_PROFILES
        WHERE TRIM(MEMBER_ID) IS NOT NULL
          AND TRIM(MEMBER_ID) <> ''
          AND UPPER(TRIM(COUNTRY)) IN ('IND', 'USA', 'AUS')
    ),
    enriched AS (
        SELECT
            MEMBER_NAME,
            MEMBER_ID,
            ENROLLMENT_DATE,
            LAST_FLIGHT_DATE,
            TIER_CODE,
            AGENT_NAME,
            STATE,
            COUNTRY,
            POST_CODE,
            DATE_OF_BIRTH,
            CASE
                WHEN DATE_OF_BIRTH IS NULL THEN NULL
                ELSE FLOOR(DATEDIFF('day', DATE_OF_BIRTH, CURRENT_DATE()) / 365.25)::NUMBER(3, 0)
            END AS AGE,
            ACTIVE_MEMBER,
            CASE
                WHEN LAST_FLIGHT_DATE IS NULL
                  OR DATEDIFF('day', LAST_FLIGHT_DATE, CURRENT_DATE()) > 90
                THEN 'Y'
                ELSE 'N'
            END AS STALE_MEMBER,
            CASE
                WHEN ACTIVE_MEMBER = 'I' THEN 'N'
                ELSE 'Y'
            END AS IS_ACTIVE,
            :v_run_id AS BATCH_ID,
            BIW_LOAD_DATE
        FROM parsed_dates
        WHERE rn = 1
          AND ENROLLMENT_DATE IS NOT NULL
    )
    SELECT * FROM enriched;

    -- Deactivate member in prior country tables when their latest COUNTRY changed
    UPDATE SILVER.TABLE_INDIA tgt
    SET tgt.IS_ACTIVE = 'N',
        tgt.BATCH_ID = :v_run_id,
        tgt.UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE tgt.IS_ACTIVE = 'Y'
      AND EXISTS (
          SELECT 1 FROM TEMP_CLEAN_MEMBERS src
          WHERE src.MEMBER_ID = tgt.MEMBER_ID AND src.COUNTRY <> 'IND'
      );

    UPDATE SILVER.TABLE_USA tgt
    SET tgt.IS_ACTIVE = 'N',
        tgt.BATCH_ID = :v_run_id,
        tgt.UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE tgt.IS_ACTIVE = 'Y'
      AND EXISTS (
          SELECT 1 FROM TEMP_CLEAN_MEMBERS src
          WHERE src.MEMBER_ID = tgt.MEMBER_ID AND src.COUNTRY <> 'USA'
      );

    UPDATE SILVER.TABLE_AUS tgt
    SET tgt.IS_ACTIVE = 'N',
        tgt.BATCH_ID = :v_run_id,
        tgt.UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE tgt.IS_ACTIVE = 'Y'
      AND EXISTS (
          SELECT 1 FROM TEMP_CLEAN_MEMBERS src
          WHERE src.MEMBER_ID = tgt.MEMBER_ID AND src.COUNTRY <> 'AUS'
      );

    -- Full merge into SILVER.TABLE_INDIA
    MERGE INTO SILVER.TABLE_INDIA tgt
    USING (SELECT * FROM TEMP_CLEAN_MEMBERS WHERE COUNTRY = 'IND') src
    ON tgt.MEMBER_ID = src.MEMBER_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.MEMBER_NAME      = src.MEMBER_NAME,
        tgt.ENROLLMENT_DATE  = src.ENROLLMENT_DATE,
        tgt.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        tgt.TIER_CODE        = src.TIER_CODE,
        tgt.AGENT_NAME       = src.AGENT_NAME,
        tgt.STATE            = src.STATE,
        tgt.COUNTRY          = src.COUNTRY,
        tgt.POST_CODE        = src.POST_CODE,
        tgt.DATE_OF_BIRTH    = src.DATE_OF_BIRTH,
        tgt.ACTIVE_MEMBER    = src.ACTIVE_MEMBER,
        tgt.AGE              = src.AGE,
        tgt.STALE_MEMBER     = src.STALE_MEMBER,
        tgt.IS_ACTIVE        = src.IS_ACTIVE,
        tgt.BATCH_ID         = src.BATCH_ID,
        tgt.BIW_LOAD_DATE    = src.BIW_LOAD_DATE,
        tgt.UPDATED_AT       = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        MEMBER_NAME, MEMBER_ID, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
        TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DATE_OF_BIRTH,
        ACTIVE_MEMBER, AGE, STALE_MEMBER, IS_ACTIVE,
        BATCH_ID, BIW_LOAD_DATE, UPDATED_AT
    ) VALUES (
        src.MEMBER_NAME, src.MEMBER_ID, src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE,
        src.TIER_CODE, src.AGENT_NAME, src.STATE, src.COUNTRY, src.POST_CODE, src.DATE_OF_BIRTH,
        src.ACTIVE_MEMBER, src.AGE, src.STALE_MEMBER, src.IS_ACTIVE,
        src.BATCH_ID, src.BIW_LOAD_DATE, CURRENT_TIMESTAMP()
    );

    -- Full merge into SILVER.TABLE_USA
    MERGE INTO SILVER.TABLE_USA tgt
    USING (SELECT * FROM TEMP_CLEAN_MEMBERS WHERE COUNTRY = 'USA') src
    ON tgt.MEMBER_ID = src.MEMBER_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.MEMBER_NAME      = src.MEMBER_NAME,
        tgt.ENROLLMENT_DATE  = src.ENROLLMENT_DATE,
        tgt.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        tgt.TIER_CODE        = src.TIER_CODE,
        tgt.AGENT_NAME       = src.AGENT_NAME,
        tgt.STATE            = src.STATE,
        tgt.COUNTRY          = src.COUNTRY,
        tgt.POST_CODE        = src.POST_CODE,
        tgt.DATE_OF_BIRTH    = src.DATE_OF_BIRTH,
        tgt.ACTIVE_MEMBER    = src.ACTIVE_MEMBER,
        tgt.AGE              = src.AGE,
        tgt.STALE_MEMBER     = src.STALE_MEMBER,
        tgt.IS_ACTIVE        = src.IS_ACTIVE,
        tgt.BATCH_ID         = src.BATCH_ID,
        tgt.BIW_LOAD_DATE    = src.BIW_LOAD_DATE,
        tgt.UPDATED_AT       = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        MEMBER_NAME, MEMBER_ID, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
        TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DATE_OF_BIRTH,
        ACTIVE_MEMBER, AGE, STALE_MEMBER, IS_ACTIVE,
        BATCH_ID, BIW_LOAD_DATE, UPDATED_AT
    ) VALUES (
        src.MEMBER_NAME, src.MEMBER_ID, src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE,
        src.TIER_CODE, src.AGENT_NAME, src.STATE, src.COUNTRY, src.POST_CODE, src.DATE_OF_BIRTH,
        src.ACTIVE_MEMBER, src.AGE, src.STALE_MEMBER, src.IS_ACTIVE,
        src.BATCH_ID, src.BIW_LOAD_DATE, CURRENT_TIMESTAMP()
    );

    -- Full merge into SILVER.TABLE_AUS
    MERGE INTO SILVER.TABLE_AUS tgt
    USING (SELECT * FROM TEMP_CLEAN_MEMBERS WHERE COUNTRY = 'AUS') src
    ON tgt.MEMBER_ID = src.MEMBER_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.MEMBER_NAME      = src.MEMBER_NAME,
        tgt.ENROLLMENT_DATE  = src.ENROLLMENT_DATE,
        tgt.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
        tgt.TIER_CODE        = src.TIER_CODE,
        tgt.AGENT_NAME       = src.AGENT_NAME,
        tgt.STATE            = src.STATE,
        tgt.COUNTRY          = src.COUNTRY,
        tgt.POST_CODE        = src.POST_CODE,
        tgt.DATE_OF_BIRTH    = src.DATE_OF_BIRTH,
        tgt.ACTIVE_MEMBER    = src.ACTIVE_MEMBER,
        tgt.AGE              = src.AGE,
        tgt.STALE_MEMBER     = src.STALE_MEMBER,
        tgt.IS_ACTIVE        = src.IS_ACTIVE,
        tgt.BATCH_ID         = src.BATCH_ID,
        tgt.BIW_LOAD_DATE    = src.BIW_LOAD_DATE,
        tgt.UPDATED_AT       = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        MEMBER_NAME, MEMBER_ID, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
        TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DATE_OF_BIRTH,
        ACTIVE_MEMBER, AGE, STALE_MEMBER, IS_ACTIVE,
        BATCH_ID, BIW_LOAD_DATE, UPDATED_AT
    ) VALUES (
        src.MEMBER_NAME, src.MEMBER_ID, src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE,
        src.TIER_CODE, src.AGENT_NAME, src.STATE, src.COUNTRY, src.POST_CODE, src.DATE_OF_BIRTH,
        src.ACTIVE_MEMBER, src.AGE, src.STALE_MEMBER, src.IS_ACTIVE,
        src.BATCH_ID, src.BIW_LOAD_DATE, CURRENT_TIMESTAMP()
    );

    -- ------------------------------------------------------------------------
    -- 2. FULL SCAN & MERGE REDEMPTIONS -> SILVER.MEMBER_REDEMPTIONS
    -- ------------------------------------------------------------------------
    MERGE INTO SILVER.MEMBER_REDEMPTIONS tgt
    USING (
        WITH ranked_txns AS (
            SELECT
                TRIM(TXN_ID)    AS TXN_ID,
                TRIM(MEMBER_ID) AS MEMBER_ID,
                COALESCE(
                    TRY_TO_DATE(TRIM(FEED_DATE_RAW), 'YYYYMMDD'),
                    TRY_TO_DATE(LPAD(TRIM(FEED_DATE_RAW), 8, '0'), 'MMDDYYYY')
                ) AS FEED_DATE,
                COALESCE(
                    TRY_TO_DATE(TRIM(TXN_DATE_RAW), 'YYYYMMDD'),
                    TRY_TO_DATE(LPAD(TRIM(TXN_DATE_RAW), 8, '0'), 'MMDDYYYY')
                ) AS TXN_DATE,
                TRIM(PARTNER) AS PARTNER,
                TRY_TO_DECIMAL(TRIM(MILES_RAW), 18, 2) AS MILES_REDEEMED,
                UPPER(TRIM(STATUS)) AS STATUS,
                SOURCE_FILE,
                :v_run_id AS BATCH_ID,
                BIW_LOAD_DATE,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(TXN_ID)
                    ORDER BY BIW_LOAD_DATE DESC, BATCH_ID DESC
                ) AS rn
            FROM BRONZE.REDEMPTION_JSON_FLAT
            WHERE TRIM(TXN_ID) IS NOT NULL
              AND TRIM(TXN_ID) <> ''
              AND TRIM(MEMBER_ID) IS NOT NULL
              AND TRIM(MEMBER_ID) <> ''
        )
        SELECT
            TXN_ID,
            MEMBER_ID,
            FEED_DATE,
            TXN_DATE,
            PARTNER,
            MILES_REDEEMED,
            STATUS,
            SOURCE_FILE,
            BATCH_ID,
            BIW_LOAD_DATE
        FROM ranked_txns
        WHERE rn = 1
          AND MILES_REDEEMED IS NOT NULL
          AND MILES_REDEEMED > 0
    ) src
    ON tgt.TXN_ID = src.TXN_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.MEMBER_ID      = src.MEMBER_ID,
        tgt.FEED_DATE      = src.FEED_DATE,
        tgt.TXN_DATE       = src.TXN_DATE,
        tgt.PARTNER        = src.PARTNER,
        tgt.MILES_REDEEMED = src.MILES_REDEEMED,
        tgt.STATUS         = src.STATUS,
        tgt.SOURCE_FILE    = src.SOURCE_FILE,
        tgt.BATCH_ID       = src.BATCH_ID,
        tgt.BIW_LOAD_DATE  = src.BIW_LOAD_DATE,
        tgt.UPDATED_AT     = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        TXN_ID, MEMBER_ID, FEED_DATE, TXN_DATE,
        PARTNER, MILES_REDEEMED, STATUS,
        SOURCE_FILE, BATCH_ID, BIW_LOAD_DATE, UPDATED_AT
    ) VALUES (
        src.TXN_ID, src.MEMBER_ID, src.FEED_DATE, src.TXN_DATE,
        src.PARTNER, src.MILES_REDEEMED, src.STATUS,
        src.SOURCE_FILE, src.BATCH_ID, src.BIW_LOAD_DATE, CURRENT_TIMESTAMP()
    );

    SELECT COUNT(*) INTO :v_ind_count FROM SILVER.TABLE_INDIA;
    SELECT COUNT(*) INTO :v_usa_count FROM SILVER.TABLE_USA;
    SELECT COUNT(*) INTO :v_aus_count FROM SILVER.TABLE_AUS;
    SELECT COUNT(*) INTO :v_redemp_count FROM SILVER.MEMBER_REDEMPTIONS;

    v_rows_written := :v_ind_count + :v_usa_count + :v_aus_count + :v_redemp_count;

    INSERT INTO AUDIT.PIPELINE_AUDIT (
        PIPELINE_RUN_ID, STAGE, STATUS, ROWS_READ, ROWS_WRITTEN, ROWS_QUARANTINED,
        ERROR_MESSAGE, STARTED_AT, ENDED_AT, BIW_LOAD_DATE
    )
    VALUES (
        :v_run_id, 'RAW_TO_STG', 'SUCCESS', :v_rows_read, :v_rows_written, 0,
        NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

    RETURN 'RAW_TO_STG completed [' || :v_run_id || '] -> IND: ' || :v_ind_count
        || ', USA: ' || :v_usa_count
        || ', AUS: ' || :v_aus_count
        || ', REDEMPTIONS: ' || :v_redemp_count;

EXCEPTION
    WHEN OTHER THEN
        INSERT INTO AUDIT.PIPELINE_AUDIT (
            PIPELINE_RUN_ID, STAGE, STATUS, ROWS_READ, ROWS_WRITTEN, ROWS_QUARANTINED,
            ERROR_MESSAGE, STARTED_AT, ENDED_AT, BIW_LOAD_DATE
        )
        VALUES (
            :v_run_id, 'RAW_TO_STG', 'FAILED', :v_rows_read, 0, 0,
            LEFT(SQLERRM, 3900), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        );
        RAISE;
END;
$$;