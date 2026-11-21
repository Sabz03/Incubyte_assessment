from __future__ import annotations

import os
from datetime import datetime
from importlib.util import find_spec
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

if find_spec("dotenv") is not None:
    from dotenv import load_dotenv

    load_dotenv()

REFRESH_SECONDS = 1800  # 30 minutes
DETAIL_LIMIT = 500
IST = ZoneInfo("Asia/Kolkata")
WAREHOUSE = "SKYPOINTS_WH"
DATABASE = "SKYPOINTS_DB"

st.set_page_config(
    page_title="SkyPoints Dashboard",
    page_icon="✈️",
    layout="wide",
)


def _get_snowpark_session() -> Any | None:
    try:
        from snowflake.snowpark.context import get_active_session

        session = get_active_session()
        session.sql(f"USE WAREHOUSE {WAREHOUSE}").collect()
        session.sql(f"USE DATABASE {DATABASE}").collect()
        return session
    except Exception:
        return None


@st.cache_resource
def _get_local_connection():
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=WAREHOUSE,
        database=DATABASE,
    )
    with conn.cursor() as cur:
        cur.execute(f"USE WAREHOUSE {WAREHOUSE}")
        cur.execute(f"USE DATABASE {DATABASE}")
    return conn


def run_query(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    """
    Executes SQL via Snowpark (SiS) or falls back to local snowflake-connector-python.
    Automatically adapts %s placeholders to ? for Snowpark's session.sql().
    """
    session = _get_snowpark_session()
    if session is not None:
        snowpark_sql = sql.replace("%s", "?") if params else sql
        df = session.sql(snowpark_sql, params=list(params) if params else None).to_pandas()
    else:
        conn = _get_local_connection()
        with conn.cursor() as cur:
            cur.execute(sql, params if params else None)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description] if cur.description else []
            df = pd.DataFrame(rows, columns=cols)

    if not df.empty:
        df.columns = [str(c).upper() for c in df.columns]
    return df


def _member_where_clause(
    countries: list[str],
    stale_opt: str,
    search: str,
) -> tuple[str, list[Any]]:
    clauses: list[str] = ["1 = 1"]
    params: list[Any] = []

    if countries:
        placeholders = ", ".join(["%s"] * len(countries))
        clauses.append(f"COUNTRY IN ({placeholders})")
        params.extend(countries)
    else:
        clauses.append("1 = 0")

    if stale_opt in ("Y", "N"):
        clauses.append("STALE_MEMBER = %s")
        params.append(stale_opt)

    if search:
        term = search.lower()
        prefix_term = f"{term}%"
        contains_term = f"%{term}%"
        # Prefix match on MEMBER_ID and POST_CODE enables Snowflake micro-partition pruning
        clauses.append(
            "(LOWER(MEMBER_ID) LIKE %s OR LOWER(COALESCE(POST_CODE, '')) LIKE %s OR LOWER(MEMBER_NAME) LIKE %s)"
        )
        params.extend([prefix_term, prefix_term, contains_term])

    return " AND ".join(clauses), params


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner="Querying Snowflake...")
def fetch_filter_options(_buster: int) -> tuple[list[str], list[str]]:
    c_df = run_query(
        f"SELECT DISTINCT COUNTRY FROM {DATABASE}.GOLD.MEMBER_LOYALTY_SUMMARY WHERE COUNTRY IS NOT NULL ORDER BY COUNTRY"
    )
    p_df = run_query(
        f"SELECT DISTINCT PARTNER FROM {DATABASE}.SILVER.MEMBER_REDEMPTIONS WHERE PARTNER IS NOT NULL ORDER BY PARTNER"
    )
    countries = c_df["COUNTRY"].dropna().tolist() if not c_df.empty else ["IND", "USA", "AUS"]
    partners = p_df["PARTNER"].dropna().tolist() if not p_df.empty else []
    return countries, partners


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner="Loading KPIs...")
def fetch_kpis(_buster: int, countries: tuple[str, ...], stale_opt: str, search: str):
    where_sql, params = _member_where_clause(list(countries), stale_opt, search)
    kpi_df = run_query(
        f"""
        SELECT
            COUNT(*) AS ACTIVE_MEMBERS,
            COUNT_IF(STALE_MEMBER = 'Y') AS STALE_MEMBERS,
            COALESCE(SUM(TOTAL_MILES_REDEEMED), 0) AS COMPLETED_MILES_REDEEMED,
            COALESCE(SUM(PENDING_MILES), 0) AS PENDING_MILES_TOTAL,
            COALESCE(SUM(REVERSED_MILES), 0) AS REVERSED_MILES_TOTAL
        FROM {DATABASE}.GOLD.MEMBER_LOYALTY_SUMMARY
        WHERE {where_sql}
        """,
        tuple(params),
    )

    q_total = 0
    for q_table in (
        f"{DATABASE}.BRONZE.QUARANTINE_MEMBER_PROFILES",
        f"{DATABASE}.BRONZE.QUARANTINE_REDEMPTION_JSON",
    ):
        try:
            t_df = run_query(f"SELECT COUNT(*) AS CNT FROM {q_table}")
            if not t_df.empty:
                q_total += int(t_df.iloc[0]["CNT"])
        except Exception:
            pass
    q_df = pd.DataFrame({"QUARANTINED_ROWS": [q_total]})

    audit_df = run_query(
        f"""
        SELECT PIPELINE_RUN_ID, STAGE, STATUS, ROWS_READ, ROWS_WRITTEN, ROWS_QUARANTINED,
               STARTED_AT, ENDED_AT, ERROR_MESSAGE
        FROM {DATABASE}.AUDIT.PIPELINE_AUDIT
        ORDER BY STARTED_AT DESC
        LIMIT 100
        """
    )
    return kpi_df, q_df, audit_df


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner="Loading Loyalty Overview...")
def fetch_loyalty_tab(_buster: int, countries: tuple[str, ...], stale_opt: str, search: str):
    where_sql, params = _member_where_clause(list(countries), stale_opt, search)
    by_country = run_query(
        f"""
        SELECT COUNTRY, COUNT(*) AS MEMBER_COUNT
        FROM {DATABASE}.GOLD.MEMBER_LOYALTY_SUMMARY
        WHERE {where_sql}
        GROUP BY COUNTRY
        ORDER BY MEMBER_COUNT DESC
        """,
        tuple(params),
    )
    by_tier = run_query(
        f"""
        SELECT TIER_CODE, STALE_MEMBER, COUNT(*) AS MEMBER_COUNT
        FROM {DATABASE}.GOLD.MEMBER_LOYALTY_SUMMARY
        WHERE {where_sql}
        GROUP BY TIER_CODE, STALE_MEMBER
        """,
        tuple(params),
    )
    detail = run_query(
        f"""
        SELECT *
        FROM {DATABASE}.GOLD.MEMBER_LOYALTY_SUMMARY
        WHERE {where_sql}
        ORDER BY TOTAL_MILES_REDEEMED DESC, MEMBER_ID
        LIMIT {DETAIL_LIMIT}
        """,
        tuple(params),
    )
    return by_country, by_tier, detail


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner="Loading Redemptions...")
def fetch_redemptions_tab(
    _buster: int,
    countries: tuple[str, ...],
    stale_opt: str,
    search: str,
    partners: tuple[str, ...],
):
    where_sql, params = _member_where_clause(list(countries), stale_opt, search)
    partner_clause = "1 = 0"
    p_params: list[Any] = list(params)
    if partners:
        placeholders = ", ".join(["%s"] * len(partners))
        partner_clause = f"r.PARTNER IN ({placeholders})"
        p_params.extend(partners)

    base_cte = f"""
        WITH filtered_members AS (
            SELECT MEMBER_ID
            FROM {DATABASE}.GOLD.MEMBER_LOYALTY_SUMMARY
            WHERE {where_sql}
        ),
        filtered_redemptions AS (
            SELECT r.*
            FROM {DATABASE}.SILVER.MEMBER_REDEMPTIONS r
            INNER JOIN filtered_members m ON r.MEMBER_ID = m.MEMBER_ID
            WHERE {partner_clause}
        )
    """
    summary = run_query(
        base_cte
        + """
        SELECT
            PARTNER,
            COUNT(*) AS REDEMPTION_COUNT,
            COALESCE(SUM(MILES_REDEEMED), 0) AS TOTAL_MILES_REDEEMED
        FROM filtered_redemptions
        GROUP BY PARTNER
        ORDER BY TOTAL_MILES_REDEEMED DESC
        """,
        tuple(p_params),
    )
    by_status = run_query(
        base_cte
        + """
        SELECT STATUS, COALESCE(SUM(MILES_REDEEMED), 0) AS TOTAL_MILES_REDEEMED
        FROM filtered_redemptions
        GROUP BY STATUS
        """,
        tuple(p_params),
    )
    daily = run_query(
        base_cte
        + """
        SELECT TXN_DATE, COALESCE(SUM(MILES_REDEEMED), 0) AS TOTAL_MILES_REDEEMED
        FROM filtered_redemptions
        WHERE TXN_DATE IS NOT NULL
        GROUP BY TXN_DATE
        ORDER BY TXN_DATE
        """,
        tuple(p_params),
    )
    detail = run_query(
        base_cte
        + f"""
        SELECT *
        FROM filtered_redemptions
        ORDER BY TXN_DATE DESC, TXN_ID
        LIMIT {DETAIL_LIMIT}
        """,
        tuple(p_params),
    )
    return summary, by_status, daily, detail


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner="Loading Quarantine Records...")
def fetch_quarantine(_buster: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for rec_type, q_table in (
        ("MEMBER", f"{DATABASE}.BRONZE.QUARANTINE_MEMBER_PROFILES"),
        ("REDEMPTION", f"{DATABASE}.BRONZE.QUARANTINE_REDEMPTION_JSON"),
    ):
        try:
            df = run_query(
                f"""
                SELECT
                    '{rec_type}' AS RECORD_TYPE,
                    MEMBER_ID,
                    REJECT_REASON,
                    SOURCE_FILE,
                    BATCH_ID,
                    BIW_LOAD_DATE
                FROM {q_table}
                ORDER BY BIW_LOAD_DATE DESC
                LIMIT 100
                """
            )
            if not df.empty:
                frames.append(df)
        except Exception:
            pass

    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    if "BIW_LOAD_DATE" in combined.columns:
        combined = combined.sort_values("BIW_LOAD_DATE", ascending=False).head(200)
    return combined


def _latest_stage_status(df: pd.DataFrame, stage_name: str) -> str:
    if df.empty or "STAGE" not in df.columns or "STATUS" not in df.columns:
        return "N/A"
    matched = df[df["STAGE"].astype(str).str.upper() == stage_name.upper()]
    if matched.empty:
        return "N/A"
    raw_status = str(matched.iloc[0]["STATUS"]).upper()
    return "FAILURE" if raw_status in ("FAILED", "FAILURE") else raw_status


if "buster" not in st.session_state:
    st.session_state.buster = 0
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = datetime.now(IST)


def refresh_now():
    st.session_state.buster += 1
    st.session_state.last_refresh = datetime.now(IST)
    st.cache_data.clear()
    st.rerun()


available_countries, available_partners = fetch_filter_options(st.session_state.buster)

with st.sidebar:
    st.header("✈️ SkyPoints")
    if st.button("🔄 Manual Refresh", use_container_width=True, type="primary"):
        refresh_now()
    st.caption(
        f"Auto-refresh: Every 30 min\nLast synced: {st.session_state.last_refresh.strftime('%d %b, %I:%M:%S %p')}"
    )
    st.divider()
    countries = st.multiselect("Country", available_countries, default=available_countries)
    stale_opt = st.selectbox("Stale (>90d)", ["All", "Y", "N"])
    search = st.text_input("Search ID / Name / Post Code").strip()


@st.fragment(run_every=REFRESH_SECONDS)
def dashboard():
    st.session_state.last_refresh = datetime.now(IST)
    st.title("SkyPoints Loyalty & Pipeline Dashboard")

    try:
        kpi_df, q_df, audit_df = fetch_kpis(
            st.session_state.buster, tuple(countries), stale_opt, search
        )
    except Exception as err:
        st.error(f"Query failed: {err}")
        return

    bronze_status = _latest_stage_status(audit_df, "BRONZE_INGEST")
    stg_status = _latest_stage_status(audit_df, "RAW_TO_STG")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Active Members", f"{int(kpi_df.iloc[0]['ACTIVE_MEMBERS']):,}" if not kpi_df.empty else "0")
    c2.metric("Stale Members", f"{int(kpi_df.iloc[0]['STALE_MEMBERS']):,}" if not kpi_df.empty else "0")
    c3.metric(
        "Completed Miles",
        f"{float(kpi_df.iloc[0]['COMPLETED_MILES_REDEEMED']):,.2f}" if not kpi_df.empty else "0.00",
    )
    c4.metric(
        "Quarantined Rows",
        f"{int(q_df.iloc[0]['QUARANTINED_ROWS']):,}" if not q_df.empty else "0",
    )
    with c5:
        st.caption("Last Run")
        st.markdown(f"**Bronze:** `{bronze_status}`  \n**STG:** `{stg_status}`")

    tab1, tab2, tab3 = st.tabs(["Loyalty Overview", "Redemptions", "Pipeline Audit"])

    with tab1:
        by_country, by_tier, detail_df = fetch_loyalty_tab(
            st.session_state.buster, tuple(countries), stale_opt, search
        )
        left, right = st.columns(2)
        with left:
            st.subheader("Members by Country")
            if not by_country.empty:
                st.bar_chart(by_country.set_index("COUNTRY")["MEMBER_COUNT"])
        with right:
            st.subheader("Members by Tier & Stale Status")
            if not by_tier.empty:
                pivot = by_tier.pivot_table(
                    index="TIER_CODE",
                    columns="STALE_MEMBER",
                    values="MEMBER_COUNT",
                    aggfunc="sum",
                    fill_value=0,
                )
                st.bar_chart(pivot)

        st.subheader(f"Gold Member Summary (Top {DETAIL_LIMIT})")
        st.dataframe(detail_df, use_container_width=True, hide_index=True)
        st.download_button(
            "Download CSV",
            data=detail_df.to_csv(index=False).encode("utf-8"),
            file_name=f"skypoints_members_{datetime.now(IST).strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )

    with tab2:
        selected_partners = st.multiselect(
            "Filter by Partner",
            options=available_partners,
            default=available_partners,
            placeholder="Select one or more partners",
        )
        partner_summary, by_status, daily, redemp_detail = fetch_redemptions_tab(
            st.session_state.buster,
            tuple(countries),
            stale_opt,
            search,
            tuple(selected_partners),
        )

        p1, p2, p3 = st.columns(3)
        p1.metric(
            "Filtered Redemptions",
            f"{int(partner_summary['REDEMPTION_COUNT'].sum()):,}" if not partner_summary.empty else "0",
        )
        p2.metric(
            "Filtered Miles Redeemed",
            f"{float(partner_summary['TOTAL_MILES_REDEEMED'].sum()):,.2f}" if not partner_summary.empty else "0.00",
        )
        p3.metric("Selected Partners", f"{len(partner_summary)}")

        if not partner_summary.empty:
            st.subheader("Redemption Summary by Partner")
            st.dataframe(partner_summary, use_container_width=True, hide_index=True)

        left, right = st.columns(2)
        with left:
            st.subheader("Miles by Partner")
            if not partner_summary.empty:
                st.bar_chart(partner_summary.set_index("PARTNER")["TOTAL_MILES_REDEEMED"])
        with right:
            st.subheader("Miles by Status")
            if not by_status.empty:
                st.bar_chart(by_status.set_index("STATUS")["TOTAL_MILES_REDEEMED"])

        if not daily.empty:
            st.subheader("Daily Redeemed Miles")
            daily["TXN_DATE"] = pd.to_datetime(daily["TXN_DATE"], errors="coerce")
            st.line_chart(daily.dropna(subset=["TXN_DATE"]).set_index("TXN_DATE")["TOTAL_MILES_REDEEMED"])

        st.subheader(f"Redemption Transactions (Latest {DETAIL_LIMIT})")
        st.dataframe(redemp_detail, use_container_width=True, hide_index=True)

    with tab3:
        st.subheader("Pipeline Audit Log")
        st.dataframe(audit_df, use_container_width=True, hide_index=True)

        st.subheader("Quarantine Records")
        quarantine_df = fetch_quarantine(st.session_state.buster)
        if not quarantine_df.empty:
            st.dataframe(quarantine_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No quarantine records found.")


dashboard()