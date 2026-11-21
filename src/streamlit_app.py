from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

REFRESH_SECONDS = 1800  # 30 minutes
IST = ZoneInfo("Asia/Kolkata")

st.set_page_config(
    page_title="SkyPoints Dashboard",
    page_icon="✈️",
    layout="wide",
)

session = get_active_session()
session.use_database("SKYPOINTS_DB")
session.use_warehouse("SKYPOINTS_WH")


def run_query(sql: str) -> pd.DataFrame:
    df = session.sql(sql).to_pandas()
    if not df.empty:
        df.columns = [str(c).upper() for c in df.columns]
    return df


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner="Loading SkyPoints data...")
def fetch_all(_buster: int):
    gold_df = run_query("SELECT * FROM SKYPOINTS_DB.GOLD.MEMBER_LOYALTY_SUMMARY")
    redemp_df = run_query("SELECT * FROM SKYPOINTS_DB.SILVER.MEMBER_REDEMPTIONS")
    audit_df = run_query(
        "SELECT * FROM SKYPOINTS_DB.AUDIT.PIPELINE_AUDIT ORDER BY STARTED_AT DESC LIMIT 100"
    )
    try:
        quarantine_df = run_query("SELECT * FROM SKYPOINTS_DB.AUDIT.QUARANTINE_RECORDS LIMIT 200")
    except Exception:
        quarantine_df = pd.DataFrame()
    return gold_df, redemp_df, audit_df, quarantine_df


if "buster" not in st.session_state:
    st.session_state.buster = 0
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = datetime.now(IST)


def refresh_now():
    st.session_state.buster += 1
    st.session_state.last_refresh = datetime.now(IST)
    st.cache_data.clear()
    st.rerun()


# --- Sidebar: Refresh + Member Filters ---
with st.sidebar:
    st.header("✈️ SkyPoints")
    if st.button("🔄 Manual Refresh", use_container_width=True, type="primary"):
        refresh_now()
    st.caption(
        f"Auto-refresh: Every 30 min\nLast synced: {st.session_state.last_refresh.strftime('%d %b, %I:%M:%S %p')}"
    )

    st.divider()
    countries = st.multiselect("Country", ["IND", "USA", "AUS"], default=["IND", "USA", "AUS"])
    stale_opt = st.selectbox("Stale (>90d)", ["All", "Y", "N"])
    search = st.text_input("Search ID / Name / Post Code").strip().lower()


@st.fragment(run_every=REFRESH_SECONDS)
def dashboard():
    st.session_state.last_refresh = datetime.now(IST)
    st.title("SkyPoints Loyalty & Pipeline Dashboard")

    try:
        gold_df, redemp_df, audit_df, quarantine_df = fetch_all(st.session_state.buster)
    except Exception as err:
        st.error(f"Query failed: {err}")
        return

    # Filter member view
    m_df = gold_df.copy()
    if not m_df.empty:
        if countries and "COUNTRY" in m_df.columns:
            m_df = m_df[m_df["COUNTRY"].isin(countries)]
        if stale_opt in ("Y", "N") and "STALE_MEMBER" in m_df.columns:
            m_df = m_df[m_df["STALE_MEMBER"] == stale_opt]
        if search:
            cols = [c for c in ["MEMBER_ID", "MEMBER_NAME", "POST_CODE"] if c in m_df.columns]
            if cols:
                mask = pd.Series(False, index=m_df.index)
                for c in cols:
                    mask = mask | m_df[c].astype(str).str.lower().str.contains(search, na=False)
                m_df = m_df[mask]

    member_ids = (
        set(m_df["MEMBER_ID"].dropna().astype(str))
        if not m_df.empty and "MEMBER_ID" in m_df.columns
        else set()
    )
    base_redemp_df = (
        redemp_df[redemp_df["MEMBER_ID"].astype(str).isin(member_ids)].copy()
        if not redemp_df.empty and "MEMBER_ID" in redemp_df.columns
        else redemp_df.copy()
    )

    # Top KPI row
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Active Members", f"{len(m_df):,}")
    stale_cnt = (
        int((m_df["STALE_MEMBER"] == "Y").sum())
        if not m_df.empty and "STALE_MEMBER" in m_df.columns
        else 0
    )
    c2.metric("Stale Members", f"{stale_cnt:,}")
    miles_val = (
        float(base_redemp_df["MILES_REDEEMED"].fillna(0).sum())
        if not base_redemp_df.empty and "MILES_REDEEMED" in base_redemp_df.columns
        else 0.0
    )
    c3.metric("Miles Redeemed", f"{miles_val:,.2f}")
    q_cnt = (
        int(audit_df["ROWS_QUARANTINED"].fillna(0).sum())
        if not audit_df.empty and "ROWS_QUARANTINED" in audit_df.columns
        else 0
    )
    c4.metric("Quarantined Rows", f"{q_cnt:,}")
    run_status = (
        str(audit_df.iloc[0]["STATUS"])
        if not audit_df.empty and "STATUS" in audit_df.columns
        else "N/A"
    )
    c5.metric("Last Run", run_status)

    tab1, tab2, tab3 = st.tabs(["Loyalty Overview", "Redemptions", "Pipeline Audit"])

    # Tab 1: Members & Regions
    with tab1:
        left, right = st.columns(2)
        with left:
            st.subheader("Members by Country")
            if not m_df.empty and "COUNTRY" in m_df.columns:
                by_country = m_df.groupby("COUNTRY")["MEMBER_ID"].count()
                st.bar_chart(by_country)
        with right:
            st.subheader("Members by Tier & Stale Status")
            if not m_df.empty and {"TIER_CODE", "STALE_MEMBER"}.issubset(m_df.columns):
                by_tier = m_df.pivot_table(
                    index="TIER_CODE",
                    columns="STALE_MEMBER",
                    values="MEMBER_ID",
                    aggfunc="count",
                    fill_value=0,
                )
                st.bar_chart(by_tier)

        st.subheader("Gold Member Summary")
        st.dataframe(m_df, use_container_width=True, hide_index=True)
        st.download_button(
            "Download CSV",
            data=m_df.to_csv(index=False).encode("utf-8"),
            file_name=f"skypoints_members_{datetime.now(IST).strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )

    # Tab 2: Redemptions (All Statuses + Partner Filter)
    with tab2:
        partner_options = (
            sorted(base_redemp_df["PARTNER"].dropna().astype(str).unique().tolist())
            if not base_redemp_df.empty and "PARTNER" in base_redemp_df.columns
            else []
        )
        selected_partners = st.multiselect(
            "Filter by Partner",
            options=partner_options,
            default=partner_options,
            placeholder="Select one or more partners",
        )

        r_df = base_redemp_df.copy()
        if not r_df.empty and "PARTNER" in r_df.columns:
            if selected_partners:
                r_df = r_df[r_df["PARTNER"].astype(str).isin(selected_partners)]
            else:
                r_df = r_df.iloc[0:0]

        p1, p2, p3 = st.columns(3)
        p1.metric("Filtered Redemptions", f"{len(r_df):,}")
        p2.metric(
            "Filtered Miles Redeemed",
            f"{float(r_df['MILES_REDEEMED'].fillna(0).sum()) if not r_df.empty and 'MILES_REDEEMED' in r_df.columns else 0.0:,.2f}",
        )
        p3.metric(
            "Selected Partners",
            f"{r_df['PARTNER'].nunique() if not r_df.empty and 'PARTNER' in r_df.columns else 0}",
        )

        if not r_df.empty and {"PARTNER", "MILES_REDEEMED"}.issubset(r_df.columns):
            st.subheader("Redemption Summary by Partner")
            txn_col = "TXN_ID" if "TXN_ID" in r_df.columns else "MEMBER_ID"
            partner_summary = (
                r_df.groupby("PARTNER", as_index=False)
                .agg(
                    REDEMPTION_COUNT=(txn_col, "count"),
                    TOTAL_MILES_REDEEMED=("MILES_REDEEMED", "sum"),
                )
                .sort_values("TOTAL_MILES_REDEEMED", ascending=False)
            )
            st.dataframe(partner_summary, use_container_width=True, hide_index=True)

        left, right = st.columns(2)
        with left:
            st.subheader("Miles by Partner")
            if not r_df.empty and {"PARTNER", "MILES_REDEEMED"}.issubset(r_df.columns):
                st.bar_chart(r_df.groupby("PARTNER")["MILES_REDEEMED"].sum())
        with right:
            st.subheader("Miles by Status")
            if not r_df.empty and {"STATUS", "MILES_REDEEMED"}.issubset(r_df.columns):
                st.bar_chart(r_df.groupby("STATUS")["MILES_REDEEMED"].sum())

        if not r_df.empty and {"TXN_DATE", "MILES_REDEEMED"}.issubset(r_df.columns):
            st.subheader("Daily Redeemed Miles")
            t_df = r_df.copy()
            t_df["TXN_DATE"] = pd.to_datetime(t_df["TXN_DATE"], errors="coerce")
            daily = (
                t_df.dropna(subset=["TXN_DATE"])
                .groupby("TXN_DATE")["MILES_REDEEMED"]
                .sum()
                .sort_index()
            )
            if not daily.empty:
                st.line_chart(daily)

        st.subheader("Redemption Transactions")
        st.dataframe(r_df, use_container_width=True, hide_index=True)

    # Tab 3: Audit & Quarantine
    with tab3:
        st.subheader("Pipeline Audit Log")
        st.dataframe(audit_df, use_container_width=True, hide_index=True)

        st.subheader("Quarantine Records")
        if not quarantine_df.empty:
            st.dataframe(quarantine_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No quarantine records found.")


dashboard()