import streamlit as st
import pandas as pd
import altair as alt
import time
from datetime import datetime
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.set_page_config(page_title="Tasty Bytes — Live Dashboard", layout="wide")

if "prev_counts" not in st.session_state:
    st.session_state.prev_counts = {}
if "refresh_count" not in st.session_state:
    st.session_state.refresh_count = 0

REFRESH_INTERVAL = st.sidebar.slider("Auto-refresh interval (seconds)", 5, 120, 15)
auto_refresh = st.sidebar.toggle("Auto-refresh", value=True)

st.sidebar.divider()
st.sidebar.metric("Refreshes", st.session_state.refresh_count)
st.sidebar.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")

if st.sidebar.button("Refresh Now"):
    st.cache_data.clear()
    st.session_state.refresh_count += 1

st.title("Tasty Bytes: Road to 1,120 Trucks & $320M Revenue")
st.caption("Live data from Iceberg tables → Dynamic Tables → Horizon-governed analytics")

tab_live, tab_sales, tab_supply, tab_sustain, tab_click, tab_dq, tab_cost = st.tabs([
    "Live Ingestion", "Sales & Loyalty", "Supply Chain", "Sustainability", "Clickstream", "Data Quality", "Cost Governance"
])


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_live_counts():
    return session.sql("""
        SELECT 'RAW_ORDERS' AS TBL, COUNT(*) AS ROW_COUNT, MAX(INGESTED_AT) AS LATEST_INGESTED FROM TASTY_BYTES_DEMO.DEV.RAW_ORDERS
        UNION ALL SELECT 'RAW_CUSTOMERS', COUNT(*), MAX(INGESTED_AT) FROM TASTY_BYTES_DEMO.DEV.RAW_CUSTOMERS
        UNION ALL SELECT 'RAW_MENU', COUNT(*), MAX(INGESTED_AT) FROM TASTY_BYTES_DEMO.DEV.RAW_MENU
        UNION ALL SELECT 'RAW_CLICKSTREAM', COUNT(*), MAX(INGESTED_AT) FROM TASTY_BYTES_DEMO.DEV.RAW_CLICKSTREAM
    """).to_pandas()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_ingestion_timeline():
    return session.sql("""
        SELECT
            DATE_TRUNC('MINUTE', INGESTED_AT) AS INGESTION_MINUTE,
            'Orders' AS SOURCE,
            COUNT(*) AS ROWS_INGESTED
        FROM TASTY_BYTES_DEMO.DEV.RAW_ORDERS
        WHERE INGESTED_AT IS NOT NULL
        GROUP BY 1
        UNION ALL
        SELECT DATE_TRUNC('MINUTE', INGESTED_AT), 'Customers', COUNT(*)
        FROM TASTY_BYTES_DEMO.DEV.RAW_CUSTOMERS
        WHERE INGESTED_AT IS NOT NULL
        GROUP BY 1
        UNION ALL
        SELECT DATE_TRUNC('MINUTE', INGESTED_AT), 'Clickstream', COUNT(*)
        FROM TASTY_BYTES_DEMO.DEV.RAW_CLICKSTREAM
        WHERE INGESTED_AT IS NOT NULL
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 200
    """).to_pandas()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_latency():
    return session.sql("""
        SELECT
            DATEDIFF('SECOND', MAX(INGESTED_AT), CURRENT_TIMESTAMP()) AS ORDERS_LATENCY_SEC,
            (SELECT DATEDIFF('SECOND', MAX(INGESTED_AT), CURRENT_TIMESTAMP()) FROM TASTY_BYTES_DEMO.DEV.RAW_CUSTOMERS) AS CUSTOMERS_LATENCY_SEC,
            (SELECT DATEDIFF('SECOND', MAX(INGESTED_AT), CURRENT_TIMESTAMP()) FROM TASTY_BYTES_DEMO.DEV.RAW_CLICKSTREAM) AS CLICKSTREAM_LATENCY_SEC
        FROM TASTY_BYTES_DEMO.DEV.RAW_ORDERS
    """).to_pandas()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_dt_freshness():
    return session.sql("""
        SELECT
            'DT_ORDERS_ENRICHED' AS DT_NAME, MAX(REFRESHED_AT) AS LAST_REFRESH, COUNT(*) AS ROWS
        FROM TASTY_BYTES_DEMO.DEV.DT_ORDERS_ENRICHED
        UNION ALL
        SELECT 'DT_DAILY_SALES_SUMMARY', MAX(REFRESHED_AT), COUNT(*)
        FROM TASTY_BYTES_DEMO.DEV.DT_DAILY_SALES_SUMMARY
        UNION ALL
        SELECT 'DT_CUSTOMER_360', MAX(REFRESHED_AT), COUNT(*)
        FROM TASTY_BYTES_DEMO.DEV.DT_CUSTOMER_360
        UNION ALL
        SELECT 'DT_SUPPLY_CHAIN_METRICS', MAX(REFRESHED_AT), COUNT(*)
        FROM TASTY_BYTES_DEMO.DEV.DT_SUPPLY_CHAIN_METRICS
        UNION ALL
        SELECT 'DT_CLICKSTREAM_ANALYTICS', MAX(REFRESHED_AT), COUNT(*)
        FROM TASTY_BYTES_DEMO.DEV.DT_CLICKSTREAM_ANALYTICS
    """).to_pandas()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_sales():
    return session.sql("""
        SELECT LOYALTY_TIER, ORDER_CHANNEL, CUSTOMER_COUNTRY,
               SUM(TOTAL_ORDERS) AS ORDERS, SUM(TOTAL_REVENUE) AS REVENUE,
               AVG(AVG_ORDER_VALUE) AS AOV, SUM(TOTAL_DISCOUNTS) AS DISCOUNTS,
               SUM(UNIQUE_CUSTOMERS) AS CUSTOMERS
        FROM TASTY_BYTES_DEMO.DEV.DT_DAILY_SALES_SUMMARY
        GROUP BY 1, 2, 3
    """).to_pandas()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_supply():
    return session.sql("""
        SELECT TRUCK_BRAND_NAME, ITEM_CATEGORY,
               SUM(TOTAL_COGS) AS COGS, SUM(TOTAL_REVENUE) AS REVENUE,
               SUM(TOTAL_MARGIN) AS MARGIN, AVG(AVG_MARGIN_PCT) AS MARGIN_PCT,
               AVG(ORDERS_PER_TRUCK) AS ORDERS_PER_TRUCK
        FROM TASTY_BYTES_DEMO.DEV.DT_SUPPLY_CHAIN_METRICS
        GROUP BY 1, 2
    """).to_pandas()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_sustainability():
    return session.sql("""
        SELECT TRUCK_BRAND_NAME, ITEM_CATEGORY,
               SUM(EST_CO2_KG_PER_DAY) AS CO2_KG,
               SUM(REVENUE) AS REVENUE,
               AVG(REVENUE_PER_KG_CO2) AS REV_PER_KG_CO2,
               AVG(COGS_TO_REVENUE_PCT) AS WASTE_PCT,
               SUM(EST_WASTE_COST_USD) AS WASTE_COST
        FROM TASTY_BYTES_DEMO.DEV.DT_SUSTAINABILITY_REPORT
        GROUP BY 1, 2
    """).to_pandas()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_clickstream():
    return session.sql("""
        SELECT DEVICE_TYPE, MEMBERSHIP_LEVEL, REFERRER, PAGE_URL,
               SUM(PAGE_VIEWS) AS VIEWS, SUM(UNIQUE_VISITORS) AS VISITORS,
               AVG(AVG_SESSION_MIN) AS AVG_SESSION,
               AVG(AVG_TIME_ON_APP_MIN) AS APP_TIME,
               AVG(AVG_TIME_ON_WEBSITE_MIN) AS WEB_TIME,
               AVG(AVG_YEARLY_SPEND) AS AVG_SPEND
        FROM TASTY_BYTES_DEMO.DEV.DT_CLICKSTREAM_ANALYTICS
        GROUP BY 1, 2, 3, 4
    """).to_pandas()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_dq():
    return session.sql("""
        SELECT METRIC_NAME AS DMF, TABLE_NAME, COLUMN_NAME,
               VALUE AS METRIC_VALUE, MEASUREMENT_TIME
        FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
        WHERE TABLE_DATABASE = 'TASTY_BYTES_DEMO'
        ORDER BY MEASUREMENT_TIME DESC
        LIMIT 50
    """).to_pandas()


with tab_live:
    st.subheader("Real-Time Ingestion Monitor")
    status_indicator = "🟢 STREAMING" if auto_refresh else "⏸️ PAUSED"
    st.markdown(f"**Pipeline Status:** {status_indicator} &nbsp;&nbsp; | &nbsp;&nbsp; **Refresh:** every {REFRESH_INTERVAL}s")

    counts = load_live_counts()
    count_dict = dict(zip(counts["TBL"], counts["ROW_COUNT"]))

    c1, c2, c3, c4 = st.columns(4)
    for col, tbl, icon in [(c1, "RAW_ORDERS", "🛒"), (c2, "RAW_CUSTOMERS", "👤"), (c3, "RAW_MENU", "🍔"), (c4, "RAW_CLICKSTREAM", "🌐")]:
        current = int(count_dict.get(tbl, 0))
        prev = st.session_state.prev_counts.get(tbl, current)
        delta = current - prev
        col.metric(f"{icon} {tbl.replace('RAW_', '')}", f"{current:,}", delta=f"+{delta:,}" if delta > 0 else "0")

    st.session_state.prev_counts = count_dict

    st.divider()

    lcol, rcol = st.columns(2)

    with lcol:
        st.markdown("##### Ingestion Latency (seconds since last row)")
        try:
            latency = load_latency()
            lat_c1, lat_c2, lat_c3 = st.columns(3)
            orders_lat = int(latency["ORDERS_LATENCY_SEC"].iloc[0] or 0)
            cust_lat = int(latency["CUSTOMERS_LATENCY_SEC"].iloc[0] or 0)
            click_lat = int(latency["CLICKSTREAM_LATENCY_SEC"].iloc[0] or 0)
            lat_c1.metric("Orders", f"{orders_lat}s", delta="OK" if orders_lat < 60 else "STALE", delta_color="normal" if orders_lat < 60 else "inverse")
            lat_c2.metric("Customers", f"{cust_lat}s", delta="OK" if cust_lat < 60 else "STALE", delta_color="normal" if cust_lat < 60 else "inverse")
            lat_c3.metric("Clickstream", f"{click_lat}s", delta="OK" if click_lat < 60 else "STALE", delta_color="normal" if click_lat < 60 else "inverse")
        except Exception:
            st.info("Latency data not yet available.")

    with rcol:
        st.markdown("##### Dynamic Table Freshness")
        try:
            dt_fresh = load_dt_freshness()
            st.dataframe(
                dt_fresh.rename(columns={"DT_NAME": "Dynamic Table", "LAST_REFRESH": "Last Refresh", "ROWS": "Rows"}),
                use_container_width=True, hide_index=True, height=220
            )
        except Exception:
            st.info("Dynamic Tables not yet refreshed.")

    st.divider()
    st.markdown("##### Ingestion Timeline (rows per minute by source)")
    try:
        timeline = load_ingestion_timeline()
        if len(timeline) > 0:
            timeline_chart = alt.Chart(timeline).mark_area(opacity=0.7).encode(
                x=alt.X("INGESTION_MINUTE:T", title="Time"),
                y=alt.Y("ROWS_INGESTED:Q", title="Rows Ingested", stack=True),
                color=alt.Color("SOURCE:N", scale=alt.Scale(
                    domain=["Orders", "Customers", "Clickstream"],
                    range=["#1565c0", "#43a047", "#f9a825"]
                )),
                tooltip=["SOURCE", "INGESTION_MINUTE:T", "ROWS_INGESTED"]
            ).properties(height=300)
            st.altair_chart(timeline_chart, use_container_width=True)
        else:
            st.info("No ingestion data yet.")
    except Exception:
        st.info("Timeline data not yet available.")

    recent_orders = session.sql("""
        SELECT ORDER_ID, TRUCK_ID, CUSTOMER_ID, ORDER_CHANNEL, ORDER_TOTAL, ORDER_TS, INGESTED_AT
        FROM TASTY_BYTES_DEMO.DEV.RAW_ORDERS
        ORDER BY INGESTED_AT DESC
        LIMIT 10
    """).to_pandas()
    st.markdown("##### Latest 10 Orders (most recent first)")
    st.dataframe(recent_orders, use_container_width=True, hide_index=True)

with tab_sales:
    sales = load_sales()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Revenue", f"${sales['REVENUE'].sum():,.0f}")
    c2.metric("Total Orders", f"{sales['ORDERS'].sum():,.0f}")
    c3.metric("Avg Order Value", f"${sales['AOV'].mean():,.2f}")
    c4.metric("Unique Customers", f"{sales['CUSTOMERS'].sum():,.0f}")

    col1, col2 = st.columns(2)
    with col1:
        tier_chart = alt.Chart(sales.groupby('LOYALTY_TIER', as_index=False)['REVENUE'].sum()).mark_bar().encode(
            x=alt.X('REVENUE:Q', title='Revenue ($)'),
            y=alt.Y('LOYALTY_TIER:N', sort='-x', title='Loyalty Tier'),
            color=alt.Color('LOYALTY_TIER:N', scale=alt.Scale(
                domain=['Platinum', 'Gold', 'Silver', 'Bronze'],
                range=['#1a237e', '#f9a825', '#78909c', '#8d6e63']
            ))
        ).properties(title='Revenue by Loyalty Tier', height=300)
        st.altair_chart(tier_chart, use_container_width=True)

    with col2:
        channel_chart = alt.Chart(sales.groupby('ORDER_CHANNEL', as_index=False)['ORDERS'].sum()).mark_arc().encode(
            theta='ORDERS:Q',
            color=alt.Color('ORDER_CHANNEL:N', title='Channel'),
            tooltip=['ORDER_CHANNEL', 'ORDERS']
        ).properties(title='Orders by Channel', height=300)
        st.altair_chart(channel_chart, use_container_width=True)

    country_chart = alt.Chart(sales.groupby('CUSTOMER_COUNTRY', as_index=False).agg(
        {'REVENUE': 'sum', 'ORDERS': 'sum'}
    )).mark_bar().encode(
        x=alt.X('CUSTOMER_COUNTRY:N', title='Country'),
        y=alt.Y('REVENUE:Q', title='Revenue ($)'),
        color='CUSTOMER_COUNTRY:N',
        tooltip=['CUSTOMER_COUNTRY', 'REVENUE', 'ORDERS']
    ).properties(title='Revenue by Country', height=300)
    st.altair_chart(country_chart, use_container_width=True)

with tab_supply:
    supply = load_supply()
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Margin", f"${supply['MARGIN'].sum():,.0f}")
    c2.metric("Avg Margin %", f"{supply['MARGIN_PCT'].mean():.1f}%")
    c3.metric("Avg Orders/Truck", f"{supply['ORDERS_PER_TRUCK'].mean():.1f}")

    col1, col2 = st.columns(2)
    with col1:
        brand_data = supply.groupby('TRUCK_BRAND_NAME', as_index=False).agg({'MARGIN': 'sum', 'COGS': 'sum'})
        brand_melt = brand_data.melt(id_vars='TRUCK_BRAND_NAME', value_vars=['MARGIN', 'COGS'])
        brand_chart = alt.Chart(brand_melt).mark_bar().encode(
            y=alt.Y('TRUCK_BRAND_NAME:N', title='Brand'),
            x=alt.X('value:Q', title='USD'),
            color=alt.Color('variable:N', scale=alt.Scale(domain=['MARGIN', 'COGS'], range=['#2e7d32', '#c62828'])),
            tooltip=['TRUCK_BRAND_NAME', 'variable', 'value']
        ).properties(title='Margin vs COGS by Brand', height=300)
        st.altair_chart(brand_chart, use_container_width=True)

    with col2:
        cat_chart = alt.Chart(supply.groupby('ITEM_CATEGORY', as_index=False)['MARGIN_PCT'].mean()).mark_bar().encode(
            y=alt.Y('ITEM_CATEGORY:N', sort='-x', title='Category'),
            x=alt.X('MARGIN_PCT:Q', title='Margin %'),
            color=alt.value('#1565c0')
        ).properties(title='Avg Margin % by Category', height=300)
        st.altair_chart(cat_chart, use_container_width=True)

with tab_sustain:
    sust = load_sustainability()
    c1, c2, c3 = st.columns(3)
    c1.metric("Total CO2 (kg/day)", f"{sust['CO2_KG'].sum():,.0f}")
    c2.metric("Revenue/kg CO2", f"${sust['REV_PER_KG_CO2'].mean():,.2f}")
    c3.metric("Est. Waste Cost", f"${sust['WASTE_COST'].sum():,.0f}")

    col1, col2 = st.columns(2)
    with col1:
        co2_chart = alt.Chart(sust.groupby('TRUCK_BRAND_NAME', as_index=False)['CO2_KG'].sum()).mark_bar().encode(
            x=alt.X('TRUCK_BRAND_NAME:N', title='Brand'),
            y=alt.Y('CO2_KG:Q', title='kg CO2/day'),
            color=alt.value('#e65100'),
            tooltip=['TRUCK_BRAND_NAME', 'CO2_KG']
        ).properties(title='CO2 Emissions by Brand', height=300)
        st.altair_chart(co2_chart, use_container_width=True)

    with col2:
        eff_chart = alt.Chart(sust.groupby('TRUCK_BRAND_NAME', as_index=False)['REV_PER_KG_CO2'].mean()).mark_bar().encode(
            x=alt.X('TRUCK_BRAND_NAME:N', title='Brand'),
            y=alt.Y('REV_PER_KG_CO2:Q', title='$/kg CO2'),
            color=alt.value('#2e7d32'),
            tooltip=['TRUCK_BRAND_NAME', 'REV_PER_KG_CO2']
        ).properties(title='Revenue per kg CO2 (higher = greener)', height=300)
        st.altair_chart(eff_chart, use_container_width=True)

with tab_click:
    click = load_clickstream()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Page Views", f"{click['VIEWS'].sum():,.0f}")
    c2.metric("Unique Visitors", f"{click['VISITORS'].sum():,.0f}")
    c3.metric("Avg Session (min)", f"{click['AVG_SESSION'].mean():.1f}")
    c4.metric("Avg Yearly Spend", f"${click['AVG_SPEND'].mean():,.0f}")

    col1, col2 = st.columns(2)
    with col1:
        device_chart = alt.Chart(click.groupby('DEVICE_TYPE', as_index=False)['VIEWS'].sum()).mark_arc().encode(
            theta='VIEWS:Q',
            color='DEVICE_TYPE:N',
            tooltip=['DEVICE_TYPE', 'VIEWS']
        ).properties(title='Page Views by Device', height=300)
        st.altair_chart(device_chart, use_container_width=True)

    with col2:
        ref_chart = alt.Chart(click.groupby('REFERRER', as_index=False)['VISITORS'].sum()).mark_bar().encode(
            y=alt.Y('REFERRER:N', sort='-x', title='Referrer'),
            x=alt.X('VISITORS:Q', title='Unique Visitors'),
            color=alt.value('#7b1fa2'),
            tooltip=['REFERRER', 'VISITORS']
        ).properties(title='Visitors by Referrer', height=300)
        st.altair_chart(ref_chart, use_container_width=True)

    membership_data = click.groupby('MEMBERSHIP_LEVEL', as_index=False).agg({'AVG_SPEND': 'mean', 'APP_TIME': 'mean', 'WEB_TIME': 'mean'})
    spend_chart = alt.Chart(membership_data).mark_bar().encode(
        x=alt.X('MEMBERSHIP_LEVEL:N', sort=['Free', 'Silver', 'Gold', 'Platinum'], title='Membership'),
        y=alt.Y('AVG_SPEND:Q', title='Avg Yearly Spend ($)'),
        color=alt.Color('MEMBERSHIP_LEVEL:N', scale=alt.Scale(
            domain=['Free', 'Silver', 'Gold', 'Platinum'],
            range=['#8d6e63', '#78909c', '#f9a825', '#1a237e']
        )),
        tooltip=['MEMBERSHIP_LEVEL', 'AVG_SPEND']
    ).properties(title='Avg Yearly Spend by Membership Level', height=300)
    st.altair_chart(spend_chart, use_container_width=True)

    page_chart = alt.Chart(click.groupby('PAGE_URL', as_index=False)['VIEWS'].sum()).mark_bar().encode(
        y=alt.Y('PAGE_URL:N', sort='-x', title='Page'),
        x=alt.X('VIEWS:Q', title='Page Views'),
        color=alt.value('#00695c'),
        tooltip=['PAGE_URL', 'VIEWS']
    ).properties(title='Most Visited Pages', height=350)
    st.altair_chart(page_chart, use_container_width=True)

with tab_dq:
    st.subheader("Data Quality Monitoring — DMF Results")
    st.caption("Powered by Snowflake Data Metric Functions running on Iceberg tables")
    try:
        dq = load_dq()
        if len(dq) > 0:
            failures = dq[dq['METRIC_VALUE'] > 0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Checks", len(dq))
            c2.metric("Passing", len(dq[dq['METRIC_VALUE'] == 0]))
            c3.metric("Flagged", len(failures), delta=f"-{len(failures)}" if len(failures) > 0 else "0", delta_color="inverse")
            st.dataframe(dq, use_container_width=True, height=400)
        else:
            st.info("No DMF results yet — metrics run on schedule after data changes.")
    except Exception as e:
        st.info(f"DMF results not yet available: {e}")

with tab_cost:
    st.subheader("Cost Governance — Resource Monitors & Credit Usage")
    st.caption("Real-time visibility into credit consumption, budget thresholds, and warehouse efficiency")

    @st.cache_data(ttl=REFRESH_INTERVAL)
    def load_resource_monitors():
        return session.sql("""
            SELECT "name" AS MONITOR_NAME,
                   "credit_quota" AS CREDIT_QUOTA,
                   "used_credits" AS USED_CREDITS,
                   "remaining_credits" AS REMAINING_CREDITS,
                   "frequency" AS FREQUENCY,
                   "level" AS LEVEL,
                   "start_time" AS START_TIME,
                   "end_time" AS END_TIME
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        """).to_pandas()

    @st.cache_data(ttl=60)
    def load_monitor_data():
        session.sql("SHOW RESOURCE MONITORS").collect()
        return session.sql("""
            SELECT "name" AS MONITOR_NAME,
                   "credit_quota" AS CREDIT_QUOTA,
                   "used_credits" AS USED_CREDITS,
                   "remaining_credits" AS REMAINING_CREDITS,
                   "frequency" AS FREQUENCY,
                   "level" AS LEVEL
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        """).to_pandas()

    @st.cache_data(ttl=60)
    def load_wh_usage_7d():
        return session.sql("""
            SELECT
                WAREHOUSE_NAME,
                DATE_TRUNC('HOUR', START_TIME) AS USAGE_HOUR,
                SUM(CREDITS_USED) AS CREDITS
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD('DAY', -7, CURRENT_TIMESTAMP())
            GROUP BY 1, 2
            ORDER BY 2
        """).to_pandas()

    @st.cache_data(ttl=60)
    def load_credit_summary():
        return session.sql("""
            SELECT
                WAREHOUSE_NAME,
                SUM(CREDITS_USED) AS TOTAL_CREDITS,
                COUNT(DISTINCT DATE_TRUNC('DAY', START_TIME)) AS ACTIVE_DAYS,
                ROUND(SUM(CREDITS_USED) / NULLIF(COUNT(DISTINCT DATE_TRUNC('DAY', START_TIME)), 0), 2) AS CREDITS_PER_DAY
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD('DAY', -30, CURRENT_TIMESTAMP())
            GROUP BY 1
            ORDER BY TOTAL_CREDITS DESC
        """).to_pandas()

    @st.cache_data(ttl=60)
    def load_daily_spend():
        return session.sql("""
            SELECT
                DATE_TRUNC('DAY', START_TIME)::DATE AS SPEND_DATE,
                SUM(CREDITS_USED) AS DAILY_CREDITS,
                SUM(CREDITS_USED) * 3.0 AS EST_COST_USD
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD('DAY', -30, CURRENT_TIMESTAMP())
            GROUP BY 1
            ORDER BY 1
        """).to_pandas()

    try:
        monitors = load_monitor_data()
        if len(monitors) > 0:
            st.markdown("##### Resource Monitors")
            for _, row in monitors.iterrows():
                name = row["MONITOR_NAME"]
                quota = float(row["CREDIT_QUOTA"] or 0)
                used = float(row["USED_CREDITS"] or 0)
                remaining = float(row["REMAINING_CREDITS"] or 0)
                pct = (used / quota * 100) if quota > 0 else 0

                with st.container():
                    mc1, mc2, mc3, mc4 = st.columns(4)
                    mc1.metric(f"{name}", f"{row['LEVEL']} / {row['FREQUENCY']}")
                    mc2.metric("Used / Quota", f"{used:.1f} / {quota:.0f}")
                    mc3.metric("Remaining", f"{remaining:.1f}", delta=f"{100-pct:.0f}% left", delta_color="normal" if pct < 80 else "inverse")
                    mc4.metric("Usage", f"{pct:.0f}%", delta="OK" if pct < 80 else "WARNING" if pct < 95 else "CRITICAL", delta_color="normal" if pct < 80 else "inverse")

                    st.progress(min(pct / 100, 1.0))
            st.divider()
        else:
            st.info("No resource monitors found. Run the notebook §5b cells to create them.")
    except Exception:
        st.info("Resource monitors not yet configured. Run the notebook §5b cells first.")

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("##### Credit Usage by Warehouse (Last 30 Days)")
        try:
            summary = load_credit_summary()
            if len(summary) > 0:
                summary_chart = alt.Chart(summary).mark_bar().encode(
                    y=alt.Y("WAREHOUSE_NAME:N", sort="-x", title="Warehouse"),
                    x=alt.X("TOTAL_CREDITS:Q", title="Total Credits"),
                    color=alt.condition(
                        alt.datum.TOTAL_CREDITS > 50,
                        alt.value("#c62828"),
                        alt.value("#1565c0")
                    ),
                    tooltip=["WAREHOUSE_NAME", "TOTAL_CREDITS", "CREDITS_PER_DAY", "ACTIVE_DAYS"]
                ).properties(height=300)
                st.altair_chart(summary_chart, use_container_width=True)
            else:
                st.info("No warehouse usage data available yet.")
        except Exception:
            st.info("Warehouse metering data not yet available (ACCOUNT_USAGE has ~2hr latency).")

    with col_right:
        st.markdown("##### Daily Credit Spend (Last 30 Days)")
        try:
            daily = load_daily_spend()
            if len(daily) > 0:
                daily_chart = alt.Chart(daily).mark_area(opacity=0.7, color="#1a237e").encode(
                    x=alt.X("SPEND_DATE:T", title="Date"),
                    y=alt.Y("DAILY_CREDITS:Q", title="Credits"),
                    tooltip=["SPEND_DATE:T", "DAILY_CREDITS", "EST_COST_USD"]
                ).properties(height=300)

                budget_line = alt.Chart(pd.DataFrame({"y": [500/30]})).mark_rule(
                    color="#c62828", strokeDash=[5, 5]
                ).encode(y="y:Q")

                st.altair_chart(daily_chart + budget_line, use_container_width=True)
                st.caption("Red dashed line = daily budget target (500 credits / 30 days)")
            else:
                st.info("No daily spend data available yet.")
        except Exception:
            st.info("Daily spend data not yet available.")

    st.markdown("##### Hourly Credit Usage by Warehouse (Last 7 Days)")
    try:
        wh_usage = load_wh_usage_7d()
        if len(wh_usage) > 0:
            hourly_chart = alt.Chart(wh_usage).mark_area(opacity=0.6).encode(
                x=alt.X("USAGE_HOUR:T", title="Time"),
                y=alt.Y("CREDITS:Q", title="Credits", stack=True),
                color=alt.Color("WAREHOUSE_NAME:N", title="Warehouse"),
                tooltip=["WAREHOUSE_NAME", "USAGE_HOUR:T", "CREDITS"]
            ).properties(height=300)
            st.altair_chart(hourly_chart, use_container_width=True)
        else:
            st.info("No hourly usage data available yet.")
    except Exception:
        st.info("Hourly usage data not yet available.")

    st.markdown("##### Cost Governance Checklist")
    st.markdown("""
    | Control | Status | Detail |
    |---------|--------|--------|
    | Account-level resource monitor | Configured | 500 credits/month, notify at 50%/75%, suspend at 90%/100% |
    | Warehouse-level resource monitor | Configured | COMPUTE_WH: 100 credits/week, notify at 50%/80%, suspend at 95%/100% |
    | AUTO_SUSPEND | Enabled | 60 seconds on all warehouses — zero idle cost |
    | AUTO_RESUME | Enabled | Warehouses wake on demand — no manual intervention |
    | Warehouse size | XSMALL | Right-sized for demo; scale up as needed for 1,120 trucks |
    """)

st.divider()
st.caption("Built on: Snowpipe Streaming v2 → Iceberg Tables → Dynamic Tables → Horizon Governance → Snowflake Tasks | Streamlit in Snowflake")

if auto_refresh:
    time.sleep(REFRESH_INTERVAL)
    st.session_state.refresh_count += 1
    st.rerun()
