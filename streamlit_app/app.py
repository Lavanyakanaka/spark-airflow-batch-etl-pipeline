import pandas as pd
import psycopg2
import streamlit as st


def get_connection():
    conn = psycopg2.connect(
        host="warehouse-postgres",
        port=5432,
        dbname="warehouse",
        user="warehouse",
        password="warehouse",
    )
    return conn


def load_data():
    conn = get_connection()
    df = pd.read_sql(
        "SELECT metric_date, daily_active_users, total_revenue, "
        "top_product_id, top_product_revenue "
        "FROM daily_metrics ORDER BY metric_date",
        conn,
    )
    conn.close()
    return df


def main():
    st.title("User Events Analytics Dashboard")

    df = load_data()
    if df.empty:
        st.warning("No data available yet.")
        return

    latest = df.sort_values("metric_date").iloc[-1]

    col1, col2, col3 = st.columns(3)
    col1.metric("Daily Active Users", int(latest["daily_active_users"]))
    col2.metric("Total Revenue", float(latest["total_revenue"]))
    col3.metric(
        "Top Product Revenue",
        float(latest["top_product_revenue"] or 0.0),
    )

    st.subheader("Daily Active Users Over Time")
    st.line_chart(df.set_index("metric_date")["daily_active_users"])

    st.subheader("Daily Revenue Over Time")
    st.bar_chart(df.set_index("metric_date")["total_revenue"])

    st.subheader("Raw Metrics")
    st.dataframe(df)


if __name__ == "__main__":
    main()
