import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px

# ===============================
# DB CONFIG
# ===============================
DB_NAME = "portfolio"
DB_USER = "postgres"
DB_PASSWORD = ""
DB_HOST = "localhost"
DB_PORT = "5432"

def get_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

st.set_page_config(page_title="Portfolio Dashboard", layout="wide")
st.title("📊 Portfolio Dashboard")

conn = get_connection()

# ===============================
# FIFO REALIZED P&L
# ===============================
st.header("💰 FIFO Realized P&L")

fifo_query = """
WITH fifo_matches AS (
   WITH buys AS (
    SELECT
        trade_id AS buy_id,
        stock_id,
        trade_date,
        quantity,
        price,
        SUM(quantity) OVER (
            PARTITION BY stock_id
            ORDER BY trade_date, trade_id
        ) AS buy_cum_qty
    FROM trades
    WHERE side = 'BUY'
),
sells AS (
    SELECT
        trade_id AS sell_id,
        stock_id,
        trade_date,
        quantity,
        price,
        SUM(quantity) OVER (
            PARTITION BY stock_id
            ORDER BY trade_date, trade_id
        ) AS sell_cum_qty
    FROM trades
    WHERE side = 'SELL'
)
SELECT
    b.stock_id,
    LEAST(b.buy_cum_qty, s.sell_cum_qty)
    - GREATEST(b.buy_cum_qty - b.quantity, s.sell_cum_qty - s.quantity)
        AS matched_qty,
    b.price AS buy_price,
    s.price AS sell_price
FROM buys b
JOIN sells s
  ON b.stock_id = s.stock_id
 AND b.buy_cum_qty > s.sell_cum_qty - s.quantity
 AND b.buy_cum_qty - b.quantity < s.sell_cum_qty
)
SELECT
    s.ticker,
    SUM((sell_price - buy_price) * matched_qty) AS realized_pnl
FROM fifo_matches f
JOIN stocks s ON f.stock_id = s.stock_id
GROUP BY s.ticker;
"""

fifo_df = pd.read_sql(fifo_query, conn)
st.dataframe(fifo_df, use_container_width=True)
st.metric("Total Realized P&L", round(fifo_df["realized_pnl"].sum(), 2))

# ===============================
# UNREALIZED P&L
# ===============================
st.header("📈 Unrealized P&L")

unrealized_query = """
WITH holdings AS (
    SELECT
        stock_id,
        SUM(
            CASE
                WHEN side = 'BUY' THEN quantity
                WHEN side = 'SELL' THEN -quantity
            END
        ) AS net_qty
    FROM trades
    GROUP BY stock_id
),
avg_buy AS (
    SELECT
        stock_id,
        SUM(quantity * price) / SUM(quantity) AS avg_buy_price
    FROM trades
    WHERE side = 'BUY'
    GROUP BY stock_id
),
latest_prices AS (
    SELECT
        p1.stock_id,
        p1.close AS latest_price
    FROM prices p1
    WHERE p1.price_date = (
        SELECT MAX(p2.price_date)
        FROM prices p2
        WHERE p2.stock_id = p1.stock_id
    )
)
SELECT
    s.ticker,
    h.net_qty,
    lp.latest_price,
    ab.avg_buy_price,
    (lp.latest_price - ab.avg_buy_price) * h.net_qty AS unrealized_pnl
FROM holdings h
JOIN avg_buy ab ON h.stock_id = ab.stock_id
JOIN latest_prices lp ON h.stock_id = lp.stock_id
JOIN stocks s ON s.stock_id = h.stock_id;
"""

unrealized_df = pd.read_sql(unrealized_query, conn)
st.dataframe(unrealized_df, use_container_width=True)
st.metric("Total Unrealized P&L", round(unrealized_df["unrealized_pnl"].sum(), 2))

# ===============================
# TRADE LEDGER
# ===============================
st.header("📜 Trade Ledger")

trade_query = """
SELECT
    t.trade_id,
    s.ticker,
    t.trade_date,
    t.side,
    t.quantity,
    t.price,
    t.fees
FROM trades t
JOIN stocks s ON t.stock_id = s.stock_id
ORDER BY t.trade_date DESC;
"""

trade_df = pd.read_sql(trade_query, conn)
st.dataframe(trade_df, use_container_width=True)

# ===============================
# STOCK PERFORMANCE
# ===============================
st.header("📈 Stock Performance")

stock_list = trade_df["ticker"].unique()
selected_stock = st.selectbox("Select Stock", stock_list)

price_query = f"""
SELECT
    p.price_date,
    p.close
FROM prices p
JOIN stocks s ON p.stock_id = s.stock_id
WHERE s.ticker = '{selected_stock}'
ORDER BY p.price_date;
"""

price_df = pd.read_sql(price_query, conn)
fig_price = px.line(price_df, x="price_date", y="close", title=f"{selected_stock} Price")
st.plotly_chart(fig_price, use_container_width=True)

# ===============================
# HOLDINGS
# ===============================
holding_query = f"""
SELECT
    SUM(
        CASE
            WHEN side = 'BUY' THEN quantity
            WHEN side = 'SELL' THEN -quantity
        END
    ) AS net_quantity
FROM trades t
JOIN stocks s ON t.stock_id = s.stock_id
WHERE s.ticker = '{selected_stock}';
"""

holding_df = pd.read_sql(holding_query, conn)
st.metric("Net Quantity", int(holding_df.iloc[0, 0]))

# ===============================
# PORTFOLIO VALUE OVER TIME
# ===============================
st.header("📊 Portfolio Performance")

portfolio_query = """
WITH daily_positions AS (
    SELECT
        t.stock_id,
        p.price_date,
        SUM(
            CASE
                WHEN t.side = 'BUY' THEN t.quantity
                WHEN t.side = 'SELL' THEN -t.quantity
            END
        ) AS net_qty
    FROM trades t
    JOIN prices p 
      ON p.stock_id = t.stock_id
     AND p.price_date >= t.trade_date
    GROUP BY t.stock_id, p.price_date
)
SELECT
    dp.price_date,
    SUM(dp.net_qty * p.close) AS portfolio_value
FROM daily_positions dp
JOIN prices p
  ON dp.stock_id = p.stock_id
 AND dp.price_date = p.price_date
GROUP BY dp.price_date
ORDER BY dp.price_date;
"""

portfolio_df = pd.read_sql(portfolio_query, conn)

# Daily P&L
portfolio_df["daily_pnl"] = portfolio_df["portfolio_value"].diff()

# Drawdown
portfolio_df["peak"] = portfolio_df["portfolio_value"].cummax()
portfolio_df["drawdown"] = (
    portfolio_df["portfolio_value"] - portfolio_df["peak"]
) / portfolio_df["peak"]

fig_equity = px.line(portfolio_df, x="price_date", y="portfolio_value", title="Equity Curve")
st.plotly_chart(fig_equity, use_container_width=True)

fig_pnl = px.bar(portfolio_df, x="price_date", y="daily_pnl", title="Daily P&L")
st.plotly_chart(fig_pnl, use_container_width=True)

fig_dd = px.area(portfolio_df, x="price_date", y="drawdown", title="Drawdown")
st.plotly_chart(fig_dd, use_container_width=True)

conn.close()

