import yfinance as yf
import psycopg2
import pandas as pd
from datetime import datetime, timedelta

# ---------------- CONFIG ----------------
TICKERS = ["AAPL", "TSLA", "GOOG","ANANTRAJ.NS"]
DAYS_BACK = 40

DB_NAME = "portfolio"   # or market_data
DB_USER = "postgres"
DB_PASSWORD = ""
DB_HOST = "localhost"
DB_PORT = "5432"

# ---------------- DB CONNECTION ----------------
def get_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
print("Database connection established.")
# ---------------- GET STOCK ID ----------------
def get_stock_id(cursor, ticker):
    cursor.execute("SELECT stock_id FROM stocks WHERE ticker = %s", (ticker,))
    result = cursor.fetchone()
    return result[0] if result else None

# ---------------- INSERT PRICE DATA ----------------
def insert_prices(cursor, stock_id, df):
    insert_query = """
    INSERT INTO prices (stock_id, price_date, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (stock_id, price_date) DO NOTHING;
    """

    for row in df.itertuples(index=False):
        cursor.execute(insert_query, (
            stock_id,
            row[0],   # Date
            float(row[1]),  # Open
            float(row[2]),  # High
            float(row[3]),  # Low
            float(row[4]),  # Close
            int(row[5])     # Volume
        ))

  
# ---------------- MAIN ----------------
def main():
    conn = get_connection()
    cursor = conn.cursor()

    end_date = datetime.today()
    start_date = end_date - timedelta(days=DAYS_BACK)

    for ticker in TICKERS:
        print(f"Fetching {ticker}...")

        stock_id = get_stock_id(cursor, ticker)
        if not stock_id:
            print(f"{ticker} not found in stocks table!")
            continue

        df = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False)
        df.reset_index(inplace=True)
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
 
        insert_prices(cursor, stock_id, df)
        print(f"Inserted prices for {ticker}")

    conn.commit()
    cursor.close()
    conn.close()
    print("Price loading complete!")

if __name__ == "__main__":
    main()

