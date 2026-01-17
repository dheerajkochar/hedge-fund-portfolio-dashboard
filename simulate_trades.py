import psycopg2
import random
from datetime import datetime, timedelta

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

def get_latest_price(cursor, stock_id):
    cursor.execute("""
        SELECT close
        FROM prices
        WHERE stock_id = %s
        ORDER BY price_date DESC
        LIMIT 1
    """, (stock_id,))
    result = cursor.fetchone()
    return float(result[0]) if result else None

def get_all_stocks(cursor):
    cursor.execute("SELECT stock_id, ticker FROM stocks")
    return cursor.fetchall()

def simulate_trade(cursor, stock_id, ticker):
    price = get_latest_price(cursor, stock_id)

    # HARD SAFETY CHECK
    if price is None:
        print(f"⚠️ No price found for {ticker}, skipping.")
        return

    side = random.choice(["BUY", "SELL"])
    quantity = random.randint(1, 50)

    # Slippage
    price = price * random.uniform(0.995, 1.005)

    fees = round(price * quantity * 0.001, 2)

    # Spread trades across last 6 months
    trade_date = datetime.today().date() - timedelta(days=random.randint(0, 180))

    cursor.execute("""
        INSERT INTO trades (stock_id, trade_date, side, quantity, price, fees)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (stock_id, trade_date, side, quantity, price, fees))

def main():
    conn = get_connection()
    cursor = conn.cursor()

    stocks = get_all_stocks(cursor)

    for stock_id, ticker in stocks:
        n_trades = random.randint(20, 40)  # HIGH VOLUME

        for _ in range(n_trades):
            simulate_trade(cursor, stock_id, ticker)

        print(f"✅ Generated {n_trades} trades for {ticker}")

    conn.commit()
    cursor.close()
    conn.close()

    print("🔥 Trade simulation complete.")

if __name__ == "__main__":
    main()

