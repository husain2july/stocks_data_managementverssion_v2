import os
import sqlite3
import logging
from datetime import datetime
import pytz
import yfinance as yf
import pandas as pd
from tabulate import tabulate


# ----------------------------
# CONFIGURATIONS
# ----------------------------
DB_NAME = "nifty50_top20_v1.db"
README_FILE = "README_v1.md"

# Top 20 NIFTY50 stocks (symbols must match Yahoo Finance format, ".NS" for NSE India)
STOCKS = [
'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS',
    'HINDUNILVR.NS', 'ITC.NS', 'SBIN.NS', 'BHARTIARTL.NS', 'KOTAKBANK.NS',
    'BAJFINANCE.NS', 'LT.NS', 'ASIANPAINT.NS', 'HCLTECH.NS', 'AXISBANK.NS',
    'MARUTI.NS', 'SUNPHARMA.NS', 'TITAN.NS', 'ULTRACEMCO.NS', 'NESTLEIND.NS',
    'BAJAJFINSV.NS', 'WIPRO.NS', 'ADANIENT.NS', 'ONGC.NS', 'NTPC.NS',
    'TECHM.NS', 'POWERGRID.NS', 'M&M.NS', 'TATAMOTORS.NS', 'TATASTEEL.NS',
    'INDUSINDBK.NS', 'DIVISLAB.NS', 'BAJAJ-AUTO.NS', 'DRREDDY.NS', 'JSWSTEEL.NS',
    'BRITANNIA.NS', 'CIPLA.NS', 'APOLLOHOSP.NS', 'EICHERMOT.NS', 'GRASIM.NS',
    'HINDALCO.NS', 'COALINDIA.NS', 'BPCL.NS', 'HEROMOTOCO.NS', 'TATACONSUM.NS',
    'ADANIPORTS.NS', 'SBILIFE.NS', 'HDFCLIFE.NS', 'UPL.NS', 'SHREECEM.NS',
    'PIDILITIND.NS', 'GODREJCP.NS', 'DABUR.NS', 'BERGEPAINT.NS', 'MARICO.NS',
    'COLPAL.NS', 'MCDOWELL-N.NS', 'HAVELLS.NS', 'BOSCHLTD.NS', 'SIEMENS.NS',
    'ABB.NS', 'VEDL.NS', 'HINDZINC.NS', 'BANKBARODA.NS', 'PNB.NS',
    'CANBK.NS', 'UNIONBANK.NS', 'IDFCFIRSTB.NS', 'BANDHANBNK.NS', 'FEDERALBNK.NS',
    'IDEA.NS', 'ZEEL.NS', 'DLF.NS', 'GODREJPROP.NS', 'OBEROIRLTY.NS',
    'AMBUJACEM.NS', 'ACC.NS', 'GAIL.NS', 'IOC.NS', 'PETRONET.NS',
    'MRF.NS', 'BALKRISIND.NS', 'CUMMINSIND.NS', 'TORNTPHARM.NS', 'LUPIN.NS',
    'BIOCON.NS', 'AUROPHARMA.NS', 'CADILAHC.NS', 'GLENMARK.NS', 'ALKEM.NS',
    'TRENT.NS', 'ABFRL.NS', 'PAGEIND.NS', 'PVR.NS', 'JUBLFOOD.NS',
    'MPHASIS.NS', 'LTTS.NS', 'COFORGE.NS', 'PERSISTENT.NS', 'MINDTREE.NS',
]

# Setup logging
logging.basicConfig(
    filename="data_fetch_v1.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# IST timezone
IST = pytz.timezone("Asia/Kolkata")


# ----------------------------
# DATABASE FUNCTIONS
# ----------------------------
def init_db():
    """Ensure database exists with tables for each stock."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for stock in STOCKS:
        # table_name = stock.replace(".NS", ".NS")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS '{stock}' (
                datetime TEXT PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER
            )
        """)
    conn.commit()
    conn.close()


def insert_data(stock, df):
    """Insert stock data into database with ON CONFLICT IGNORE to avoid duplicates."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Convert datetime column to string
    df["datetime"] = df["datetime"].astype(str)
    df["volume"] = df["volume"].astype(int)
    print("My data is \n",df.head(2))

    rows = df[["datetime", "open", "high", "low", "close", "volume"]].values.tolist()
    print("rows", rows[:2])

    cursor.executemany(
        f"""
        INSERT OR IGNORE INTO '{stock}' (datetime, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows
    )

    conn.commit()
    conn.close()
    logging.info(f"[{stock}] Inserted {len(rows)} rows (duplicates ignored)")

# ----------------------------
# DATA FETCHING
# ----------------------------
def fetch_stock_data(stock):
    """Fetch 1-min data for the past 15 minutes for a stock."""
    try:
        df = yf.download(
            tickers=stock,
            interval="1m",
            period="1d",
            progress=True)

        
        if df.empty:
            logging.warning(f"No data returned for {stock}")
            return None

        # Reset index to get datetime as column
        df = df.droplevel('Ticker', axis=1)
        df.reset_index(inplace=True)

        # Convert timezone to IST
        df["Datetime"] = df["Datetime"].dt.tz_convert(IST)
        
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype(int)
        df["Volume"] = df["Volume"] * 1 

        df.rename(columns={
            "Datetime": "datetime",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        }, inplace=True)

        # df['volume'] = df['volume'].apply(lambda x: int.from_bytes(x, byteorder='little', signed=False))
        
        

        return df[["datetime", "open", "high", "low", "close", "volume"]]

    except Exception as e:
        logging.error(f"Error fetching data for {stock}: {e}")
        return None


# ----------------------------
# README UPDATE
# ----------------------------
def update_readme():
    """Append last 2 rows from each stock table to README.md using HTML table."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    with open(README_FILE, "w", encoding="utf-8") as f:
        f.write("# 📈 NIFTY50 Top 20 Data Snapshot\n\n")
        f.write(f"Last updated: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S %Z')}\n\n")

        for stock in STOCKS:
            table_name = stock.replace(".NS", ".NS")
            try:
                df = pd.read_sql_query(
                    f"SELECT datetime, close, volume FROM '{table_name}' ORDER BY datetime DESC LIMIT 2", conn
                )
                if df.empty:
                    continue

                # Write HTML table
                f.write(f"## {stock}\n\n")
                f.write('<table>\n')
                f.write('  <tr><th>Datetime</th><th>Close</th><th>Volume</th></tr>\n')
                for _, row in df.iterrows():
                    f.write(f"  <tr><td>{row['datetime']}</td><td>{row['close']}</td><td>{row['volume']}</td></tr>\n")
                f.write('</table>\n\n')
            except Exception as e:
                logging.error(f"Error updating README for {stock}: {e}")

    conn.close()


# ----------------------------
# MAIN WORKFLOW
# ----------------------------
def main():
    logging.info("Starting data fetch cycle...")
    init_db()

    for stock in STOCKS:
        df = fetch_stock_data(stock)
        if df is not None and not df.empty:
            insert_data(stock, df)
            logging.info(f"Inserted {len(df)} rows for {stock}")
        else:
            logging.warning(f"No data to insert for {stock}")

    update_readme()
    logging.info("Cycle complete. README updated.")


if __name__ == "__main__":
    main()
