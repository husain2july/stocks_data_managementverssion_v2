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
# ============================================
# SMALL CAP STOCKS (251-1500 by Market Cap)
# ============================================
SMALL_CAP_STOCKS = [
    # Nifty Smallcap 250 + Additional stocks
    'AAVAS.NS', 'ACE.NS', 'ADANIENSOL.NS', 'AMBER.NS', 'ANURAS.NS',
    'APPLEINDS.NS', 'ARVINDFASN.NS', 'ASIANHOTNR.NS', 'BAJAJHLDNG.NS', 'BANCOINDIA.NS',
    'BBL.NS', 'BEARDSELL.NS', 'BEPL.NS', 'BHEDADEQP.NS', 'BHUSANSTL.NS',
    'BINDALAGRO.NS', 'CHOLAHLDNG.NS', 'CLEAN.NS', 'COCHIN.NS', 'COCHINSHIP.NS',
    'COLLAR.NS', 'COMPINFO.NS', 'CONCORDBIO.NS', 'CONFIPET.NS', 'COSPOWER.NS',
    'CPSEETEC.NS', 'CRAFTSMAN.NS', 'CREDITACC.NS', 'CRISIL.NS', 'CSB.NS',
    'CUPID.NS', 'CYBERMEDIA.NS', 'CYIENT.NS', 'DADRAPHARM.NS', 'DATAPATTNS.NS',
    'DCBBANK.NS', 'DCMSHRIRAM.NS', 'DEEPAKFERT.NS', 'DESICAL.NS', 'DHANI.NS',
    'DHANUKA.NS', 'DHARSUGAR.NS', 'DHUNINV.NS', 'DIAMINESQ.NS', 'DICIND.NS',
    'DIGISPICE.NS', 'DLINKINDIA.NS', 'DOLLAR.NS', 'DOVEMARINE.NS', 'DPWIRES.NS',
    'DREDGECORP.NS', 'DUCON.NS', 'DYCL.NS', 'DYNAMATECH.NS', 'EASEMYTRIP.NS',
    'EASTSILK.NS', 'ECLERX.NS', 'EDELWEISS.NS', 'EIDPARRY.NS', 'EIHOTEL.NS',
    'EKC.NS', 'ELGIEQUIP.NS', 'EMAMILTD.NS', 'EMKAY.NS', 'EMMBI.NS',
    'ENDURANCE.NS', 'ENERGYDEV.NS', 'ENGINERSIN.NS', 'ENTERO.NS', 'EPL.NS',
    'EQUITAS.NS', 'EQUITASBNK.NS', 'ERIS.NS', 'EROSMEDIA.NS', 'ESCORT.NS',
    'ESSDEE.NS', 'ESTER.NS', 'EUROTEXIND.NS', 'EVEREADY.NS', 'EXCELINDUS.NS',
    'FAIRCHEM.NS', 'FAIRFIN.NS', 'FCL.NS', 'FCONSUMER.NS', 'FDC.NS',
    'FIEMIND.NS', 'FILATEX.NS', 'FINCABLES.NS', 'FINPIPE.NS', 'FLAIR.NS',
    'FLEXITUFF.NS', 'FLUOROCHEM.NS', 'FMNL.NS', 'FORCEMOT.NS', 'FORTISONICS.NS',
    'FREEDOM.NS', 'FSC.NS', 'FSL.NS', 'GABRIEL.NS', 'GAEL.NS',
    'GALAXYSURF.NS', 'GALLANTT.NS', 'GANDHITUBE.NS', 'GARFIBRES.NS', 'GARNETINT.NS',
    'GATEWAY.NS', 'GDL.NS', 'GEECEE.NS', 'GENCON.NS', 'GENESYS.NS',
    'GESHIP.NS', 'GHCL.NS', 'GICHSGFIN.NS', 'GILLANDERS.NS', 'GILLETTE.NS',
    'GINNIFILA.NS', 'GIPCL.NS', 'GKBOPTICAL.NS', 'GKW.NS', 'GLAXO.NS',
    'GLOBAL.NS', 'GLOBALPET.NS', 'GLOBE.NS', 'GLOBUSSPR.NS', 'GMBREW.NS',
    'GMDCLTD.NS', 'GMMPFAUDLR.NS', 'GOACARBON.NS', 'GOCLCORP.NS', 'GODFRYPHLP.NS',
    'GODREJAGRO.NS', 'GOKEX.NS', 'GOKUL.NS', 'GOLD.NS', 'GOODLUCK.NS',
    'GOODYEAR.NS', 'GPIL.NS', 'GPPL.NS', 'GRAPHITE.NS', 'GREAVESCOT.NS',
    'GREENLAM.NS', 'GREENPANEL.NS', 'GREENPLY.NS', 'GRINDWELL.NS', 'GRSE.NS',
    'GRUH.NS', 'GSFC.NS', 'GSHIP.NS', 'GSS.NS', 'GTLINFRA.NS',
    'GTL.NS', 'GTPL.NS', 'GUFICBIO.NS', 'GUJALKALI.NS', 'GUJAPOLLO.NS',
    'GUJGAS.NS', 'GULFOILLUB.NS', 'GULFPETRO.NS', 'GVKPIL.NS', 'HAPPSTMNDS.NS',
    'HATHWAY.NS', 'HCC.NS', 'HCG.NS', 'HCLINFOSYS.NS', 'HCL-INSYS.NS',
    'HEG.NS', 'HEIDELBERG.NS', 'HERANBA.NS', 'HERCULES.NS', 'HERITGFOOD.NS',
    'HESTERBIO.NS', 'HEXAWARE.NS', 'HFCL.NS', 'HGINFRA.NS', 'HIKAL.NS',
    'HIL.NS', 'HIMATSEIDE.NS', 'HINDCOMPOS.NS', 'HINDDORROL.NS', 'HINDMOTOR.NS',
    'HINDNATGLS.NS', 'HINDOILEXP.NS', 'HINDSANGAM.NS', 'HINDTANAC.NS', 'HINDWARE.NS',
    'HINDSYN.NS', 'HIRECT.NS', 'HISARMETAL.NS', 'HITECH.NS', 'HITECHCORP.NS',
    'HITECHGEAR.NS', 'HMT.NS', 'HMVL.NS', 'HNDFDS.NS', 'HONEY.NS',
    'HOTELEELA.NS', 'HOVS.NS', 'HPL.NS', 'HSCL.NS', 'HTMEDIA.NS',
    'HUBTOWN.NS', 'HUHTAMAKI.NS', 'HYDRAB.NS', 'HYSTEEL.NS', 'IAF.NS',
    'IBREALEST.NS', 'IBULHSGFIN.NS', 'ICEMAKE.NS', 'ICICIBANKP.NS', 'ICICIBIO.NS',
    'ICICICARFIN.NS', 'ICICIM.NS', 'ICICIMF.NS', 'ICICIPRUD.NS', 'ICICIPRU.NS',
    'ICIL.NS', 'ICRA.NS', 'ICRACD.NS', 'ICSA.NS', 'IDEAFORGE.NS',
    'IDFC.NS', 'IDFCBANK.NS', 'IDFCLIM.NS', 'IDFCMF.NS', 'IDFNL.NS',
    'IDFSECURITIES.NS', 'IDNTHSUG.NS', 'IFBAGRO.NS', 'IFBIND.NS', 'IFCI.NS',
    'IFGL.NS', 'IFGLEXP.NS', 'IFL.NS', 'IGARASHI.NS', 'IGPL.NS',
    'IIB.NS', 'IIFL.NS', 'IIFLSEC.NS', 'IIFLW.NS', 'IIHFL.NS',
    'IITL.NS', 'IL&FSENGG.NS', 'IL&FSTR.NS', 'IL&FSTHQ.NS', 'IL&FSWLTD.NS',
    'IMAGICAA.NS', 'IMFA.NS', 'IMPAL.NS', 'IMPEXFERRO.NS', 'INDBANK.NS',
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
