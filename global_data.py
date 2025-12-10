import yfinance as yf
import pandas as pd
from datetime import datetime

# Tickers
TICKERS = {
    "Dow Jones": "^DJI",
    "S&P 500": "^GSPC",
    "NASDAQ 100": "^NDX",
    "VIX": "^VIX",
    "US 10-Year Yield": "^TNX",
    "Nikkei 225": "^N225",
    "Euro Stoxx 50": "^STOXX50E",
    "FTSE 100": "^FTSE",
    "Gold ETF": "GLD",
    "Silver ETF": "SLV",
}

def fetch_global_data():
    print("Fetching global data...\n")

    records = []

    for name, ticker in TICKERS.items():
        print(f"Fetching {name} ({ticker})...")

        # MOST STABLE METHOD FOR GITHUB ACTIONS
        df = yf.download(ticker, period="5d", interval="1d", progress=False)

        if df.empty or len(df) < 2:
            print(f"  ⚠️ No data for {name}")
            continue

        # Get last & previous close as FLOATS
        last = float(df["Close"].iloc[-1])
        prev = float(df["Close"].iloc[-2])

        change = last - prev
        percent = (change / prev * 100) if prev != 0 else 0

        # 1-year high/low
        yearly = yf.download(ticker, period="1y", interval="1d", progress=False)
        high = float(yearly["High"].max()) if not yearly.empty else last
        low = float(yearly["Low"].min()) if not yearly.empty else last

        records.append({
            "Index Name": name,
            "Last": round(last, 2),
            "Previous Close": round(prev, 2),
            "Change": round(change, 2),
            "% Change": f"{percent:+.2f}%",
            "Year High": round(high, 2),
            "Year Low": round(low, 2),
        })

        print(f"  ✓ {last:.2f} ({percent:+.2f}%)")

    if not records:
        print("\n‼️ ERROR: No data fetched!")
        return

    df_out = pd.DataFrame(records)
    filename = "GLOBAL_DATA.csv"
    df_out.to_csv(filename, index=False)

    timestamp = datetime.now().strftime("%d-%b-%Y %H:%M")
    print(f"\nSaved to {filename} at {timestamp}")

if __name__ == "__main__":
    fetch_global_data()
