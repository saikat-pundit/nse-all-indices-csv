import yfinance as yf
import pandas as pd
from datetime import datetime
import concurrent.futures

# Tickers
TICKERS = {
    "Dow Jones": "^DJI",
    "S&P 500": "^GSPC",
    "NASDAQ 100": "^NDX",
    "VIX": "^VIX",
    "US 10-Year Yield": "^TNX",
    "Dollar Index": "DX-Y.NYB",
    "Nikkei 225": "^N225",
    "Euro Stoxx 50": "^STOXX50E",
    "FTSE 100": "^FTSE",
    "Gold": "GC=F",  # Changed from GC%3DF
    "Silver": "SI=F",  # Changed from SI%3DF
    "Bitcoin": "BTC-USD",
}

def fetch_single_ticker(name_ticker):
    """Fetch data for one ticker - optimized version"""
    name, ticker = name_ticker
    
    try:
        # ONE API CALL - get all data at once
        df = yf.download(ticker, period="1y", interval="1d", progress=False, timeout=10)
        
        if df.empty or len(df) < 5:  # Need at least 5 days for proper calculation
            return None
        
        # Get last & previous close from 1-year data
        last = float(df["Close"].iloc[-1])
        prev = float(df["Close"].iloc[-2])
        
        change = last - prev
        percent = (change / prev * 100) if prev != 0 else 0
        
        # Get year high/low from the SAME dataframe
        high = float(df["High"].max())
        low = float(df["Low"].min())
        
        return {
            "Index Name": name,
            "Last": round(last, 2),
            "Previous Close": round(prev, 2),
            "Change": round(change, 2),
            "% Change": f"{percent:+.2f}%",
            "Year High": round(high, 2),
            "Year Low": round(low, 2),
        }
        
    except Exception as e:
        print(f"  ⚠️ {name}: {str(e)[:50]}")
        return None

def fetch_global_data():
    print("Fetching global data (parallel)...")
    print("="*40)
    
    start_time = datetime.now()
    
    # PARALLEL FETCHING - all tickers at once
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        results = list(executor.map(fetch_single_ticker, TICKERS.items()))
    
    # Filter out None results
    records = [r for r in results if r is not None]
    
    if not records:
        print("\n‼️ ERROR: No data fetched!")
        return
    
    # Create and save DataFrame
    df_out = pd.DataFrame(records)
    filename = "GLOBAL_DATA.csv"
    df_out.to_csv(filename, index=False)
    
    # Add timestamp
    timestamp = datetime.now().strftime("%d-%b-%Y %H:%M")
    with open(filename, "a") as f:
        f.write(f,,,,"Update Time:,{timestamp}\n")
    
    # Print stats
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n✅ Saved {len(records)} records to {filename}")
    print(f"⏱️  Execution time: {elapsed:.1f} seconds")
    print(f"📅 Updated: {timestamp}")
    
    return df_out

if __name__ == "__main__":
    fetch_global_data()
