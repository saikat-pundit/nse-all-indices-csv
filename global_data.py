import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# Ticker mapping with descriptive names
TICKERS = {
    "US30 Futures": "YM=F",           # Dow Jones Futures
    "US500 Futures": "ES=F",          # S&P 500 Futures
    "NASDAQ100 Futures": "NQ=F",      # NASDAQ 100 Futures
    "USA VIX": "^VIX",                # Volatility Index
    "US10Y Govt Bond": "^TNX",        # 10-Year Treasury Yield
    "Nikkei Futures": "NKD=F",        # Nikkei 225 Futures
    "Euro Stoxx Futures": "ESTX50",   # Euro Stoxx 50
    "FTSE Futures": "Z=F",            # FTSE 100 Futures
    "Gold": "GC=F",                   # COMEX Gold Futures
    "Comex Gold": "GC=F",             # Same as Gold
    "Silver": "SI=F",                 # COMEX Silver Futures
    "Comex Silver Futures": "SI=F"    # Same as Silver
}

def fetch_global_data():
    """Fetch global market data and format it properly."""
    
    print("Fetching global market data...")
    print("="*60)
    
    # Get ticker symbols for yfinance
    ticker_symbols = list(TICKERS.values())
    
    try:
        # Download all data at once (more efficient)
        # Using '1mo' period to get enough data for calculations
        raw_data = yf.download(
            ticker_symbols,
            period="1mo",
            interval="1d",
            group_by="ticker",
            progress=False,
            auto_adjust=True
        )
        
        if raw_data.empty:
            print("Error: No data received!")
            return None
            
        records = []
        
        # Process each ticker
        for display_name, ticker in TICKERS.items():
            try:
                # Skip duplicate tickers (like Gold/Comex Gold)
                if display_name in ["Comex Gold", "Comex Silver Futures"]:
                    # Use the non-duplicate data with different display name
                    base_name = "Gold" if display_name == "Comex Gold" else "Silver"
                    continue
                
                # Get data for this specific ticker
                if ticker in raw_data:
                    ticker_data = raw_data[ticker]
                else:
                    # For single column data (like yields)
                    ticker_data = raw_data.xs(ticker, axis=1, level=0, drop_level=True)
                
                # Check if we have data
                if ticker_data.empty or len(ticker_data) < 2:
                    print(f"  ⚠️  {display_name}: Insufficient data")
                    continue
                
                # Get latest and previous day data
                latest = ticker_data.iloc[-1]
                previous = ticker_data.iloc[-2]
                
                # Calculate values
                last_price = latest.get("Close", latest.get("Adj Close", 0))
                prev_close = previous.get("Close", previous.get("Adj Close", 0))
                
                # Handle potential missing values
                if pd.isna(last_price) or pd.isna(prev_close):
                    print(f"  ⚠️  {display_name}: Missing price data")
                    continue
                
                change = last_price - prev_close
                percent_change = (change / prev_close * 100) if prev_close != 0 else 0
                
                # Get year high/low (from the last year of data)
                year_high = ticker_data["High"].max() if "High" in ticker_data.columns else last_price
                year_low = ticker_data["Low"].min() if "Low" in ticker_data.columns else last_price
                
                # Add to records
                records.append({
                    "Index Name": display_name,
                    "Last": round(last_price, 2),
                    "Change": round(change, 2),
                    "% Change": f"{percent_change:.2f}%",
                    "Previous Close": round(prev_close, 2),
                    "Year High": round(year_high, 2),
                    "Year Low": round(year_low, 2)
                })
                
                print(f"  ✓ {display_name}: ${last_price:.2f} ({percent_change:+.2f}%)")
                
            except Exception as e:
                print(f"  ✗ {display_name} error: {str(e)[:50]}")
                continue
        
        if not records:
            print("No valid records created!")
            return None
        
        # Create DataFrame
        df = pd.DataFrame(records)
        
        # Save to CSV
        filename = "GLOBAL_DATA.csv"
        df.to_csv(filename, index=False)
        
        # Add timestamp as last row (empty except for timestamp in last column)
        timestamp = datetime.now().strftime("%d-%b %H:%M")
        df_timestamp = pd.DataFrame([{
            "Index Name": "",
            "Last": "",
            "Change": "",
            "% Change": "",
            "Previous Close": "",
            "Year High": "Update Time:",
            "Year Low": timestamp
        }])
        
        # Append timestamp row
        df_timestamp.to_csv(filename, mode='a', header=False, index=False)
        
        print(f"\n" + "="*60)
        print(f"✅ Data saved to {filename}")
        print(f"📊 {len(records)} instruments processed")
        print(f"🕐 Last updated: {timestamp}")
        
        # Show preview
        print("\n📋 Data Preview:")
        print(df.to_string(index=False))
        
        return df
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        return None

if __name__ == "__main__":
    fetch_global_data()
