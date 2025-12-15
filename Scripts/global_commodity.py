import requests
import pandas as pd
from datetime import datetime
import pytz
import os

# Headers to mimic a browser request
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.tradingview.com/'
}

# Commodity definitions with TradingView symbols
commodity_symbols = [
    {"name": "GOLD", "symbol": "TVC:GOLD"},
    {"name": "GOLD!", "symbol": "COMEX:GC1!"},
    {"name": "SILVER", "symbol": "TVC:SILVER"},
    {"name": "SILVER!", "symbol": "COMEX:SI1!"},
    {"name": "GOLD:SILVER", "symbol": "TVC:GOLDSILVER"},
    {"name": "DXY", "symbol": "TVC:DXY"},
    {"name": "US10Y", "symbol": "TVC:US10Y"},
    {"name": "BRENT", "symbol": "FX:UKOIL"},
    {"name": "GOLDINR", "symbol": "MCX:GOLD1!"},
    {"name": "SILVERINR", "symbol": "MCX:SILVER1!"},
    {"name": "GOLD ETF", "symbol": "NSE:GOLDBEES"},
    {"name": "SILVER ETF", "symbol": "NSE:SILVERBEES"}
]

# Base API URL with placeholders
BASE_API_URL = "https://scanner.tradingview.com/symbol?symbol={symbol}&fields=close[1],change_abs,price_52_week_high,price_52_week_low,close,change&no_404=true&label-product=symbols-performance"

# Dictionary to store extracted records
commodity_data = []

print("Fetching global commodity data...")

# Process each symbol
for commodity in commodity_symbols:
    symbol_name = commodity["name"]
    tv_symbol = commodity["symbol"]
    
    # Build the API URL
    url = BASE_API_URL.format(symbol=tv_symbol)
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an error for bad status codes
        
        if response.status_code == 200:
            data = response.json()
            
            # Extract data fields
            close = data.get('close', 0)
            change = data.get('change', 0)
            change_abs = data.get('change_abs', 0)
            previous_close = data.get('close[1]', 0)
            yr_hi = data.get('price_52_week_high', 0)
            yr_lo = data.get('price_52_week_low', 0)
            
            # Format percentage change
            if change is not None:
                try:
                    percent_change = f"{change:.2f}%"
                except (ValueError, TypeError):
                    percent_change = "0.00%"
            else:
                percent_change = "0.00%"
            
            # Format absolute change
            if change_abs is not None:
                try:
                    abs_change = f"{change_abs:.2f}"
                except (ValueError, TypeError):
                    abs_change = "0.00"
            else:
                abs_change = "0.00"
            
            # Format other numeric values
            close_formatted = f"{close:.2f}" if close is not None else "0.00"
            previous_formatted = f"{previous_close:.2f}" if previous_close is not None else "0.00"
            yr_hi_formatted = f"{yr_hi:.2f}" if yr_hi is not None else "0.00"
            yr_lo_formatted = f"{yr_lo:.2f}" if yr_lo is not None else "0.00"
            
            # Add to records
            commodity_data.append({
                'Index': symbol_name,
                'LTP': close_formatted,
                'Chng': abs_change,
                '% Chng': percent_change,
                'Previous': previous_formatted,
                'Yr Hi': yr_hi_formatted,
                'Yr Lo': yr_lo_formatted
            })
            
            print(f"✓ Fetched data for {symbol_name}")
            
        else:
            print(f"✗ Failed to fetch {symbol_name}: HTTP {response.status_code}")
            
            # Add placeholder data for failed fetch
            commodity_data.append({
                'Index': symbol_name,
                'LTP': "0.00",
                'Chng': "0.00",
                '% Chng': "0.00%",
                'Previous': "0.00",
                'Yr Hi': "0.00",
                'Yr Lo': "0.00"
            })
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching {symbol_name}: {e}")
        
        # Add placeholder data for error
        commodity_data.append({
            'Index': symbol_name,
            'LTP': "0.00",
            'Chng': "0.00",
            '% Chng': "0.00%",
            'Previous': "0.00",
            'Yr Hi': "0.00",
            'Yr Lo': "0.00"
        })
    except Exception as e:
        print(f"✗ Unexpected error for {symbol_name}: {e}")
        
        # Add placeholder data for unexpected error
        commodity_data.append({
            'Index': symbol_name,
            'LTP': "0.00",
            'Chng': "0.00",
            '% Chng': "0.00%",
            'Previous': "0.00",
            'Yr Hi': "0.00",
            'Yr Lo': "0.00"
        })

# Add timestamp row at the end
ist = pytz.timezone('Asia/Kolkata')
current_time = datetime.now(ist).strftime('%d-%b %H:%M')

# Add Update Time row with time in the last column
commodity_data.append({
    'Index': '',
    'LTP': '',
    'Chng': '',
    '% Chng': '',
    'Previous': '',
    'Yr Hi': 'Update Time',
    'Yr Lo': current_time
})

df = pd.DataFrame(commodity_data)
# Ensure Data directory exists
os.makedirs('Data', exist_ok=True)

# DELETE THE OLD CSV FILE IF IT EXISTS
csv_path = 'Data/GLOBAL_COMMODITIES.csv'
if os.path.exists(csv_path):
    os.remove(csv_path)
    print(f"✓ Removed old file: {csv_path}")

# Save to CSV
df.to_csv(csv_path, index=False)
