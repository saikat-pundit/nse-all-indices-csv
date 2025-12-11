import requests
import pandas as pd
from datetime import datetime
import pytz
import os
# Headers to mimic a browser request
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
}

# URL for ETF data
url = "https://www.nseindia.com/api/etf"

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an error for bad status codes
    data = response.json()
except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
    exit(1)

# List of target ETF symbols to fetch
target_symbols = [
    "NIFTYBEES",
    "METALIETF",
    "PVTBANIETF",
    "ALPHA",
    "GOLDBEES",
    "SILVERBEES",
    "PHARMABEES",
    "ITBEES",
    "BANKBEES"
]

# Dictionary to store extracted records
symbol_dict = {}

# Process each ETF in the data
for item in data.get('data', []):
    symbol = item.get('symbol', '-')
    
    # Only process if symbol is in our target list
    if symbol in target_symbols:
        # Extract required fields with fallback values
        assets = item.get('assets', '-')
        
        # Get the last traded price
        ltP = item.get('ltP', '-')
        
        # Get the change amount and percentage
        chn = item.get('chn', '-')
        per = item.get('per', '-')
        
        # Format percentage change with % sign if it's a valid number
        if per != '-' and per is not None:
            try:
                percent_change_str = f"{per}%"
            except (ValueError, TypeError):
                percent_change_str = "-"
        else:
            percent_change_str = "-"
        
        # Get previous close and 52-week high/low
        prev_close = item.get('prevClose', '-')
        wkhi = item.get('wkhi', '-')
        wklo = item.get('wklo', '-')
        
        # Store in dictionary with symbol as key
        symbol_dict[symbol] = {
            'SYMBOL': symbol,
            'ASSETS': assets,
            'LAST': ltP,
            'CHANGE': chn,
            '%CHANGE': percent_change_str,
            'PREVIOUS CLOSE': prev_close,
            '52w High': wkhi,
            '52w Low': wklo
        }

# Create records in the specified order
records = []
for symbol in target_symbols:
    if symbol in symbol_dict:
        records.append(symbol_dict[symbol])
    else:
        # Create empty entry for missing symbols
        records.append({
            'SYMBOL': symbol,
            'ASSETS': '-',
            'LAST': '-',
            'CHANGE': '-',
            '%CHANGE': '-',
            'PREVIOUS CLOSE': '-',
            '52w High': '-',
            '52w Low': '-'
        })

# Add timestamp row at the end
ist = pytz.timezone('Asia/Kolkata')
current_time = datetime.now(ist).strftime('%d-%b %H:%M')

# Add Update Time row with time in the last column
records.append({
    'SYMBOL': '',
    'ASSETS': '',
    'LAST': '',
    'CHANGE': '',
    '%CHANGE': '',
    'PREVIOUS CLOSE': '',
    '52w High': 'Update Time',
    '52w Low': current_time
})
os.makedirs('../Data', exist_ok=True)
# Create DataFrame and save to CSV
df = pd.DataFrame(records)
df.to_csv('Data/etf.csv', index=False)

# Print success message
print(f"ETF data saved to etf.csv successfully!")
print(f"Total records: {len(target_symbols)} ETFs + 1 timestamp row")
print(f"Found {len(symbol_dict)} out of {len(target_symbols)} target symbols")
print(f"Last update: {current_time} IST")
