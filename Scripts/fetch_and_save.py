import requests
import pandas as pd
from datetime import datetime
import pytz
import os

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}

TV_SYMBOLS = {
    "USD/INR": "FX_IDC:USDINR",
    "GIFT-NIFTY": "NSEIX:NIFTY1!",
    "GOLD": "MCX:GOLD1!",
    "SILVER": "MCX:SILVER1!",
    "IND 5Y": "TVC:IN05Y",
    "IND 10Y": "TVC:IN10Y",
    "IND 30Y": "TVC:IN30Y"
}

target_indices = [
    "NIFTY 50", "INDIA VIX", "GIFT-NIFTY", "USD/INR", "GOLD", "SILVER", 
    "IND 5Y", "IND 10Y", "IND 30Y", "NIFTY NEXT 50", "NIFTY MIDCAP SELECT",
    "NIFTY MIDCAP 50", "NIFTY SMALLCAP 50", "NIFTY 500", "NIFTY ALPHA 50",
    "NIFTY IT", "NIFTY BANK", "NIFTY FINANCIAL SERVICES", "NIFTY PSU BANK",
    "NIFTY PRIVATE BANK", "NIFTY FMCG", "NIFTY CONSUMER DURABLES", "NIFTY PHARMA",
    "NIFTY HEALTHCARE INDEX", "NIFTY METAL", "NIFTY AUTO", "NIFTY SERVICES SECTOR",
    "NIFTY OIL & GAS", "NIFTY CHEMICALS", "NIFTY COMMODITIES", "NIFTY INDIA CONSUMPTION",
    "NIFTY PSE"
]

index_dict = {}

def format_tv_value(value, is_percent=False):
    if value == '-' or value is None:
        return '-'
    try:
        if is_percent:
            return f"{float(value):.2f}%"
        return f"{float(value):.2f}" if '.' in str(value) else str(value)
    except:
        return '-'

def fetch_tradingview_data(symbol_name, tv_symbol):
    url = f"https://scanner.tradingview.com/symbol?symbol={tv_symbol}&fields=close[1],change_abs,price_52_week_high,price_52_week_low,close,change&no_404=true"
    try:
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            data = response.json()
            return {
                'Index': symbol_name,
                'LTP': format_tv_value(data.get('close')),
                'Chng': format_tv_value(data.get('change_abs')),
                '% Chng': format_tv_value(data.get('change'), is_percent=True),
                'Previous': format_tv_value(data.get('close[1]')),
                'Adv:Dec': '-',
                'Yr Hi': format_tv_value(data.get('price_52_week_high')),
                'Yr Lo': format_tv_value(data.get('price_52_week_low'))
            }
    except:
        pass
    return None

# Fetch TradingView data
for index_name, tv_symbol in TV_SYMBOLS.items():
    tv_data = fetch_tradingview_data(index_name, tv_symbol)
    if tv_data:
        index_dict[index_name] = tv_data

# Fetch NSE data
try:
    data_indices = requests.get("https://www.nseindia.com/api/allIndices", headers=headers, timeout=5).json()
    for item in data_indices.get('data', []):
        index_name = item.get('index')
        if index_name in TV_SYMBOLS or index_name not in target_indices:
            continue
        
        advances = int(item.get('advances', 0))
        declines = int(item.get('declines', 0))
        adv_dec_ratio = f"{advances/declines:.2f}" if declines != 0 else "Max" if advances > 0 else "-"
        
        index_dict[index_name] = {
            'Index': index_name,
            'LTP': item.get('last', '-'),
            'Chng': item.get('variation', '-'),
            '% Chng': f"{item.get('percentChange', '-')}%" if item.get('percentChange') is not None else '-',
            'Previous': item.get('previousClose', '-'),
            'Adv:Dec': adv_dec_ratio,
            'Yr Hi': item.get('yearHigh', '-'),
            'Yr Lo': item.get('yearLow', '-')
        }
except:
    pass

# Create records
records = []
for index_name in target_indices:
    if index_name in index_dict:
        records.append(index_dict[index_name])
    else:
        records.append({
            'Index': index_name,
            'LTP': '-', 'Chng': '-', '% Chng': '-', 'Previous': '-',
            'Adv:Dec': '-', 'Yr Hi': '-', 'Yr Lo': '-'
        })

# Add timestamp
current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%b %H:%M')
records.append({
    'Index': '', 'LTP': '', 'Chng': '', '% Chng': '', 'Previous': '',
    'Adv:Dec': '', 'Yr Hi': 'Updated Time:', 'Yr Lo': current_time
})

# Save to CSV
os.makedirs('Data', exist_ok=True)
pd.DataFrame(records).to_csv('Data/nse_all_indices.csv', index=False)
