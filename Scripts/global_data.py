import requests
import pandas as pd
from datetime import datetime
import pytz
import os

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

commodity_symbols = [
    {"name": "Dow Jones", "symbol": "CBOT_MINI:YM1!"},
    {"name": "S&P 500", "symbol": "CME_MINI:ES1!"},
    {"name": "NASDAQ 100", "symbol": "CME_MINI:NQ1!"},
    {"name": "VIX", "symbol": "CBOE:VX1!"},
    {"name": "Dollar Index", "symbol": "TVC:DXY"},
    {"name": "US10Y", "symbol": "TVC:US10Y"},
    {"name": "Nikkei 225", "symbol": "CME:NKD1!"},
    {"name": "Euro Stoxx 50", "symbol": "TVC:SX5E"},
    {"name": "DAX", "symbol": "EUREX:FDAX1!"},
    {"name": "FTSE 100", "symbol": "TVC:UKX"},
    {"name": "Bitcoin", "symbol": "CRYPTO:BTCUSD"},
    {"name": "USD/INR", "symbol": "FX_IDC:USDINR"},
    {"name": "USD/JPY", "symbol": "OANDA:USDJPY"}
]

BASE_API_URL = "https://scanner.tradingview.com/symbol?symbol={symbol}&fields=close[1],change_abs,price_52_week_high,price_52_week_low,close,change&no_404=true&label-product=symbols-performance"
commodity_data = []

def format_value(value, default="0.00", is_percent=False):
    try:
        if value is None:
            return default
        if is_percent:
            return f"{float(value):.2f}%"
        return f"{float(value):.2f}"
    except:
        return default

for commodity in commodity_symbols:
    url = BASE_API_URL.format(symbol=commodity["symbol"])
    
    try:
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            data = response.json()
            commodity_data.append({
                'Index': commodity["name"],
                'LTP': format_value(data.get('close')),
                'Chng': format_value(data.get('change_abs')),
                '% Chng': format_value(data.get('change'), is_percent=True),
                'Previous': format_value(data.get('close[1]')),
                'Yr Hi': format_value(data.get('price_52_week_high')),
                'Yr Lo': format_value(data.get('price_52_week_low'))
            })
        else:
            commodity_data.append({
                'Index': commodity["name"],
                'LTP': "0.00",
                'Chng': "0.00",
                '% Chng': "0.00%",
                'Previous': "0.00",
                'Yr Hi': "0.00",
                'Yr Lo': "0.00"
            })
    except:
        commodity_data.append({
            'Index': commodity["name"],
            'LTP': "0.00",
            'Chng': "0.00",
            '% Chng': "0.00%",
            'Previous': "0.00",
            'Yr Hi': "0.00",
            'Yr Lo': "0.00"
        })

# Add timestamp
ist = pytz.timezone('Asia/Kolkata')
current_time = datetime.now(ist).strftime('%d-%b %H:%M')
commodity_data.append({
    'Index': '',
    'LTP': '',
    'Chng': '',
    '% Chng': '',
    'Previous': '',
    'Yr Hi': 'Update Time',
    'Yr Lo': current_time
})

# Save to CSV
os.makedirs('Data', exist_ok=True)
pd.DataFrame(commodity_data).to_csv('Data/GLOBAL_DATA.csv', index=False)
