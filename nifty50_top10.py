import requests
import pandas as pd

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
}

url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"

response = requests.get(url, headers=headers)
data = response.json()

target_symbols = [
    "RELIANCE",
    "HDFCBANK", 
    "BHARTIARTL",
    "TCS",
    "ICICIBANK",
    "SBIN",
    "INFY",
    "BAJFINANCE",
    "LT",
    "HINDUNILVR"
]

symbol_dict = {}
for item in data['data']:
    symbol = item.get('symbol')
    
    if symbol in target_symbols:
        pchange = item.get('pChange')
        if pchange is not None:
            percent_change_str = f"{pchange}%"
        else:
            percent_change_str = ""
        
        symbol_dict[symbol] = {
            'Symbol': symbol,
            'Last': item.get('lastPrice'),
            'Change': item.get('change'),
            '% Change': percent_change_str,
            'Previous Close': item.get('previousClose'),
            'Year High': item.get('yearHigh'),
            'Year Low': item.get('yearLow')
        }

records = []
for symbol in target_symbols:
    if symbol in symbol_dict:
        records.append(symbol_dict[symbol])

df = pd.DataFrame(records)
df.to_csv('nifty50_stocks_top10.csv', index=False)
print("CSV created successfully!")
