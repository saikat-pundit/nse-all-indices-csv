import requests
import pandas as pd
import json

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
}

url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"

response = requests.get(url, headers=headers)
data = response.json()

records = []
for item in data['data']:
    symbol = item.get('symbol')
    
    if symbol != "NIFTY 50":
        pchange = item.get('pChange')
        if pchange is not None:
            percent_change_str = f"{pchange}%"
        else:
            percent_change_str = ""
        
        records.append({
            'Index Name': symbol,
            'Last': item.get('lastPrice'),
            'Change': item.get('change'),
            '% Change': percent_change_str,
            'Previous Close': item.get('previousClose'),
            'PE': "",  
            'Year High': item.get('yearHigh'),
            'Year Low': item.get('yearLow')
        })

df = pd.DataFrame(records)
df.to_csv('nifty50stock_top10.csv', index=False)
print("CSV created successfully!")
