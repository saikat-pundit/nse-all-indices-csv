import requests
import pandas as pd
import json

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
}

url = "https://www.nseindia.com/api/allIndices"

response = requests.get(url, headers=headers)
data = response.json()

target_indices = [
    "NIFTY 50",
    "INDIA VIX",
    "NIFTY NEXT 50",
    "NIFTY MIDCAP SELECT",
    "NIFTY MIDCAP 50",
    "NIFTY SMALLCAP 50",
    "NIFTY 500",
    "NIFTY ALPHA 50",
    "NIFTY IT",
    "NIFTY BANK",
    "NIFTY FINANCIAL SERVICES",
    "NIFTY PSU BANK",
    "NIFTY PRIVATE BANK",
    "NIFTY FMCG",
    "NIFTY CONSUMER DURABLES",
    "NIFTY PHARMA",
    "NIFTY HEALTHCARE INDEX",
    "NIFTY METAL",
    "NIFTY AUTO",
    "NIFTY SERVICES SECTOR",
    "NIFTY OIL & GAS",
    "NIFTY CHEMICALS",
    "NIFTY COMMODITIES",
    "NIFTY INDIA CONSUMPTION",
    "NIFTY PSE"
]

records = []
for item in data['data']:
    index_name = item.get('index')
    
    if index_name in target_indices:
        try:
            advances = int(item.get('advances', 0))
        except (ValueError, TypeError):
            advances = 0
        
        try:
            declines = int(item.get('declines', 0))
        except (ValueError, TypeError):
            declines = 0
        
        try:
            unchanged = int(item.get('unchanged', 0))
        except (ValueError, TypeError):
            unchanged = 0
        
        if declines != 0:
            adv_dec_ratio = advances / declines
            adv_dec_ratio_str = f"{adv_dec_ratio:.2f}"
        else:
            if advances > 0:
                adv_dec_ratio_str = "Max"
            else:
                adv_dec_ratio_str = "-"
        
        records.append({
            'Index Name': index_name,
            'Last': item.get('last'),
            'Change': item.get('variation'),
            '% Change': item.get('percentChange'),
            'Previous Close': item.get('previousClose'),
            'Adv/Dec Ratio': adv_dec_ratio_str,
            'Year High': item.get('yearHigh'),
            'Year Low': item.get('yearLow')
        })

df = pd.DataFrame(records)
df.to_csv('nse_all_indices.csv', index=False)
print("CSV created successfully!")
