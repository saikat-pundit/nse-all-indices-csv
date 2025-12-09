import requests
import pandas as pd
import json
from datetime import datetime

# List of indices to fetch
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

# NSE blocks bots, so we pretend to be a browser
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
}

url = "https://www.nseindia.com/api/allIndices"

response = requests.get(url, headers=headers)
data = response.json()

# Convert to simple list
records = []
for item in data['data']:
    index_name = item.get('index')
    
    # Only process if the index is in our target list
    if index_name in target_indices:
        # Convert to integers, default to 0 if conversion fails
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
        
        # Calculate Advances/Declines ratio
        # Avoid division by zero - if declines is 0, handle it appropriately
        if declines != 0:
            adv_dec_ratio = advances / declines
            adv_dec_ratio_str = f"{adv_dec_ratio:.2f}"  # Format to 2 decimal places
        else:
            if advances > 0:
                adv_dec_ratio_str = "Max"  # Infinity symbol if advances > 0 and declines = 0
            else:
                adv_dec_ratio_str = "-"  # 0/0 case
        
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

# Save as CSV
df = pd.DataFrame(records)
df.to_csv('nse_all_indices.csv', index=False)
print(f"CSV created successfully! Found {len(records)} out of {len(target_indices)} indices.")
