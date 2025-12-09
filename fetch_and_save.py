import requests
import pandas as pd
import json
from datetime import datetime

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
            adv_dec_ratio_str = "∞"  # Infinity symbol if advances > 0 and declines = 0
        else:
            adv_dec_ratio_str = "-"  # 0/0 case
    
    records.append({
        'Index Name': item.get('index'),
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
print("CSV created successfully!")
