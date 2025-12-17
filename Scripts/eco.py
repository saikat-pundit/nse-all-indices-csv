import requests, csv, os, pandas as pd
from datetime import datetime

# First CSV
url1 = "https://oxide.sensibull.com/v1/compute/cache/fii_dii_daily"
data1 = requests.get(url1).json()
os.makedirs("Data", exist_ok=True)

with open("Data/Cash.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Date", "FII Net Buy/Sell", "DII Net Buy/Sell"])
    for date_str in sorted(data1["data"], reverse=True):
        day = data1["data"][date_str]
        if "cash" in day:
            formatted_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %y")
            fii_val = int(day["cash"]["fii"]["buy_sell_difference"])
            dii_val = int(day["cash"]["dii"]["buy_sell_difference"])
            writer.writerow([formatted_date, f"{fii_val} Cr.", f"{dii_val} Cr."])

# Second CSV
url2 = "https://oxide.sensibull.com/v1/compute/market_global_events"
payload = {"from_date": "2025-12-01", "to_date": "2025-12-31", "countries": ["India", "China", "Japan", "Euro Area", "USA"], "impacts": []}
headers = {'User-Agent': 'Mozilla/5.0', 'Content-Type': 'application/json'}
data2 = requests.post(url2, headers=headers, json=payload).json()

records = []
for item in data2.get('payload', {}).get('data', []):
    records.append({
        'Date': datetime.strptime(item.get('date', ''), "%Y-%m-%d").strftime("%d %b %y") if item.get('date') else '',
        'Time': item.get('time', '')[:5] if item.get('time') else '',
        'Country': item.get('country', ''),
        'Title': item.get('title', ''),
        'Impact': item.get('impact', '').capitalize(),
        'Actual': item.get('actual', ''),
        'Expected': item.get('expected', ''),
        'Previous': item.get('previous', '')
    })

records.append({
    'Date': '', 'Time': '', 'Country': '', 'Title': '', 'Impact': '', 'Actual': '',
    'Expected': 'Update Time:', 'Previous': datetime.now().strftime('%d-%b %H:%M')
})

pd.DataFrame(records).to_csv('Data/Economic.csv', index=False)
