import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
import os

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}

target_funds_api = [
    "Aditya Birla Sun Life PSU Equity Fund-Direct Plan-Growth",
    "Axis Focused Fund - Direct Plan - Growth Option",
    "Axis Large & Mid Cap Fund - Direct Plan - Growth",
    "Axis Large Cap Fund - Direct Plan - Growth",
    "Axis Small Cap Fund - Direct Plan - Growth",
    "ICICI Prudential Banking and PSU Debt Fund - Direct Plan -  Growth",
    "ICICI Prudential Corporate Bond Fund - Direct Plan - Growth",
    "ICICI Prudential Gilt Fund - Direct Plan - Growth",
    "ICICI Prudential Nifty 50 Index Fund - Direct Plan Cumulative Option",
    "ICICI PRUDENTIAL SILVER ETF FUND OF FUND - Direct Plan - Growth",
    "ICICI Prudential Technology Fund - Direct Plan -  Growth",
    "Mahindra Manulife Consumption Fund - Direct Plan -Growth",
    "Mirae Asset Arbitrage Fund Direct Growth",
    "Mirae Asset ELSS Tax Saver Fund - Direct Plan - Growth",
    "Mirae Asset Healthcare Fund Direct Growth",
    "Nippon India Gold Savings Fund - Direct Plan Growth Plan - Growth Option",
    "Nippon India Nifty Next 50 Junior BeES FoF - Direct Plan - Growth Plan - Growth Option",
    "Nippon India Nivesh Lakshya Long Duration Fund- Direct Plan- Growth Option",
    "quant ELSS Tax Saver Fund - Growth Option - Direct Plan",
    "SBI GILT FUND - DIRECT PLAN - GROWTH"
]

def extract_display_name(full_name):
    if "FoF" in full_name:
        parts = full_name.split("FoF")
        result = parts[0] + "FoF"
    elif "Fund" in full_name:
        parts = full_name.split("Fund")
        result = parts[0] + "Fund"
    else:
        result = full_name.split('-')[0]
    
    result = ' '.join(result.split()).upper()
    return result

display_names = [extract_display_name(fund) for fund in target_funds_api]
fund_name_mapping = dict(zip(display_names, target_funds_api))

ist = pytz.timezone('Asia/Kolkata')
today = datetime.now(ist)

if today.weekday() == 0:
    target_date = today - timedelta(days=3)
elif today.weekday() == 6:
    target_date = today - timedelta(days=2)
else:
    target_date = today - timedelta(days=1)

target_date_str = target_date.strftime('%Y-%m-%d')
url = f"https://www.amfiindia.com/api/nav-history?query_type=all_for_date&from_date={target_date_str}"

response = requests.get(url, headers=headers)
data = response.json()

nav_data = {}
for fund in data['data']:
    for scheme in fund['schemes']:
        for nav in scheme['navs']:
            nav_name = nav['NAV_Name']
            if nav_name in target_funds_api:
                display_name = extract_display_name(nav_name)
                upload_time = nav['hNAV_Upload_display']
                if upload_time:
                    date_only = ' '.join(upload_time.split()[:2])
                else:
                    date_only = '-'
                nav_data[display_name] = {
                    'NAV': nav['hNAV_Amt'],
                    'Update Time': date_only
                }

sorted_records = []
funds_found = 0

for display_name in display_names:
    if display_name in nav_data:
        sorted_records.append({
            'Fund Name': display_name,
            'NAV': nav_data[display_name]['NAV'],
            'Update Time': nav_data[display_name]['Update Time']
        })
        funds_found += 1
    else:
        sorted_records.append({
            'Fund Name': display_name,
            'NAV': '-',
            'Update Time': '-'
        })

timestamp = datetime.now(ist).strftime('%d-%b %H:%M')
sorted_records.append({
    'Fund Name': '',
    'NAV': 'LAST UPDATED:',
    'Update Time': f'{timestamp}'
})

os.makedirs('Data', exist_ok=True)
df = pd.DataFrame(sorted_records)
df.to_csv('Data/Daily_NAV.csv', index=False)

print(f"Funds found: {funds_found} out of {len(display_names)}")
print(f"Timestamp: {timestamp} IST")
