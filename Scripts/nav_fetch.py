import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
import os

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
}

# Specific funds to fetch
target_funds = [
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
    "SBI MAGNUM GILT FUND - DIRECT PLAN - GROWTH"
]

# Set IST timezone
ist = pytz.timezone('Asia/Kolkata')

# Get the correct date in IST
today = datetime.now(ist)  # Use IST timezone
# If today is Monday, go back to Friday (3 days)
if today.weekday() == 0:  # Monday = 0
    target_date = today - timedelta(days=3)
# If today is Sunday, go back to Friday (2 days)
elif today.weekday() == 6:  # Sunday = 6
    target_date = today - timedelta(days=2)
# For other days, use previous day
else:
    target_date = today - timedelta(days=1)

target_date_str = target_date.strftime('%Y-%m-%d')
url = f"https://www.amfiindia.com/api/nav-history?query_type=all_for_date&from_date={target_date_str}"

response = requests.get(url, headers=headers)
data = response.json()

records = []
funds_found = 0

for fund in data['data']:
    for scheme in fund['schemes']:
        for nav in scheme['navs']:
            nav_name = nav['NAV_Name']
            
            # Check if this NAV is in our target list
            if nav_name in target_funds:
                records.append({
                    'Fund Name': nav_name,
                    'Fund NAV': nav['hNAV_Amt'],
                    'Update Time': nav['hNAV_Upload_display']
                })
                funds_found += 1

# Sort records in the same order as target_funds list
sorted_records = []
for fund_name in target_funds:
    # Find the record for this fund
    for record in records:
        if record['Fund Name'] == fund_name:
            sorted_records.append(record)
            break
    else:
        # If fund not found, add placeholder
        sorted_records.append({
            'Fund Name': fund_name,
            'Fund NAV': 'Not Available',
            'Update Time': 'Not Available'
        })

# Add timestamp row with IST
timestamp = datetime.now(ist).strftime('%d-%b-%Y %H:%M')
sorted_records.append({
    'Fund Name': '',
    'Fund NAV': 'Last Updated:',
    'Update Time': f'{timestamp} IST'
})

# Save to CSV
os.makedirs('Data', exist_ok=True)
df = pd.DataFrame(sorted_records)
df.to_csv('Data/Daily_NAV.csv', index=False)

print(f"NAV data saved successfully for date: {target_date_str}!")
print(f"Funds found: {funds_found} out of {len(target_funds)}")
print(f"Timestamp: {timestamp} IST")
print(f"File saved to: Data/Daily_NAV.csv")
