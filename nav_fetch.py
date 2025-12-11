import requests
import pandas as pd
from datetime import datetime, timedelta
import os
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
}

# Get the correct date
today = datetime.now()
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
for fund in data['data']:
    for scheme in fund['schemes']:
        for nav in scheme['navs']:
            records.append({
                'Fund Name': nav['NAV_Name'],
                'Fund NAV': nav['hNAV_Amt'],
                'Update Time': nav['hNAV_Upload_display']
            })
os.makedirs('../Data', exist_ok=True)
df = pd.DataFrame(records)
df.to_csv('Data/Daily_NAV.csv', index=False)
print(f"CSV created successfully for date: {target_date_str}!")
