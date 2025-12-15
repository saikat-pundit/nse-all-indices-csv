import requests
import pandas as pd
from datetime import datetime
import pytz
import os

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

urls = {
    "Today": "https://www.moneycontrol.com/economic-widget?duration=T&startDate=&endDate=&impact=&country=&deviceType=web&classic=true",
    "Tomorrow": "https://www.moneycontrol.com/economic-widget?duration=TO&startDate=&endDate=&impact=&country=&deviceType=web&classic=true"
}

def format_time(timestamp):
    try:
        utc_time = datetime.utcfromtimestamp(int(timestamp))
        ist = pytz.timezone('Asia/Kolkata')
        return pytz.utc.localize(utc_time).astimezone(ist).strftime('%d-%b %H:%M')
    except:
        return timestamp

def fetch_data():
    all_records = []
    
    for period, url in urls.items():
        try:
            response = requests.get(url, headers=headers, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data:
                    for item in data['data']:
                        all_records.append({
                            'Period': period,
                            'Country': item.get('country', '-'),
                            'Indicator': item.get('indicator', '-'),
                            'Actual': item.get('actual', '-'),
                            'Forecast': item.get('forecast', '-'),
                            'Previous': item.get('previous', '-'),
                            'Unit': item.get('unit', '-'),
                            'Trend': item.get('trend', '-'),
                            'Impact': item.get('impact', '-'),
                            'Time': format_time(item.get('datetime', '-'))
                        })
        except:
            continue
    
    if not all_records:
        current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%b %H:%M')
        all_records.append({
            'Period': 'No Data', 'Country': '-', 'Indicator': '-', 'Actual': '-',
            'Forecast': '-', 'Previous': '-', 'Unit': '-', 'Trend': '-',
            'Impact': '-', 'Time': f'Last checked: {current_time}'
        })
    
    # Add timestamp
    current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%b %H:%M')
    all_records.append({
        'Period': '', 'Country': '', 'Indicator': '', 'Actual': '', 'Forecast': '',
        'Previous': '', 'Unit': '', 'Trend': '', 'Impact': 'Updated:', 'Time': current_time
    })
    
    return pd.DataFrame(all_records)

# Main execution
os.makedirs('Data', exist_ok=True)
df = fetch_data()
df.to_csv('Data/Economic.csv', index=False)
