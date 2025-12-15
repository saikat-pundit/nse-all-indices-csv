import requests
import pandas as pd
from datetime import datetime
import pytz
import os

API_URL = "https://oxide.sensibull.com/v1/compute/market_global_events"

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0',
    'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0, no-transform'
}

payload = {
    "from_date": "2025-12-01",
    "to_date": "2025-12-31",
    "countries": ["India", "China", "Japan", "Euro Area", "USA"],
    "impacts": []
}

def format_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %y")
    except:
        return date_str

def format_time(time_str):
    try:
        return time_str[:5] if time_str and len(time_str) >= 5 else time_str
    except:
        return time_str

def process_and_save():
    try:
        response = requests.post(API_URL, headers=headers, json=payload, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('success') and 'payload' in data:
                raw_data = data['payload'].get('data', [])
            else:
                raw_data = []
        else:
            raw_data = []
    except:
        raw_data = []
    
    records = []
    
    for item in raw_data:
        records.append({
            'Date': format_date(item.get('date', '')),
            'Time': format_time(item.get('time', '')),
            'Country': item.get('country', ''),
            'Title': item.get('title', ''),
            'Impact': item.get('impact', '').capitalize(),
            'Actual': item.get('actual', ''),
            'Expected': item.get('expected', ''),
            'Previous': item.get('previous', '')
        })
    
    if not records:
        records.append({
            'Date': '', 'Time': '', 'Country': 'No Data',
            'Title': '', 'Impact': '', 'Actual': '', 'Expected': '', 'Previous': ''
        })
    
    # Add timestamp with formatted date and time
    current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    records.append({
        'Date': current_time.strftime('%d %b %y'),
        'Time': current_time.strftime('%H:%M'),
        'Country': '',
        'Title': '',
        'Impact': 'Updated',
        'Actual': '',
        'Expected': '',
        'Previous': ''
    })
    
    # Sort by date
    if records and len(records) > 1:
        df = pd.DataFrame(records)
        try:
            df['SortDate'] = pd.to_datetime(df['Date'], format='%d %b %y', errors='coerce')
            df = df.sort_values(['SortDate', 'Time'])
            df = df.drop('SortDate', axis=1)
            records = df.to_dict('records')
        except:
            pass
    
    # Save to CSV
    os.makedirs('Data', exist_ok=True)
    pd.DataFrame(records).to_csv('Data/Economic.csv', index=False)

# Execute
process_and_save()
