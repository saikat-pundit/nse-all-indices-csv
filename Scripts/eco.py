import requests, pandas as pd, os, pytz
from datetime import datetime, timedelta

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0',
    'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0, no-transform'
}

today = datetime.now()
payload = {
    "from_date": (today - timedelta(days=15)).strftime("%Y-%m-%d"),
    "to_date": (today + timedelta(days=15)).strftime("%Y-%m-%d"),
    "countries": ["India", "China", "Japan", "Euro Area", "USA"],
    "impacts": []
}

try:
    data = requests.post("https://oxide.sensibull.com/v1/compute/market_global_events", headers=headers, json=payload, timeout=10).json()
    raw_data = data.get('payload', {}).get('data', []) if data.get('success') else []
except:
    raw_data = []

def impact_to_stars(impact):
    if "high" in impact.lower(): return "★★★"
    if "medium" in impact.lower(): return "★★"
    if "low" in impact.lower(): return "★"
    return impact.capitalize()

records = []
for item in raw_data:
    date_str = item.get('date', '')
    try:
        formatted_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %y")
    except:
        formatted_date = date_str
    
    records.append({
        'Date': formatted_date,
        'Time': item.get('time', '')[:5] if item.get('time') else '',
        'Country': item.get('country', ''),
        'Title': item.get('title', ''),
        'Impact': impact_to_stars(item.get('impact', '')),
        'Actual': item.get('actual', ''),
        'Expected': item.get('expected', ''),
        'Previous': item.get('previous', '')
    })

records.append({
    'Date': '', 'Time': '', 'Country': '', 'Title': '', 'Impact': '', 'Actual': '',
    'Expected': 'Update Time:', 'Previous': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%b %H:%M')
})

os.makedirs('Data', exist_ok=True)
pd.DataFrame(records).to_csv('Data/Economic.csv', index=False)
