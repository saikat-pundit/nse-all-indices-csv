import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
import os
from pathlib import Path

HOLIDAYS = [
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-12-26", "2026-01-27", "2026-03-04", 
    "2026-03-27", "2026-04-01", "2026-04-04", "2026-04-15", 
    "2026-05-02", "2026-05-29", "2026-06-27", "2026-09-15", 
    "2026-10-03", "2026-10-21", "2026-11-11", "2026-11-25", 
    "2026-12-26", "2026-01-16"
]

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
    "SBI GILT FUND - DIRECT PLAN - GROWTH"
]

def extract_name(full):
    """Extract display name from full fund name"""
    if "FoF" in full:
        parts = full.split("FoF")
        result = parts[0] + "FoF"
    elif "Fund" in full:
        parts = full.split("Fund")
        result = parts[0] + "Fund"
    else:
        result = full.split('-')[0]
    return ' '.join(result.split()).upper()

def load_old_data():
    """Load existing NAV data from CSV if exists"""
    csv_path = Path('Data/Daily_NAV.csv')
    old_data = {}
    
    if csv_path.exists():
        try:
            df_old = pd.read_csv(csv_path)
            # Exclude the timestamp row
            df_funds = df_old[~df_old['Fund Name'].str.contains('LAST UPDATED', na=False)]
            
            for _, row in df_funds.iterrows():
                fund_name = row['Fund Name']
                # Only store if we have valid NAV data (not '-')
                if str(row['NAV']).strip() not in ['-', 'nan', '']:
                    old_data[fund_name] = {
                        'NAV': str(row['NAV']).strip(),
                        'Update Time': str(row['Update Time']).strip()
                    }
            print(f"Loaded old data for {len(old_data)} funds")
        except Exception as e:
            print(f"Error loading old data: {e}")
    
    return old_data

def main():
    ist = pytz.timezone('Asia/Kolkata')
    today = datetime.now(ist)
    
    # Check if today is holiday or weekend
    if today.weekday() in [0, 6] or today.strftime('%Y-%m-%d') in HOLIDAYS:
        print(f"{today.strftime('%Y-%m-%d')} is holiday/weekend. Exiting.")
        exit()
    
    # Load old data before fetching new data
    old_data = load_old_data()
    
    # Calculate target date for NAV fetch
    if today.weekday() == 0:  # Monday
        target_date = today - timedelta(days=3)
    elif today.weekday() == 6:  # Sunday (shouldn't happen due to check above)
        target_date = today - timedelta(days=2)
    else:  # Tuesday to Friday
        target_date = today - timedelta(days=1)
    
    target_date_str = target_date.strftime('%Y-%m-%d')
    print(f"Fetching NAV data for date: {target_date_str}")
    
    # Fetch new NAV data from API
    url = f"https://www.amfiindia.com/api/nav-history?query_type=all_for_date&from_date={target_date_str}"
    
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching API data: {e}")
        exit()
    
    # Process new NAV data
    display_names = [extract_name(fund) for fund in target_funds]
    fund_mapping = dict(zip(display_names, target_funds))
    
    new_nav_data = {}
    if 'data' in data:
        for fund in data['data']:
            if 'schemes' in fund:
                for scheme in fund['schemes']:
                    if 'navs' in scheme:
                        for nav in scheme['navs']:
                            if nav['NAV_Name'] in target_funds:
                                name = extract_name(nav['NAV_Name'])
                                time_str = nav.get('hNAV_Upload_display', '')
                                date_only = ' '.join(time_str.split()[:2]) if time_str else '-'
                                new_nav_data[name] = {
                                    'NAV': str(nav.get('hNAV_Amt', '-')).strip(),
                                    'Update Time': date_only
                                }
    
    print(f"New data fetched for {len(new_nav_data)} funds")
    
    # Prepare records - use new data if available, else retain old data
    records = []
    funds_with_new_data = 0
    funds_with_old_data = 0
    funds_missing = 0
    
    for name in display_names:
        if name in new_nav_data:
            # Use new data
            records.append({
                'Fund Name': name,
                'NAV': new_nav_data[name]['NAV'],
                'Update Time': new_nav_data[name]['Update Time']
            })
            funds_with_new_data += 1
        elif name in old_data:
            # Retain old data
            records.append({
                'Fund Name': name,
                'NAV': old_data[name]['NAV'],
                'Update Time': old_data[name]['Update Time']
            })
            funds_with_old_data += 1
        else:
            # No data available at all
            records.append({
                'Fund Name': name,
                'NAV': '-',
                'Update Time': '-'
            })
            funds_missing += 1
    
    # Add timestamp row
    timestamp = today.strftime('%d-%b %H:%M')
    records.append({
        'Fund Name': '',
        'NAV': 'LAST UPDATED:',
        'Update Time': timestamp
    })
    
    # Ensure Data directory exists
    os.makedirs('Data', exist_ok=True)
    
    # Save to CSV
    df = pd.DataFrame(records)
    df.to_csv('Data/Daily_NAV.csv', index=False)
    
    print(f"File saved: Data/Daily_NAV.csv")

if __name__ == "__main__":
    main()
