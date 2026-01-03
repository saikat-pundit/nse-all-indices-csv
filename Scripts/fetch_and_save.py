import requests
import pandas as pd
import os
import pytz
from datetime import datetime

# TradingView symbols configuration
TV_SYMBOLS = {
    "USD/INR": "FX_IDC:USDINR", 
    "GIFT-NIFTY": "NSEIX:NIFTY1!", 
    "GOLD": "MCX:GOLD1!", 
    "SILVER": "MCX:SILVER1!", 
    "IND 5Y": "TVC:IN05Y", 
    "IND 10Y": "TVC:IN10Y", 
    "IND 30Y": "TVC:IN30Y"
}

# Target indices to include in the output
target_indices = [
    "NIFTY 50", "INDIA VIX", "GIFT-NIFTY", "USD/INR", "GOLD", "SILVER", 
    "IND 5Y", "IND 10Y", "IND 30Y", "NIFTY NEXT 50", "NIFTY MIDCAP SELECT", 
    "NIFTY MIDCAP 50", "NIFTY SMALLCAP 50", "NIFTY 500", "NIFTY ALPHA 50", 
    "NIFTY IT", "NIFTY BANK", "NIFTY FINANCIAL SERVICES", "NIFTY PSU BANK", 
    "NIFTY PRIVATE BANK", "NIFTY FMCG", "NIFTY CONSUMER DURABLES", 
    "NIFTY PHARMA", "NIFTY HEALTHCARE INDEX", "NIFTY METAL", "NIFTY AUTO", 
    "NIFTY SERVICES SECTOR", "NIFTY OIL & GAS", "NIFTY CHEMICALS", 
    "NIFTY COMMODITIES", "NIFTY INDIA CONSUMPTION", "NIFTY PSE", 
    "NIFTY REALTY", "NIFTY SERVICES SECTOR"
]

# Headers to mimic a browser request
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.nseindia.com/',
    'Origin': 'https://www.nseindia.com',
    'Connection': 'keep-alive'
}

def format_index_name(name):
    """Format index name for display"""
    if name == "NIFTY INDIA CONSUMPTION":
        return "CONSUMPTION"
    if name.startswith("NIFTY ") and name not in ["NIFTY 50", "NIFTY 500", "GIFT-NIFTY"]:
        return name.replace("NIFTY ", "")
    return name

def format_value(value, key, index_name):
    """Format values based on data type and index category"""
    if value == '-' or value is None or value == '':
        return '-'
    
    try:
        # Convert to float for numeric operations
        float_val = float(value)
        
        if key == '%':
            return f"{float_val:.2f}%"
        
        if key == 'Adv:Dec':
            return f"{float_val:.2f}"
        
        # Special handling for specific indices
        if index_name in ["INDIA VIX", "USD/INR", "IND 5Y", "IND 10Y", "IND 30Y"]:
            if key in ['LTP', 'Chng', 'Prev.', 'Yr Hi', 'Yr Lo']:
                return f"{float_val:.2f}"
            return str(float_val)
        
        # Integer formatting for large values
        if index_name in ["GIFT-NIFTY", "GOLD", "SILVER"] and key in ['LTP', 'Chng', 'Prev.', 'Yr Hi', 'Yr Lo']:
            return str(int(float_val)) if float_val.is_integer() else str(float_val)
        
        if key in ['LTP', 'Chng', 'Prev.', 'Yr Hi', 'Yr Lo']:
            return str(int(float_val)) if float_val.is_integer() else str(float_val)
        
        return str(float_val)
    
    except (ValueError, TypeError):
        return '-'

def fetch_tradingview_data():
    """Fetch data from TradingView API"""
    index_dict = {}
    
    for name, symbol in TV_SYMBOLS.items():
        url = f"https://scanner.tradingview.com/symbol?symbol={symbol}&fields=close[1],change_abs,price_52_week_high,price_52_week_low,close,change&no_404=true"
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                index_dict[name] = {
                    'Index': format_index_name(name),
                    'LTP': data.get('close'),
                    'Chng': data.get('change_abs'),
                    '%': data.get('change'),
                    'Prev.': data.get('close[1]'),
                    'Adv:Dec': '-',
                    'Yr Hi': data.get('price_52_week_high'),
                    'Yr Lo': data.get('price_52_week_low')
                }
                print(f"✓ Fetched TradingView data for: {name}")
            else:
                print(f"✗ TradingView API error for {name}: HTTP {response.status_code}")
                
        except Exception as e:
            print(f"✗ Error fetching TradingView data for {name}: {str(e)}")
    
    return index_dict

def fetch_nse_data():
    """Fetch data from NSE India API"""
    index_dict = {}
    
    # First, get a session cookie from NSE website (important for NSE API)
    session = requests.Session()
    
    try:
        # Get initial cookies from NSE homepage
        print("Getting NSE session cookies...")
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        
        # Now fetch the indices data
        print("Fetching NSE indices data...")
        response = session.get("https://www.nseindia.com/api/allIndices", headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'data' in data and isinstance(data['data'], list):
                print(f"✓ NSE API returned {len(data['data'])} indices")
                
                for item in data['data']:
                    name = item.get('index')
                    
                    # Skip if this index is from TradingView OR if it's not in our target list
                    if name in TV_SYMBOLS:
                        continue  # Skip TradingView indices
                    
                    if name not in target_indices:
                        continue  # Skip indices not in our target list
                    
                    # Calculate advances/declines ratio
                    adv = int(item.get('advances', 0))
                    dec = int(item.get('declines', 0))
                    
                    if dec > 0:
                        adv_dec = f"{adv/dec:.2f}"
                    elif adv > 0:
                        adv_dec = "Max"
                    else:
                        adv_dec = "-"
                    
                    # Store the data
                    index_dict[name] = {
                        'Index': format_index_name(name),
                        'LTP': item.get('last'),
                        'Chng': item.get('variation'),
                        '%': item.get('percentChange'),
                        'Prev.': item.get('previousClose'),
                        'Adv:Dec': adv_dec,
                        'Yr Hi': item.get('yearHigh'),
                        'Yr Lo': item.get('yearLow')
                    }
                    
                    print(f"  ✓ Processed NSE index: {name}")
            else:
                print(f"✗ Unexpected NSE API response structure: {data.keys()}")
        else:
            print(f"✗ NSE API error: HTTP {response.status_code}")
            print(f"  Response: {response.text[:200]}...")
            
    except Exception as e:
        print(f"✗ Error fetching NSE data: {str(e)}")
    
    return index_dict

def main():
    """Main function to fetch and process data"""
    print("Starting data fetch process...")
    print("-" * 50)
    
    # Fetch data from both sources
    tv_data = fetch_tradingview_data()
    nse_data = fetch_nse_data()
    
    # Merge the data dictionaries
    index_dict = {**tv_data, **nse_data}
    
    # Prepare records for CSV
    records = []
    
    for idx in target_indices:
        formatted_name = format_index_name(idx)
        
        if idx in index_dict:
            # Format all values
            rec = {}
            for k, v in index_dict[idx].items():
                if k == 'Index':
                    rec[k] = v
                else:
                    rec[k] = format_value(v, k, idx)
        else:
            # Create empty record for missing indices
            rec = {
                'Index': formatted_name,
                'LTP': '-',
                'Chng': '-',
                '%': '-',
                'Prev.': '-',
                'Adv:Dec': '-',
                'Yr Hi': '-',
                'Yr Lo': '-'
            }
        
        records.append(rec)
    
    # Add timestamp row
    ist_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%b %H:%M')
    records.append({
        'Index': '',
        'LTP': '',
        'Chng': '',
        '%': '',
        'Prev.': '',
        'Adv:Dec': '',
        'Yr Hi': 'Updated Time:',
        'Yr Lo': ist_time
    })
    
    # Create DataFrame and save to CSV
    df = pd.DataFrame(records)
    
    # Ensure Data directory exists
    os.makedirs('Data', exist_ok=True)
    
    # Save to CSV
    csv_path = 'Data/nse_all_indices.csv'
    df.to_csv(csv_path, index=False)
    
    print("-" * 50)
    print(f"✓ Data saved to: {csv_path}")
    print(f"✓ Total records processed: {len(records)-1} indices + 1 timestamp row")
    print(f"✓ Last update: {ist_time} IST")
    
    # Show sample of the data
    print("\nSample data (first 5 rows):")
    print(df.head().to_string(index=False))

if __name__ == "__main__":
    main()
