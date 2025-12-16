import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
import os

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nseindia.com/option-chain'
}

def get_next_tuesday():
    """Get the next Tuesday date in DD-MMM-YYYY format"""
    ist = pytz.timezone('Asia/Kolkata')
    today = datetime.now(ist).date()
    
    # Calculate days until next Tuesday
    # Monday is 0, Tuesday is 1, etc.
    days_ahead = 1 - today.weekday()  # 1 is Tuesday
    
    # If days_ahead is 0, it's Tuesday today
    if days_ahead < 0:  # If today is past Tuesday (Wed, Thu, Fri, Sat, Sun)
        days_ahead += 7  # Get next week's Tuesday
    # If days_ahead is 0 (Tuesday today), keep it as 0
    
    next_tuesday = today + timedelta(days=days_ahead)
    
    # Format date as required by NSE (e.g., "23-Dec-2025")
    formatted_date = next_tuesday.strftime('%d-%b-%Y').upper()
    return formatted_date

def get_option_chain(symbol="NIFTY", expiry=None):
    if expiry is None:
        expiry = get_next_tuesday()
    
    url = f"https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol={symbol}&expiry={expiry}"
    
    session = requests.Session()
    session.headers.update(headers)
    session.get("https://www.nseindia.com")
    
    response = session.get(url)
    data = response.json()
    
    return data, expiry

def create_option_chain_dataframe(data, expiry_date):
    records = data['records']
    timestamp = records['timestamp']
    underlying_value = records['underlyingValue']
    
    option_data = []
    
    for item in records['data']:
        strike_price = item['strikePrice']
        ce_data = item.get('CE', {})
        pe_data = item.get('PE', {})
        
        option_row = {
            'STRIKE': strike_price,
            'PUT_OI': pe_data.get('openInterest', 0),
            'PUT_CHNG_IN_OI': pe_data.get('changeinOpenInterest', 0),
            'PUT_VOLUME': pe_data.get('totalTradedVolume', 0),
            'PUT_IV': pe_data.get('impliedVolatility', 0),
            'PUT_LTP': pe_data.get('lastPrice', 0),
            'PUT_CHNG': pe_data.get('change', 0),
            'CALL_LTP': ce_data.get('lastPrice', 0),
            'CALL_CHNG': ce_data.get('change', 0),
            'CALL_IV': ce_data.get('impliedVolatility', 0),
            'CALL_VOLUME': ce_data.get('totalTradedVolume', 0),
            'CALL_CHNG_IN_OI': ce_data.get('changeinOpenInterest', 0),
            'CALL_OI': ce_data.get('openInterest', 0)
        }
        option_data.append(option_row)
    
    df = pd.DataFrame(option_data)
    
    # Updated column order without removed columns
    column_order = [
        'CALL_OI', 'CALL_CHNG_IN_OI', 'CALL_VOLUME', 'CALL_IV', 'CALL_CHNG', 'CALL_LTP',
        'STRIKE',
        'PUT_LTP', 'PUT_CHNG', 'PUT_IV', 'PUT_VOLUME', 'PUT_CHNG_IN_OI', 'PUT_OI'
    ]
    
    df = df[column_order]
    
    # Add underlying value row
    metadata = pd.DataFrame([{
        'CALL_OI': '',
        'CALL_CHNG_IN_OI': '',
        'CALL_VOLUME': '',
        'CALL_IV': '',        
        'CALL_CHNG': '',
        'CALL_LTP': '',
        'STRIKE': underlying_value,
        'PUT_LTP': 'Expiry: ' + expiry_date,
        'PUT_CHNG': '',
        'PUT_IV': '',
        'PUT_VOLUME': '',
        'PUT_CHNG_IN_OI': '',
        'PUT_OI': ''
    }])
    
    df = pd.concat([metadata, df], ignore_index=True)
    
    # Get current IST timestamp for footer
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist).strftime('%d-%b %H:%M')
    
    # Add timestamp as last row
    timestamp_row = pd.DataFrame([{
        'CALL_OI': '',
        'CALL_CHNG_IN_OI': '',
        'CALL_VOLUME': '',
        'CALL_IV': '',        
        'CALL_CHNG': '',
        'CALL_LTP': '',
        'STRIKE': '',
        'PUT_LTP': '',
        'PUT_CHNG': '',
        'PUT_IV': '',
        'PUT_VOLUME': '',
        'PUT_CHNG_IN_OI': 'Update Time',
        'PUT_OI': current_time
    }])
    
    df = pd.concat([df, timestamp_row], ignore_index=True)
    
    return df

def main():
    ist = pytz.timezone('Asia/Kolkata')
    
    # Get expiry date and data
    expiry_date = get_next_tuesday()
    print(f"Using expiry date: {expiry_date}")
    
    data, expiry = get_option_chain(expiry=expiry_date)
    
    if data:
        df = create_option_chain_dataframe(data, expiry)
        
        # Ensure directory exists
        os.makedirs('Data', exist_ok=True)
        
        # Save to CSV
        output_path = 'Data/Option.csv'
        df.to_csv(output_path, index=False)
        
        # Print summary
        current_time = datetime.now(ist).strftime('%d-%b %H:%M')
        print(f"Option chain saved to {output_path}")
        print(f"Timestamp: {current_time} IST")
        print(f"Underlying Value: {data['records']['underlyingValue']}")
        print(f"Expiry Date: {expiry}")
        print(f"Data saved with {len(df)-2} strike prices (excluding header and footer rows)")
    else:
        print("Failed to fetch option chain data")

if __name__ == "__main__":
    main()
