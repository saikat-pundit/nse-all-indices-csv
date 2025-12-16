import requests
import pandas as pd
from datetime import datetime
import pytz

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nseindia.com/option-chain'
}

def get_option_chain(symbol="NIFTY", expiry="23-Dec-2025"):
    url = f"https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol={symbol}&expiry={expiry}"
    
    session = requests.Session()
    session.headers.update(headers)
    session.get("https://www.nseindia.com")
    
    response = session.get(url)
    data = response.json()
    
    return data

def create_option_chain_dataframe(data):
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
            'OI': pe_data.get('openInterest', 0),
            'OI_CHANGE': pe_data.get('changeinOpenInterest', 0),
            'VOLUME': pe_data.get('totalTradedVolume', 0),
            'CHANGE': pe_data.get('change', 0),
            'LTP': pe_data.get('lastPrice', 0),
            'C_LTP': ce_data.get('lastPrice', 0),
            'C_CHANGE': ce_data.get('change', 0),
            'C_VOLUME': ce_data.get('totalTradedVolume', 0),
            'C_OI_CHANGE': ce_data.get('changeinOpenInterest', 0),
            'C_OI': ce_data.get('openInterest', 0)
        }
        option_data.append(option_row)
    
    df = pd.DataFrame(option_data)
    
    column_order = [
        'OI', 'OI_CHANGE', 'VOLUME', 'CHANGE', 'LTP',
        'STRIKE',
        'C_LTP', 'C_CHANGE', 'C_VOLUME', 'C_OI_CHANGE', 'C_OI'
    ]
    
    df = df[column_order]
    
    df = df.sort_values('STRIKE')
    
    timestamp_row = {
        'OI': f'TIMESTAMP: {timestamp}',
        'OI_CHANGE': f'UNDERLYING: {underlying_value}',
        'VOLUME': '',
        'CHANGE': '',
        'LTP': '',
        'STRIKE': '',
        'C_LTP': '',
        'C_CHANGE': '',
        'C_VOLUME': '',
        'C_OI_CHANGE': '',
        'C_OI': ''
    }
    
    df = pd.concat([pd.DataFrame([timestamp_row]), df], ignore_index=True)
    
    return df

def main():
    ist = pytz.timezone('Asia/Kolkata')
    
    data = get_option_chain()
    
    if data:
        df = create_option_chain_dataframe(data)
        
        import os
        os.makedirs('Data', exist_ok=True)
        
        df.to_csv('Data/Option.csv', index=False)
        
        timestamp = datetime.now(ist).strftime('%d-%b %H:%M')
        print(f"Option chain saved to Data/Option.csv")
        print(f"Timestamp: {timestamp} IST")
        print(f"Underlying Value: {data['records']['underlyingValue']}")
    else:
        print("Failed to fetch option chain data")

if __name__ == "__main__":
    main()
