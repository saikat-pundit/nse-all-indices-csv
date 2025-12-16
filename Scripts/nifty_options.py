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
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    today = now.date()
    days_ahead = 1 - today.weekday()
    
    if days_ahead < 0 or (days_ahead == 0 and now.hour >= 15):
        days_ahead += 7
    
    next_tuesday = today + timedelta(days=days_ahead)
    return next_tuesday.strftime('%d-%b-%Y').upper()

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
    underlying_value = records['underlyingValue']
    
    option_data = []
    for item in records['data']:
        strike_price = item['strikePrice']
        ce_data = item.get('CE', {})
        pe_data = item.get('PE', {})
        
        option_data.append({
            'CALL OI': ce_data.get('openInterest', 0),
            'CALL CHNG IN OI': ce_data.get('changeinOpenInterest', 0),
            'CALL VOLUME': ce_data.get('totalTradedVolume', 0),
            'CALL IV': ce_data.get('impliedVolatility', 0),
            'CALL CHNG': ce_data.get('change', 0),
            'CALL LTP': ce_data.get('lastPrice', 0),
            'STRIKE': strike_price,
            'PUT LTP': pe_data.get('lastPrice', 0),
            'PUT CHNG': pe_data.get('change', 0),
            'PUT IV': pe_data.get('impliedVolatility', 0),
            'PUT VOLUME': pe_data.get('totalTradedVolume', 0),
            'PUT CHNG IN OI': pe_data.get('changeinOpenInterest', 0),
            'PUT OI': pe_data.get('openInterest', 0)
        })
    
    df = pd.DataFrame(option_data)
    
    metadata = pd.DataFrame([{
        'CALL OI': '', 'CALL CHNG IN OI': '', 'CALL VOLUME': '', 'CALL IV': '',
        'CALL CHNG': '', 'CALL LTP': '', 'STRIKE': underlying_value,
        'PUT LTP': 'Expiry: ' + expiry_date, 'PUT CHNG': '', 'PUT IV': '',
        'PUT VOLUME': '', 'PUT CHNG IN OI': '', 'PUT OI': ''
    }])
    
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist).strftime('%d-%b %H:%M')
    
    timestamp_row = pd.DataFrame([{
        'CALL OI': '', 'CALL CHNG IN OI': '', 'CALL VOLUME': '', 'CALL IV': '',
        'CALL CHNG': '', 'CALL LTP': '', 'STRIKE': '',
        'PUT LTP': '', 'PUT CHNG': '', 'PUT IV': '', 'PUT VOLUME': '',
        'PUT CHNG IN OI': 'Update Time', 'PUT OI': current_time
    }])
    
    df = pd.concat([metadata, df, timestamp_row], ignore_index=True)
    return df

def main():
    ist = pytz.timezone('Asia/Kolkata')
    expiry_date = get_next_tuesday()
    
    data, expiry = get_option_chain(expiry=expiry_date)
    
    if data:
        df = create_option_chain_dataframe(data, expiry)
        os.makedirs('Data', exist_ok=True)
        df.to_csv('Data/Option.csv', index=False)
        
        current_time = datetime.now(ist).strftime('%d-%b %H:%M')
        print(f"Option chain saved to Data/Option.csv")
        print(f"Timestamp: {current_time} IST")
        print(f"Underlying Value: {data['records']['underlyingValue']}")
        print(f"Expiry Date: {expiry}")
    else:
        print("Failed to fetch option chain data")

if __name__ == "__main__":
    main()
