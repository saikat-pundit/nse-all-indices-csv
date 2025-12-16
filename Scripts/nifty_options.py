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
            'PUT_OI': pe_data.get('openInterest', 0),
            'PUT_CHNG_IN_OI': pe_data.get('changeinOpenInterest', 0),
            'PUT_VOLUME': pe_data.get('totalTradedVolume', 0),
            'PUT_IV': pe_data.get('impliedVolatility', 0),
            'PUT_LTP': pe_data.get('lastPrice', 0),
            'PUT_CHNG': pe_data.get('change', 0),
            'PUT_BID': pe_data.get('buyPrice1', 0),
            'PUT_BID_QTY': pe_data.get('buyQuantity1', 0),
            'PUT_ASK': pe_data.get('sellPrice1', 0),
            'PUT_ASK_QTY': pe_data.get('sellQuantity1', 0),
            'CALL_BID': ce_data.get('buyPrice1', 0),
            'CALL_BID_QTY': ce_data.get('buyQuantity1', 0),
            'CALL_ASK': ce_data.get('sellPrice1', 0),
            'CALL_ASK_QTY': ce_data.get('sellQuantity1', 0),
            'CALL_LTP': ce_data.get('lastPrice', 0),
            'CALL_CHNG': ce_data.get('change', 0),
            'CALL_IV': ce_data.get('impliedVolatility', 0),
            'CALL_VOLUME': ce_data.get('totalTradedVolume', 0),
            'CALL_CHNG_IN_OI': ce_data.get('changeinOpenInterest', 0),
            'CALL_OI': ce_data.get('openInterest', 0)
        }
        option_data.append(option_row)
    
    df = pd.DataFrame(option_data)
    
    column_order = [
        'PUT_OI', 'PUT_CHNG_IN_OI', 'PUT_VOLUME', 'PUT_IV', 'PUT_LTP', 
        'PUT_CHNG', 'PUT_BID', 'PUT_BID_QTY', 'PUT_ASK', 'PUT_ASK_QTY',
        'STRIKE',
        'CALL_BID', 'CALL_BID_QTY', 'CALL_ASK', 'CALL_ASK_QTY',
        'CALL_LTP', 'CALL_CHNG', 'CALL_IV', 'CALL_VOLUME',
        'CALL_CHNG_IN_OI', 'CALL_OI'
    ]
    
    df = df[column_order]
    
    metadata = pd.DataFrame([{
        'PUT_OI': 'UNDERLYING',
        'PUT_CHNG_IN_OI': underlying_value,
        'PUT_VOLUME': 'TIMESTAMP',
        'PUT_IV': timestamp,
        'PUT_LTP': '',
        'PUT_CHNG': '',
        'PUT_BID': '',
        'PUT_BID_QTY': '',
        'PUT_ASK': '',
        'PUT_ASK_QTY': '',
        'STRIKE': '',
        'CALL_BID': '',
        'CALL_BID_QTY': '',
        'CALL_ASK': '',
        'CALL_ASK_QTY': '',
        'CALL_LTP': '',
        'CALL_CHNG': '',
        'CALL_IV': '',
        'CALL_VOLUME': '',
        'CALL_CHNG_IN_OI': '',
        'CALL_OI': ''
    }])
    
    df = pd.concat([metadata, df], ignore_index=True)
    
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
