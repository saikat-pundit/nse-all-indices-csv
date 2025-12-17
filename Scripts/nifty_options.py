"""
Option Chain Data Extractor with IV Calculation
Fetches NSE Option Chain and calculates Implied Volatility
"""

import requests
import pandas as pd
from datetime import datetime, timedelta, time
import pytz
import os
import math
import sys

# Add the current directory to path to import iv_calculator
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from iv_calculator import CalcIvGreeks, TryMatchWith, calculate_single_iv_for_option

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nseindia.com/option-chain'
}


def get_future_price(symbol="NIFTY"):
    """Fetch futures price for NIFTY from TradingView"""
    try:
        if "NIFTY" in symbol.upper():
            url = "https://scanner.tradingview.com/symbol?symbol=NSEIX:NIFTY1!&fields=close&no_404=true"
            response = requests.get(url, headers=headers, timeout=5)
            data = response.json()
            return float(data.get('close', 0))
        return 0
    except Exception as e:
        print(f"Warning: Could not fetch future price: {e}")
        return 0


def get_next_tuesday():
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    today = now.date()
    days_ahead = 1 - today.weekday()
    
    if days_ahead < 0 or (days_ahead == 0 and now.hour >= 16):
        days_ahead += 7
    
    next_tuesday = today + timedelta(days=days_ahead)
    return next_tuesday.strftime('%d-%b-%Y').upper()


def round_to_nearest_100(price):
    return round(price / 100) * 100


def get_filtered_strike_prices(data, strike_range=10):
    underlying_value = data['records']['underlyingValue']
    rounded_strike = round_to_nearest_100(underlying_value)
    
    all_strikes = sorted([item['strikePrice'] for item in data['records']['data'] if item['strikePrice'] % 100 == 0])
    
    target_index = all_strikes.index(rounded_strike) if rounded_strike in all_strikes else \
                   min(range(len(all_strikes)), key=lambda i: abs(all_strikes[i] - rounded_strike))
    
    start_index = max(0, target_index - strike_range)
    end_index = min(len(all_strikes), target_index + strike_range + 1)
    
    return all_strikes[start_index:end_index], underlying_value, rounded_strike, target_index - start_index


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


def find_atm_strike_and_prices(df, spot_price):
    """
    Find ATM strike and its call/put prices from DataFrame
    Returns: (atm_strike, atm_call_price, atm_put_price)
    """
    # Get valid strike rows
    valid_rows = []
    for _, row in df.iterrows():
        if isinstance(row['STRIKE'], (int, float)):
            valid_rows.append(row)
    
    if not valid_rows:
        return None, 0, 0
    
    # Find ATM strike (closest to spot)
    atm_strike = min(valid_rows, key=lambda x: abs(x['STRIKE'] - spot_price))['STRIKE']
    
    # Find ATM row
    atm_row = None
    for _, row in df.iterrows():
        if row['STRIKE'] == atm_strike:
            atm_row = row
            break
    
    if atm_row is None:
        return atm_strike, 0, 0
    
    # Get ATM prices
    atm_call_price = float(atm_row['CALL LTP']) if atm_row['CALL LTP'] not in ['', None] else 0
    atm_put_price = float(atm_row['PUT LTP']) if atm_row['PUT LTP'] not in ['', None] else 0
    
    return atm_strike, atm_call_price, atm_put_price


def calculate_iv_for_dataframe(df, spot_price, future_price, expiry_datetime):
    """
    Calculate single IV column for entire DataFrame
    - For strikes BELOW ATM: Use PUT IV
    - For strikes AT/ABOVE ATM: Use CALL IV
    """
    # Find ATM strike and prices
    atm_strike, atm_call_price, atm_put_price = find_atm_strike_and_prices(df, spot_price)
    
    if atm_strike is None:
        return [''] * len(df)
    
    iv_values = []
    
    for _, row in df.iterrows():
        # Skip non-strike rows (timestamp, underlying value rows)
        if not isinstance(row['STRIKE'], (int, float)):
            iv_values.append('')
            continue
            
        strike = float(row['STRIKE'])
        
        # Get option prices
        call_price = float(row['CALL LTP']) if row['CALL LTP'] not in ['', None] else 0
        put_price = float(row['PUT LTP']) if row['PUT LTP'] not in ['', None] else 0
        
        # Skip if both prices are zero or invalid
        if call_price <= 0 and put_price <= 0:
            iv_values.append('')
            continue
        
        # Ensure minimum price for calculation
        calc_call_price = max(call_price, 0.01)
        calc_put_price = max(put_price, 0.01)
        
        try:
            # Create calculator instance
            calculator = CalcIvGreeks(
                SpotPrice=spot_price,
                FuturePrice=future_price,
                AtmStrike=atm_strike,
                AtmStrikeCallPrice=atm_call_price if atm_call_price > 0 else 1.0,
                AtmStrikePutPrice=atm_put_price if atm_put_price > 0 else 1.0,
                ExpiryDateTime=expiry_datetime,
                StrikePrice=strike,
                StrikeCallPrice=calc_call_price,
                StrikePutPrice=calc_put_price,
                tryMatchWith=TryMatchWith.SENSIBULL  # 0% interest rate
            )
            
            # Determine which IV to use
            if strike < atm_strike:
                # BELOW ATM: Use PUT IV
                iv = calculator.PutImplVol() * 100
            else:
                # AT or ABOVE ATM: Use CALL IV
                iv = calculator.CallImplVol() * 100
            
            # Format IV value
            if iv > 0:
                iv_values.append(round(iv, 2))
            else:
                iv_values.append('')
                
        except Exception as e:
            # If calculation fails, add empty value
            print(f"Warning: IV calculation failed for strike {strike}: {e}")
            iv_values.append('')
    
    return iv_values


def create_option_chain_dataframe(data, expiry_date):
    filtered_strikes, underlying_value, rounded_strike, underlying_index = get_filtered_strike_prices(data)
    
    strike_map = {item['strikePrice']: item for item in data['records']['data'] if item['strikePrice'] % 100 == 0}
    
    option_data = []
    
    for i, strike in enumerate(filtered_strikes):
        if strike not in strike_map:
            continue
        
        # Insert underlying row BEFORE the rounded strike
        if i == underlying_index:
            option_data.append({
                'CALL OI': '', 'CALL OI CHNG': '', 'CALL VOLUME': '', 
                'CALL CHNG': '', 'CALL LTP': '', 'STRIKE': f"{underlying_value}",
                'PUT LTP': 'Expiry: ' + expiry_date, 'PUT CHNG': '', 
                'PUT VOLUME': '', 'PUT OI CHNG': '', 'PUT OI': ''
            })
        
        item = strike_map[strike]
        ce_data = item.get('CE', {})
        pe_data = item.get('PE', {})
        
        option_data.append({
            'CALL OI': ce_data.get('openInterest', 0),
            'CALL OI CHNG': ce_data.get('changeinOpenInterest', 0),
            'CALL VOLUME': ce_data.get('totalTradedVolume', 0),
            'CALL CHNG': ce_data.get('change', 0),
            'CALL LTP': ce_data.get('lastPrice', 0),
            'STRIKE': strike,
            'PUT LTP': pe_data.get('lastPrice', 0),
            'PUT CHNG': pe_data.get('change', 0),
            'PUT VOLUME': pe_data.get('totalTradedVolume', 0),
            'PUT OI CHNG': pe_data.get('changeinOpenInterest', 0),
            'PUT OI': pe_data.get('openInterest', 0)
        })
    
    df = pd.DataFrame(option_data)
    
    # ===== ADD IV CALCULATION =====
    spot_price = underlying_value
    future_price = get_future_price()
    
    # Convert expiry string to datetime (15:30 IST expiry)
    expiry_datetime = datetime.strptime(expiry_date, '%d-%b-%Y')
    expiry_datetime = expiry_datetime.replace(hour=15, minute=30, second=0)
    expiry_datetime = pytz.timezone('Asia/Kolkata').localize(expiry_datetime)
    
    print(f"Calculating IV...")
    print(f"Spot Price: {spot_price}")
    print(f"Future Price: {future_price if future_price > 0 else 'Using Spot'}")
    print(f"Expiry: {expiry_datetime}")
    
    # Calculate IV for all rows
    iv_column = calculate_iv_for_dataframe(df, spot_price, future_price, expiry_datetime)
    
    # Add IV column to DataFrame
    df['IV'] = iv_column
    
    # Reorder columns to put IV next to STRIKE
    columns_order = ['CALL OI', 'CALL OI CHNG', 'CALL VOLUME', 'CALL CHNG', 'CALL LTP',
                     'STRIKE', 'IV',  # IV column placed here
                     'PUT LTP', 'PUT CHNG', 'PUT VOLUME', 'PUT OI CHNG', 'PUT OI']
    df = df[columns_order]
    
    # Add timestamp row
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist).strftime('%d-%b %H:%M')
    
    timestamp_row = pd.DataFrame([{
        'CALL OI': '', 'CALL OI CHNG': '', 'CALL VOLUME': '', 
        'CALL CHNG': '', 'CALL LTP': '', 'STRIKE': '',
        'IV': '',  # Empty IV for timestamp row
        'PUT LTP': '', 'PUT CHNG': '', 'PUT VOLUME': '',
        'PUT OI CHNG': 'Update Time', 'PUT OI': current_time
    }])
    
    df = pd.concat([df, timestamp_row], ignore_index=True)
    
    return df


def main():
    ist = pytz.timezone('Asia/Kolkata')
    expiry_date = get_next_tuesday()
    
    print(f"Fetching option chain for expiry: {expiry_date}")
    data, expiry = get_option_chain(expiry=expiry_date)
    
    if data:
        df = create_option_chain_dataframe(data, expiry)
        os.makedirs('Data', exist_ok=True)
        
        # Save to CSV
        output_file = 'Data/Option_With_IV.csv'
        df.to_csv(output_file, index=False)
        
        # Display summary
        current_time = datetime.now(ist).strftime('%d-%b %H:%M')
        print(f"\n{'='*50}")
        print(f"Option chain with IV saved to: {output_file}")
        print(f"Timestamp: {current_time} IST")
        print(f"Underlying Value: {data['records']['underlyingValue']}")
        print(f"Expiry Date: {expiry}")
        
        # Show sample of data with IV
        print(f"\nSample of data with IV:")
        print(f"{'='*50}")
        print(f"{'STRIKE':>8} | {'CALL LTP':>8} | {'PUT LTP':>8} | {'IV':>6}")
        print(f"{'-'*8}-|{'-'*9}-|{'-'*9}-|{'-'*7}")
        
        # Display first 5 strike rows with IV
        count = 0
        for _, row in df.iterrows():
            if isinstance(row['STRIKE'], (int, float)) and count < 5:
                strike = row['STRIKE']
                call_ltp = row['CALL LTP'] if row['CALL LTP'] != '' else '0'
                put_ltp = row['PUT LTP'] if row['PUT LTP'] != '' else '0'
                iv = row['IV'] if row['IV'] != '' else 'N/A'
                print(f"{strike:>8} | {call_ltp:>8} | {put_ltp:>8} | {iv:>6}")
                count += 1
        
        print(f"{'='*50}")
        print(f"Total rows saved: {len(df)}")
        
    else:
        print("Failed to fetch option chain data")


if __name__ == "__main__":
    main()
