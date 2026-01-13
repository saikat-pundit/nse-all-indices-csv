import requests
import pandas as pd
from datetime import datetime, timedelta, time, date
import pytz
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from iv_calculator import CalcIvGreeks, TryMatchWith

# Define holidays (same as before)
HOLIDAYS = [
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-12-25", "2026-01-26", "2026-03-03", 
    "2026-03-26", "2026-03-31", "2026-04-03", "2026-04-14", 
    "2026-05-01", "2026-05-28", "2026-06-26", "2026-09-14", 
    "2026-10-02", "2026-10-20", "2026-11-10", "2026-11-24", 
    "2026-12-25", "2026-01-15"
]

HOLIDAY_DATES = [datetime.strptime(holiday, "%Y-%m-%d").date() for holiday in HOLIDAYS]

def is_market_day():
    """Check if current day is a trading day (weekday and not a holiday)"""
    ist = pytz.timezone('Asia/Kolkata')
    ist_now = datetime.now(ist)
    current_date = ist_now.date()
    
    if ist_now.weekday() >= 5:
        return False
    
    if current_date in HOLIDAY_DATES:
        return False
    
    return True

def is_market_hours():
    """Check if current time is within market hours (IST: 9:15 AM to 3:40 PM)"""
    ist = pytz.timezone('Asia/Kolkata')
    ist_now = datetime.now(ist)
    
    if not is_market_day():
        return False
    
    current_time = ist_now.time()
    market_open = time(9, 15)
    market_close = time(15, 40)
    
    return market_open <= current_time <= market_close

def get_market_status_message():
    """Get detailed message about why market is closed"""
    ist = pytz.timezone('Asia/Kolkata')
    ist_now = datetime.now(ist)
    current_date = ist_now.date()
    current_time_str = ist_now.strftime('%Y-%m-%d %H:%M:%S IST')
    weekday = ist_now.strftime('%A')
    
    if ist_now.weekday() >= 5:
        return f"Market closed - {weekday} (Weekend)", False
    
    if current_date in HOLIDAY_DATES:
        holiday_str = ""
        for holiday in HOLIDAYS:
            if holiday.startswith(str(current_date)):
                holiday_str = f" ({holiday})"
                break
        return f"Market closed - {weekday}{holiday_str} (Holiday)", False
    
    market_open = time(9, 15)
    market_close = time(15, 40)
    current_time = ist_now.time()
    
    if current_time < market_open:
        time_to_open = datetime.combine(current_date, market_open) - datetime.combine(current_date, current_time)
        hours, remainder = divmod(time_to_open.seconds, 3600)
        minutes = remainder // 60
        return f"Market opens in {hours}h {minutes}m at 9:15 AM", False
    
    if current_time > market_close:
        return f"Market closed at 3:30 PM today", False
    
    return f"Market open - {weekday}", True

def main():
    status_message, is_open = get_market_status_message()
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')
    
    print(f"Current time: {current_time}")
    print(f"Status: {status_message}")
    
    if not is_open:
        print("Script not running - outside trading hours")
        print("Exiting...")
        return
    
    print("Fetching option chain data...")
    
    expiry_date = get_next_tuesday()
    data, expiry = get_option_chain(expiry=expiry_date)
    
    if data:
        df = create_option_chain_dataframe(data, expiry)
        os.makedirs('Data', exist_ok=True)
        
        output_file = 'Data/Option.csv'
        df.to_csv(output_file, index=False)
        
        current_time = datetime.now(ist).strftime('%d-%b %H:%M')
        
        print(f"Option chain saved to: {output_file}")
        print(f"Timestamp: {current_time} IST")
        print(f"Underlying: {data['records']['underlyingValue']}")
        print(f"Expiry: {expiry}")
        print(f"Rows: {len(df)}")
    else:
        print("Failed to fetch option chain data")

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nseindia.com/option-chain'
}

def get_future_price(symbol="NIFTY", data=None):
    """Calculate synthetic future price using ATM put-call parity with 50-multiple strikes"""
    try:
        if data is None:
            print("Warning: No option chain data available for future calculation")
            return 0
            
        # Get all strikes including 50-multiples
        all_strikes = []
        for item in data['records']['data']:
            strike = item['strikePrice']
            # Include both 50 and 100 multiples
            if strike % 50 == 0:
                all_strikes.append(strike)
        
        if not all_strikes:
            print("Warning: No valid strikes found for future calculation")
            return 0
        
        underlying_value = data['records']['underlyingValue']
        
        # Find ATM strike (closest to underlying value, can be 50-multiple)
        atm_strike = min(all_strikes, key=lambda x: abs(x - underlying_value))
        
        # Find the data for ATM strike
        atm_data = None
        for item in data['records']['data']:
            if item['strikePrice'] == atm_strike:
                atm_data = item
                break
        
        if atm_data is None:
            print(f"Warning: No data found for ATM strike {atm_strike}")
            return 0
        
        # Get ATM call and put prices
        ce_data = atm_data.get('CE', {})
        pe_data = atm_data.get('PE', {})
        
        atm_call_price = ce_data.get('lastPrice', 0)
        atm_put_price = pe_data.get('lastPrice', 0)
        
        # Apply minimum 5 paisa for calculation
        calc_call_price = max(atm_call_price, 0.05)
        calc_put_price = max(atm_put_price, 0.05)
        
        # Calculate synthetic future: F = K + C - P
        synthetic_future = atm_strike + calc_call_price - calc_put_price
        
        print(f"Future Calculation: ATM Strike={atm_strike} (50-multiple), "
              f"CE={calc_call_price:.2f}, PE={calc_put_price:.2f}, "
              f"Future={synthetic_future:.2f}")
        
        return synthetic_future
        
    except Exception as e:
        print(f"Error calculating synthetic future: {e}")
        return 0

def get_next_tuesday():
    """Get the next Tuesday expiry date"""
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    today = now.date()
    
    days_ahead = 1 - today.weekday()
    if days_ahead < 0 or (days_ahead == 0 and now.hour >= 16):
        days_ahead += 7
    
    next_tuesday_date = today + timedelta(days=days_ahead)
    return next_tuesday_date.strftime('%d-%b-%Y').upper()

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

def find_atm_strike_and_prices(df, future_price):
    """
    Find ATM strike based on future price with validation
    """
    valid_rows = []
    for _, row in df.iterrows():
        if isinstance(row['STRIKE'], (int, float)):
            valid_rows.append(row)
    
    if not valid_rows:
        return None, 0, 0
    
    # Find strike closest to future price
    atm_strike = min(valid_rows, key=lambda x: abs(x['STRIKE'] - future_price))['STRIKE']
    
    atm_row = None
    for _, row in df.iterrows():
        if row['STRIKE'] == atm_strike:
            atm_row = row
            break
    
    if atm_row is None:
        return atm_strike, 0, 0
    
    atm_call_price = float(atm_row['CALL LTP']) if atm_row['CALL LTP'] not in ['', None, 0] else 0
    atm_put_price = float(atm_row['PUT LTP']) if atm_row['PUT LTP'] not in ['', None, 0] else 0
    
    # Validate ATM prices
    if atm_call_price <= 0 or atm_put_price <= 0:
        print(f"Warning: ATM strike {atm_strike} has low liquidity: "
              f"Call={atm_call_price}, Put={atm_put_price}")
    
    # Use minimum 5 paisa for calculation
    calc_call_price = max(atm_call_price, 0.05)
    calc_put_price = max(atm_put_price, 0.05)
    
    return atm_strike, calc_call_price, calc_put_price

def calculate_iv_for_dataframe(df, future_price, expiry_datetime):
    """
    Calculate IV using Black-76 model with futures price
    """
    # Use future price for ATM selection
    atm_strike, atm_call_price, atm_put_price = find_atm_strike_and_prices(df, future_price)
    
    if atm_strike is None or future_price <= 0:
        return [''] * len(df)
    
    print(f"ATM Calculation: Strike={atm_strike}, Future={future_price:.2f}, "
          f"Call={atm_call_price:.2f}, Put={atm_put_price:.2f}")
    
    iv_values = []
    
    for idx, row in df.iterrows():
        if not isinstance(row['STRIKE'], (int, float)):
            iv_values.append('')
            continue
            
        strike = float(row['STRIKE'])
        
        call_price = float(row['CALL LTP']) if row['CALL LTP'] not in ['', None] else 0
        put_price = float(row['PUT LTP']) if row['PUT LTP'] not in ['', None] else 0
        
        # Skip if both prices are zero or invalid
        if (call_price <= 0 and put_price <= 0) or strike <= 0:
            iv_values.append('')
            continue
        
        # Use minimum 5 paisa for calculation
        calc_call_price = max(call_price, 0.05) if call_price > 0 else 0.05
        calc_put_price = max(put_price, 0.05) if put_price > 0 else 0.05
        
        try:
            # Initialize calculator with Black-76 parameters
            calculator = CalcIvGreeks(
                FuturePrice=future_price,
                AtmStrike=atm_strike,
                AtmStrikeCallPrice=atm_call_price,
                AtmStrikePutPrice=atm_put_price,
                ExpiryDateTime=expiry_datetime,
                tryMatchWith=TryMatchWith.CUSTOM
            )
            
            # Get IV and Greeks for this strike
            result = calculator.GetImpVolAndGreeks(
                StrikePrice=strike,
                StrikeCallPrice=calc_call_price,
                StrikePutPrice=calc_put_price,
                useOtmLiquidity=True
            )
            
            iv_values.append(round(result['ImplVol'], 2))
                
        except Exception as e:
            print(f"Error calculating IV for strike {strike}: {e}")
            iv_values.append('')
    
    return iv_values

def create_option_chain_dataframe(data, expiry_date):
    filtered_strikes, underlying_value, rounded_strike, _ = get_filtered_strike_prices(data)
    
    strike_map = {item['strikePrice']: item for item in data['records']['data'] if item['strikePrice'] % 100 == 0}
    
    option_data = []
    inserted_underlying = False
    
    for strike in filtered_strikes:
        if strike not in strike_map:
            continue
        
        if strike > underlying_value and not inserted_underlying:
            option_data.append({
                'CALL OI': '', 'CALL OI CHNG': '', 'CALL VOLUME': '', 
                'CALL CHNG': '', 'CALL LTP': '', 'STRIKE': f"{underlying_value}",
                'PUT LTP': 'Expiry: ' + expiry_date, 'PUT CHNG': '', 
                'PUT VOLUME': '', 'PUT OI CHNG': '', 'PUT OI': ''
            })
            inserted_underlying = True
        
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
    
    if not inserted_underlying:
        option_data.append({
            'CALL OI': '', 'CALL OI CHNG': '', 'CALL VOLUME': '', 
            'CALL CHNG': '', 'CALL LTP': '', 'STRIKE': f"{underlying_value}",
            'PUT LTP': 'Expiry: ' + expiry_date, 'PUT CHNG': '', 
            'PUT VOLUME': '', 'PUT OI CHNG': '', 'PUT OI': ''
        })
    
    df = pd.DataFrame(option_data)
    
    # Get futures price
    future_price = get_future_price(data=data)  # Line 220-221

    if future_price <= 0:  # Line 223 - FIXED INDENTATION
        print("Warning: Could not calculate synthetic future, using spot as fallback")
        future_price = underlying_value
    
    print(f"Future Price: {future_price:.2f}, Spot: {underlying_value}")  # Line 227
    
    # Create expiry datetime
    expiry_datetime = datetime.strptime(expiry_date, '%d-%b-%Y')
    expiry_datetime = expiry_datetime.replace(hour=15, minute=30, second=0)
    expiry_datetime = pytz.timezone('Asia/Kolkata').localize(expiry_datetime)
    
    # Calculate IV using Black-76
    iv_column = calculate_iv_for_dataframe(df, future_price, expiry_datetime)
    
    df['IV'] = iv_column
    
    columns_order = ['CALL OI', 'CALL OI CHNG', 'CALL VOLUME', 'CALL CHNG', 'CALL LTP',
                     'STRIKE', 'IV', 
                     'PUT LTP', 'PUT CHNG', 'PUT VOLUME', 'PUT OI CHNG', 'PUT OI']
    df = df[columns_order]
    
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist).strftime('%d-%b %H:%M')
    
    timestamp_row = pd.DataFrame([{
        'CALL OI': '', 'CALL OI CHNG': '', 'CALL VOLUME': '', 
        'CALL CHNG': '', 'CALL LTP': '', 'STRIKE': '',
        'IV': '', 
        'PUT LTP': '', 'PUT CHNG': '', 'PUT VOLUME': '',
        'PUT OI CHNG': 'Update Time', 'PUT OI': current_time
    }])
    
    df = pd.concat([df, timestamp_row], ignore_index=True)
    
    return df

if __name__ == "__main__":
    main()
