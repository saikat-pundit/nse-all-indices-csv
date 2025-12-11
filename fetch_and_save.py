import requests
import pandas as pd
import json
from datetime import datetime
import pytz

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
}

# FIRST API: Fetch all indices data from NSE
url_indices = "https://www.nseindia.com/api/allIndices"
response_indices = requests.get(url_indices, headers=headers)
data_indices = response_indices.json()

# SECOND API: Fetch market status data from NSE
url_market = "https://www.nseindia.com/api/marketStatus"
response_market = requests.get(url_market, headers=headers)
data_market = response_market.json()

# THIRD API: Fetch commodity data from Money Control
url_commodities = "https://priceapi.moneycontrol.com/technicalCompanyData/commodity/getMajorCommodities?tabName=SPOT&deviceType=W"
response_commodities = requests.get(url_commodities, headers=headers)
data_commodities = response_commodities.json()

# Target indices list including GIFT-NIFTY and USD/INR
target_indices = [
    "NIFTY 50",
    "INDIA VIX",
    "GIFT-NIFTY",
    "USD/INR",
    "GOLD",
    "SILVER",
    "NIFTY 10 YR BENCHMARK G-SEC",
    "NIFTY NEXT 50",
    "NIFTY MIDCAP SELECT",
    "NIFTY MIDCAP 50",
    "NIFTY SMALLCAP 50",
    "NIFTY 500",
    "NIFTY ALPHA 50",
    "NIFTY IT",
    "NIFTY BANK",
    "NIFTY FINANCIAL SERVICES",
    "NIFTY PSU BANK",
    "NIFTY PRIVATE BANK",
    "NIFTY FMCG",
    "NIFTY CONSUMER DURABLES",
    "NIFTY PHARMA",
    "NIFTY HEALTHCARE INDEX",
    "NIFTY METAL",
    "NIFTY AUTO",
    "NIFTY SERVICES SECTOR",
    "NIFTY OIL & GAS",
    "NIFTY CHEMICALS",
    "NIFTY COMMODITIES",
    "NIFTY INDIA CONSUMPTION",
    "NIFTY PSE"
]

index_dict = {}

# Process all indices data from NSE
for item in data_indices['data']:
    index_name = item.get('index')
    
    if index_name in target_indices:
        try:
            advances = int(item.get('advances', 0))
        except (ValueError, TypeError):
            advances = 0
        
        try:
            declines = int(item.get('declines', 0))
        except (ValueError, TypeError):
            declines = 0
        
        try:
            unchanged = int(item.get('unchanged', 0))
        except (ValueError, TypeError):
            unchanged = 0
        
        if declines != 0:
            adv_dec_ratio = advances / declines
            adv_dec_ratio_str = f"{adv_dec_ratio:.2f}"
        else:
            if advances > 0:
                adv_dec_ratio_str = "Max"
            else:
                adv_dec_ratio_str = "-"
        
        percent_change = item.get('percentChange')
        if percent_change is not None:
            percent_change_str = f"{percent_change}%"
        else:
            percent_change_str = "-"
        
        index_dict[index_name] = {
            'Index Name': index_name,
            'Last': item.get('last'),
            'Change': item.get('variation'),
            '% Change': percent_change_str,
            'Previous Close': item.get('previousClose'),
            'Adv/Dec Ratio': adv_dec_ratio_str,
            'Year High': item.get('yearHigh'),
            'Year Low': item.get('yearLow')
        }

# Add GIFT-NIFTY data
if 'giftnifty' in data_market:
    gift = data_market['giftnifty']
    index_dict['GIFT-NIFTY'] = {
        'Index Name': 'GIFT-NIFTY',
        'Last': gift.get('LASTPRICE', '-'),
        'Change': '-',
        '% Change': '-',
        'Previous Close': '-',
        'Adv/Dec Ratio': '-',
        'Year High': '-',
        'Year Low': '-'
    }

# Add USD/INR data
for item in data_market['marketState']:
    if item.get('market') == 'currencyfuture':
        index_dict['USD/INR'] = {
            'Index Name': 'USD/INR',
            'Last': item.get('last', '-'),
            'Change': '-',
            '% Change': '-',
            'Previous Close': '-',
            'Adv/Dec Ratio': '-',
            'Year High': '-',
            'Year Low': '-'
        }
        break

# Add GOLD and SILVER data from Money Control API
if 'data' in data_commodities and 'list' in data_commodities['data']:
    for commodity in data_commodities['data']['list']:
        symbol = commodity.get('symbol')
        
        if symbol in ['GOLD', 'SILVER']:
            try:
                last_price = float(commodity.get('lastPrice', 0))
                price_change = float(commodity.get('priceChange', 0))
                price_change_percentage = commodity.get('priceChangePercentage', '0')
                
                # Calculate previous close using formula: previousClose = lastPrice - priceChange
                previous_close = last_price - price_change
                
                # For year high/low, use current price (no historical data available)
                index_dict[symbol] = {
                    'Index Name': symbol,
                    'Last': last_price,
                    'Change': price_change,
                    '% Change': f"{price_change_percentage}%",
                    'Previous Close': round(previous_close, 2),
                    'Adv/Dec Ratio': '-',
                    'Year High': last_price,  # Using current price since no historical
                    'Year Low': last_price    # Using current price since no historical
                }
            except (ValueError, TypeError) as e:
                print(f"Error processing {symbol}: {e}")
                continue

# Create records in the specified order
records = []
for index_name in target_indices:
    if index_name in index_dict:
        records.append(index_dict[index_name])
    else:
        # Create empty entry for missing indices
        records.append({
            'Index Name': index_name,
            'Last': '-',
            'Change': '-',
            '% Change': '-',
            'Previous Close': '-',
            'Adv/Dec Ratio': '-',
            'Year High': '-',
            'Year Low': '-'
        })

# Add timestamp row
ist = pytz.timezone('Asia/Kolkata')
current_time = datetime.now(ist).strftime('%d-%b %H:%M')

# Add timestamp as last row with proper formatting
records.append({
    'Index Name': '',
    'Last': '',
    'Change': '',
    '% Change': '',
    'Previous Close': '',
    'Adv/Dec Ratio': '',
    'Year High': 'Updated Time:',
    'Year Low': current_time
})

df = pd.DataFrame(records)
df.to_csv('nse_all_indices.csv', index=False)
print("CSV created successfully!")
