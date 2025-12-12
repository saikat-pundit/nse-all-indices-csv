import requests
import pandas as pd
import json
from datetime import datetime
import pytz
import os

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

# FOURTH API: Fetch GIFT-NIFTY data from MoneyControl
url_gift_nifty = "https://appfeeds.moneycontrol.com/jsonapi/market/indices&format=json&ind_id=in;gsx&source=globalindices"
response_gift_nifty = requests.get(url_gift_nifty, headers=headers)
data_gift_nifty = response_gift_nifty.json()

# FIFTH API: Fetch IND 5Y bond data
url_ind_5y = "https://priceapi.moneycontrol.com/pricefeed/usMarket/bond/GIND5Y:IND"
response_ind_5y = requests.get(url_ind_5y, headers=headers)
data_ind_5y = response_ind_5y.json()

# SIXTH API: Fetch IND 10Y bond data
url_ind_10y = "https://priceapi.moneycontrol.com/pricefeed/usMarket/bond/GIND10YR:IND"
response_ind_10y = requests.get(url_ind_10y, headers=headers)
data_ind_10y = response_ind_10y.json()

# SEVENTH API: Fetch IND 30Y bond data
url_ind_30y = "https://priceapi.moneycontrol.com/pricefeed/usMarket/bond/GIND30Y:IND"
response_ind_30y = requests.get(url_ind_30y, headers=headers)
data_ind_30y = response_ind_30y.json()

# Target indices list including bonds
target_indices = [
    "NIFTY 50",
    "INDIA VIX",
    "GIFT-NIFTY",
    "USD/INR",
    "GOLD",
    "SILVER",
    "IND 5Y",
    "IND 10Y",
    "IND 30Y",
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

# Process GIFT-NIFTY data from MoneyControl API - UPDATED SECTION
if 'indices' in data_gift_nifty:
    gift_data = data_gift_nifty['indices']
    
    # Extract GIFT-NIFTY data
    lastprice = gift_data.get('lastprice', '-')
    change = gift_data.get('change', '-')
    percentchange = gift_data.get('percentchange', '-')
    prevclose = gift_data.get('prevclose', '-')
    yearlyhigh = gift_data.get('yearlyhigh', '-')
    yearlylow = gift_data.get('yearlylow', '-')
    
    # Clean comma from digits - ADDED THIS FUNCTION
    def clean_number(value):
        if value == '-' or value is None:
            return '-'
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            # Remove commas and convert to float if possible
            cleaned = value.replace(',', '')
            try:
                return float(cleaned)
            except ValueError:
                return value
        return value
    
    # Clean all numeric fields
    lastprice = clean_number(lastprice)
    change = clean_number(change)
    prevclose = clean_number(prevclose)
    yearlyhigh = clean_number(yearlyhigh)
    yearlylow = clean_number(yearlylow)
    
    # Format percentage change
    if percentchange != '-':
        percent_change_str = f"{percentchange}%"
    else:
        percent_change_str = "-"
    
    index_dict['GIFT-NIFTY'] = {
        'Index Name': 'GIFT-NIFTY',
        'Last': lastprice,
        'Change': change,
        '% Change': percent_change_str,
        'Previous Close': prevclose,
        'Adv/Dec Ratio': '-',  # Not available in MoneyControl API
        'Year High': yearlyhigh,
        'Year Low': yearlylow
    }

# Add USD/INR data from NSE market status
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

# Process IND 5Y bond data
if data_ind_5y.get('code') == '200' and 'data' in data_ind_5y:
    bond_data = data_ind_5y['data']
    
    current_price = bond_data.get('current_price', '-')
    net_change = bond_data.get('net_change', '-')
    percent_change = bond_data.get('percent_change', '-')
    prev_close = bond_data.get('prev_close', '-')
    wk_high = bond_data.get('52wkHigh', '-')
    wk_low = bond_data.get('52wkLow', '-')
    
    # Format percentage change
    if percent_change != '-':
        percent_change_str = f"{percent_change}%"
    else:
        percent_change_str = "-"
    
    index_dict['IND 5Y'] = {
        'Index Name': 'IND 5Y',
        'Last': current_price,
        'Change': net_change,
        '% Change': percent_change_str,
        'Previous Close': prev_close,
        'Adv/Dec Ratio': '-',
        'Year High': wk_high,
        'Year Low': wk_low
    }

# Process IND 10Y bond data
if data_ind_10y.get('code') == '200' and 'data' in data_ind_10y:
    bond_data = data_ind_10y['data']
    
    current_price = bond_data.get('current_price', '-')
    net_change = bond_data.get('net_change', '-')
    percent_change = bond_data.get('percent_change', '-')
    prev_close = bond_data.get('prev_close', '-')
    wk_high = bond_data.get('52wkHigh', '-')
    wk_low = bond_data.get('52wkLow', '-')
    
    # Format percentage change
    if percent_change != '-':
        percent_change_str = f"{percent_change}%"
    else:
        percent_change_str = "-"
    
    index_dict['IND 10Y'] = {
        'Index Name': 'IND 10Y',
        'Last': current_price,
        'Change': net_change,
        '% Change': percent_change_str,
        'Previous Close': prev_close,
        'Adv/Dec Ratio': '-',
        'Year High': wk_high,
        'Year Low': wk_low
    }

# Process IND 30Y bond data
if data_ind_30y.get('code') == '200' and 'data' in data_ind_30y:
    bond_data = data_ind_30y['data']
    
    current_price = bond_data.get('current_price', '-')
    net_change = bond_data.get('net_change', '-')
    percent_change = bond_data.get('percent_change', '-')
    prev_close = bond_data.get('prev_close', '-')
    wk_high = bond_data.get('52wkHigh', '-')
    wk_low = bond_data.get('52wkLow', '-')
    
    # Format percentage change
    if percent_change != '-':
        percent_change_str = f"{percent_change}%"
    else:
        percent_change_str = "-"
    
    index_dict['IND 30Y'] = {
        'Index Name': 'IND 30Y',
        'Last': current_price,
        'Change': net_change,
        '% Change': percent_change_str,
        'Previous Close': prev_close,
        'Adv/Dec Ratio': '-',
        'Year High': wk_high,
        'Year Low': wk_low
    }

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
                    'Year High': '-',
                    'Year Low': '-'
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

# Save to CSV
os.makedirs('Data', exist_ok=True)
df = pd.DataFrame(records)
df.to_csv('Data/nse_all_indices.csv', index=False)
print(f"CSV created successfully! Updated at {current_time} IST")
print(f"Total indices processed: {len(index_dict)} out of {len(target_indices)}")
