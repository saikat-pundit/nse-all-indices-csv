import requests, pandas as pd, os, pytz
from datetime import datetime

headers = {'User-Agent': 'Mozilla/5.0'}
TV_SYMBOLS = {"USD/INR": "FX_IDC:USDINR", "GIFT-NIFTY": "NSEIX:NIFTY1!", "GOLD": "MCX:GOLD1!", "SILVER": "MCX:SILVER1!", "IND 5Y": "TVC:IN05Y", "IND 10Y": "TVC:IN10Y", "IND 30Y": "TVC:IN30Y"}
target_indices = ["NIFTY 50", "INDIA VIX", "GIFT-NIFTY", "USD/INR", "GOLD", "SILVER", "IND 5Y", "IND 10Y", "IND 30Y", "NIFTY NEXT 50", "NIFTY MIDCAP SELECT", "NIFTY MIDCAP 50", "NIFTY SMALLCAP 50", "NIFTY 500", "NIFTY ALPHA 50", "NIFTY IT", "NIFTY BANK", "NIFTY FINANCIAL SERVICES", "NIFTY PSU BANK", "NIFTY PRIVATE BANK", "NIFTY FMCG", "NIFTY CONSUMER DURABLES", "NIFTY PHARMA", "NIFTY HEALTHCARE INDEX", "NIFTY METAL", "NIFTY AUTO", "NIFTY SERVICES SECTOR", "NIFTY OIL & GAS", "NIFTY CHEMICALS", "NIFTY COMMODITIES", "NIFTY INDIA CONSUMPTION", "NIFTY PSE"]

def format_index_name(name):
    if name == "NIFTY INDIA CONSUMPTION": return "CONSUMPTION"
    return name.replace("NIFTY ", "") if name.startswith("NIFTY ") and name not in ["NIFTY 50", "NIFTY 500", "GIFT-NIFTY"] else name

def format_value(value, key, index_name):
    if value == '-' or value is None: return '-'
    try:
        if key in ['% Chng', 'Adv:Dec']: return f"{float(value):.2f}%" if key == '% Chng' else f"{float(value):.2f}"
        if index_name in ["INDIA VIX", "USD/INR", "IND 5Y", "IND 10Y", "IND 30Y"]:
            return f"{float(value):.2f}" if key in ['LTP', 'Chng', 'Previous', 'Yr Hi', 'Yr Lo'] else str(float(value))
        if key in ['Chng', 'LTP', 'Previous', 'Yr Hi', 'Yr Lo']:
            return str(int(float(value))) if '.' in str(value) else str(float(value))
        return str(float(value))
    except: return '-'

index_dict = {}
for name, symbol in TV_SYMBOLS.items():
    url = f"https://scanner.tradingview.com/symbol?symbol={symbol}&fields=close[1],change_abs,price_52_week_high,price_52_week_low,close,change&no_404=true"
    try:
        data = requests.get(url, headers=headers, timeout=5).json()
        index_dict[name] = {
            'Index': format_index_name(name), 'LTP': data.get('close'), 'Chng': data.get('change_abs'),
            '% Chng': data.get('change'), 'Previous': data.get('close[1]'), 'Adv:Dec': '-',
            'Yr Hi': data.get('price_52_week_high'), 'Yr Lo': data.get('price_52_week_low')
        }
    except: pass

try:
    data = requests.get("https://www.nseindia.com/api/allIndices", headers=headers, timeout=5).json()
    for item in data.get('data', []):
        name = item.get('index')
        if name in TV_SYMBOLS or name not in target_indices: continue
        adv, dec = int(item.get('advances', 0)), int(item.get('declines', 0))
        adv_dec = f"{adv/dec:.2f}" if dec != 0 else "Max" if adv > 0 else "-"
        index_dict[name] = {
            'Index': format_index_name(name), 'LTP': item.get('last'), 'Chng': item.get('variation'),
            '% Chng': item.get('percentChange'), 'Previous': item.get('previousClose'), 'Adv:Dec': adv_dec,
            'Yr Hi': item.get('yearHigh'), 'Yr Lo': item.get('yearLow')
        }
except: pass

records = []
for idx in target_indices:
    formatted_name = format_index_name(idx)
    if idx in index_dict:
        rec = {k: format_value(v, k, idx) for k, v in index_dict[idx].items()}
        rec['Index'] = formatted_name
    else:
        rec = {'Index': formatted_name, 'LTP': '-', 'Chng': '-', '% Chng': '-', 'Previous': '-', 'Adv:Dec': '-', 'Yr Hi': '-', 'Yr Lo': '-'}
    records.append(rec)

records.append({'Index': '', 'LTP': '', 'Chng': '', '% Chng': '', 'Previous': '', 'Adv:Dec': '', 'Yr Hi': 'Updated Time:', 'Yr Lo': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%b %H:%M')})
os.makedirs('Data', exist_ok=True)
pd.DataFrame(records).to_csv('Data/nse_all_indices.csv', index=False)
