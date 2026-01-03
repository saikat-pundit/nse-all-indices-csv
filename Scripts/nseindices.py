import requests, pandas as pd, os, pytz
from datetime import datetime

TV_SYMBOLS = {"USD/INR": "FX_IDC:USDINR", "GIFT-NIFTY": "NSEIX:NIFTY1!", "GOLD": "MCX:GOLD1!", "SILVER": "MCX:SILVER1!", "IND 5Y": "TVC:IN05Y", "IND 10Y": "TVC:IN10Y", "IND 30Y": "TVC:IN30Y"}
target_indices = ["NIFTY 50", "INDIA VIX", "GIFT-NIFTY", "USD/INR", "GOLD", "SILVER", "IND 5Y", "IND 10Y", "IND 30Y", "NIFTY NEXT 50", "NIFTY MIDCAP SELECT", "NIFTY MIDCAP 50", "NIFTY SMALLCAP 50", "NIFTY 500", "NIFTY ALPHA 50", "NIFTY IT", "NIFTY BANK", "NIFTY FINANCIAL SERVICES", "NIFTY PSU BANK", "NIFTY PRIVATE BANK", "NIFTY FMCG", "NIFTY CONSUMER DURABLES", "NIFTY PHARMA", "NIFTY HEALTHCARE INDEX", "NIFTY METAL", "NIFTY AUTO", "NIFTY SERVICES SECTOR", "NIFTY OIL & GAS", "NIFTY CHEMICALS", "NIFTY COMMODITIES", "NIFTY INDIA CONSUMPTION", "NIFTY PSE", "NIFTY REALTY"]

def format_index_name(name):
    if name == "NIFTY INDIA CONSUMPTION": return "CONSUMPTION"
    return name.replace("NIFTY ", "") if name.startswith("NIFTY ") and name not in ["NIFTY 50", "NIFTY 500", "GIFT-NIFTY"] else name

def format_value(value, key, index_name):
    if value in ['-', None, '']: return '-'
    try:
        val = float(value)
        if key == '%': return f"{val:.2f}%"
        if key == 'Adv:Dec': return f"{val:.2f}"
        if index_name in ["INDIA VIX", "USD/INR", "IND 5Y", "IND 10Y", "IND 30Y"] and key in ['LTP', 'Chng', 'Prev.', 'Yr Hi', 'Yr Lo']: return f"{val:.2f}"
        if key in ['LTP', 'Chng', 'Prev.', 'Yr Hi', 'Yr Lo']: return str(int(val)) if val.is_integer() else str(val)
        return str(val)
    except: return '-'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json'
}

index_dict = {}

# TradingView data
for name, symbol in TV_SYMBOLS.items():
    try:
        data = requests.get(f"https://scanner.tradingview.com/symbol?symbol={symbol}&fields=close[1],change_abs,price_52_week_high,price_52_week_low,close,change&no_404=true", headers=headers, timeout=5).json()
        index_dict[name] = {
            'Index': format_index_name(name), 'LTP': data.get('close'), 'Chng': data.get('change_abs'),
            '%': data.get('change'), 'Prev.': data.get('close[1]'), 'Adv:Dec': '-',
            'Yr Hi': data.get('price_52_week_high'), 'Yr Lo': data.get('price_52_week_low')
        }
    except: pass

# NSE data - FIXED: Direct request works!
try:
    response = requests.get("https://www.nseindia.com/api/allIndices", headers=headers, timeout=10)
    if response.status_code == 200:
        nse_data = response.json()
        for item in nse_data.get('data', []):
            name = item.get('index')
            if name not in target_indices or name in TV_SYMBOLS: continue
            adv, dec = int(item.get('advances', 0)), int(item.get('declines', 0))
            adv_dec = f"{adv/dec:.2f}" if dec > 0 else ("Max" if adv > 0 else "-")
            index_dict[name] = {
                'Index': format_index_name(name), 'LTP': item.get('last'), 'Chng': item.get('variation'),
                '%': item.get('percentChange'), 'Prev.': item.get('previousClose'), 'Adv:Dec': adv_dec,
                'Yr Hi': item.get('yearHigh'), 'Yr Lo': item.get('yearLow')
            }
except: pass

# Prepare CSV
records = []
for idx in target_indices:
    if idx in index_dict:
        rec = {k: format_value(v, k, idx) if k != 'Index' else format_index_name(idx) for k, v in index_dict[idx].items()}
    else:
        rec = {'Index': format_index_name(idx), 'LTP': '-', 'Chng': '-', '%': '-', 'Prev.': '-', 'Adv:Dec': '-', 'Yr Hi': '-', 'Yr Lo': '-'}
    records.append(rec)

records.append({
    'Index': '', 'LTP': '', 'Chng': '', '%': '', 'Prev.': '', 
    'Adv:Dec': '', 'Yr Hi': 'Updated Time:', 'Yr Lo': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%b %H:%M')
})

os.makedirs('Data', exist_ok=True)
pd.DataFrame(records).to_csv('Data/nse_all_indices.csv', index=False)
print("CSV created successfully!")
