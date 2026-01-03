import pandas as pd
from tradingview_screener import Query
import os
from datetime import datetime

MARKETS = [
    ('DJ:DJI', 'Dow Jones'),
    ('SP:SPX', 'S&P 500'),
    ('NASDAQ:NDX', 'NASDAQ 100'),
    ('CBOE:VIX', 'VIX'),
    ('TVC:DXY', 'Dollar Index'),
    ('TVC:US10Y', 'US10Y'),
    ('INDEX:NKY', 'Nikkei 225'),
    ('STOXX50:SX5E', 'Euro Stoxx 50'),
    ('XETR:DAX', 'DAX'),
    ('FTSE:UKX', 'FTSE 100'),
    ('CRYPTOCAP:BTC', 'Bitcoin'),
    ('FX:USDINR', 'USD/INR'),
    ('FX:USDJPY', 'USD/JPY')
]

def main():
    print("📊 Fetching global market data...")
    
    try:
        # Get all symbols
        symbols = [s for s,_ in MARKETS]
        
        # Fetch data
        _, df = (Query()
                .set_symbols({'tickers': symbols})
                .select('close', 'change_abs', 'change', 'close[1]')
                .get_scanner_data())
        
        # Process results
        results = []
        for symbol, name in MARKETS:
            row = df[df['ticker'] == symbol]
            if not row.empty:
                r = row.iloc[0]
                results.append({
                    'timestamp': datetime.now().isoformat(),
                    'index': name,
                    'symbol': symbol,
                    'ltp': r.get('close'),
                    'change': r.get('change_abs'),
                    'change_percent': r.get('change'),
                    'previous_close': r.get('close[1]')
                })
        
        # Save to CSV
        os.makedirs('Data', exist_ok=True)
        result_df = pd.DataFrame(results)
        
        if os.path.exists('Data/GLOBAL.csv'):
            existing = pd.read_csv('Data/GLOBAL.csv')
            result_df = pd.concat([existing, result_df], ignore_index=True)
        
        result_df.to_csv('Data/GLOBAL.csv', index=False)
        print(f"✅ Saved {len(results)} records to Data/GLOBAL.csv")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
