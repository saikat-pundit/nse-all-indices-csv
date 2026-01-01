import requests
import csv
from datetime import datetime, timedelta
import os

def fetch_bse_data():
    urls = [
        "https://api.bseindia.com/BseIndiaAPI/api/MktCapBoard_indstream/w?cat=1&type=2",
        "https://api.bseindia.com/BseIndiaAPI/api/MktCapBoard_indstream/w?cat=2&type=2",
        "https://api.bseindia.com/BseIndiaAPI/api/MktCapBoard_indstream/w?cat=3&type=2"
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0",
        "Referer": "https://www.bseindia.com/"
    }
    
    all_data = []
    for url in urls:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                # Handle RealTime data
                if "RealTime" in data:
                    for item in data["RealTime"]:
                        all_data.append({
                            "IndexName": item.get("IndexName", "").strip(),
                            "Curvalue": item.get("Curvalue", 0),
                            "Chg": item.get("Chg", 0),
                            "ChgPer": item.get("ChgPer", 0),
                            "Prev_Close": item.get("Prev_Close", 0),
                            "Week52High": item.get("Week52High", 0),
                            "Week52Low": item.get("Week52Low", 0)
                        })
                
                # Handle EOD data
                if "EOD" in data:
                    for item in data["EOD"]:
                        all_data.append({
                            "IndexName": item.get("IndicesWatchName", "").strip(),
                            "Curvalue": item.get("Curvalue", 0),
                            "Chg": item.get("CHNG", 0),
                            "ChgPer": item.get("CHNGPER", 0),
                            "Prev_Close": item.get("PrevDayClose", 0),
                            "Week52High": "-",
                            "Week52Low": "-"
                        })
                        
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            continue
    
    return all_data

def transform_data(original_data):
    if not original_data:
        return []
    
    transformed = []
    for item in original_data:
        week52high = item.get("Week52High", "-")
        week52low = item.get("Week52Low", "-")
        
        if week52high != "-" and week52high != "":
            try:
                week52high = f'{float(week52high):.2f}'
            except:
                week52high = "-"
        
        if week52low != "-" and week52low != "":
            try:
                week52low = f'{float(week52low):.2f}'
            except:
                week52low = "-"
        
        try:
            row = [
                item.get("IndexName", "-"),
                f'{float(item.get("Curvalue", 0)):.2f}',
                f'{float(item.get("Chg", 0)):.2f}',
                f'{float(item.get("ChgPer", 0)):.2f}',
                f'{float(item.get("Prev_Close", 0)):.2f}',
                week52high,
                week52low
            ]
            transformed.append(row)
        except:
            continue
    
    return transformed

def save_to_csv(data, filename="Data/BSE.csv"):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    csv_headers = ["Index", "LTP", "CHNG", "%", "PREV.", "YR HI", "YR LO"]
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)
        writer.writerows(data)
        
        timestamp = (datetime.now() + timedelta(hours=5, minutes=30)).strftime("%d-%b %H:%M")
        writer.writerow(["", "", "", "", "", "Update Time", timestamp])

if __name__ == "__main__":
    raw_data = fetch_bse_data()
    print(f"Total records fetched: {len(raw_data)}")
    processed_data = transform_data(raw_data)
    save_to_csv(processed_data)
    print(f"CSV saved with {len(processed_data)} records")
