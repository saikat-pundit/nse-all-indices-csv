import requests
import csv
import os
from datetime import datetime

url = "https://oxide.sensibull.com/v1/compute/cache/fii_dii_daily"
response = requests.get(url)
data = response.json()

os.makedirs("Data", exist_ok=True)

csv_path = "Data/Cash.csv"
if os.path.exists(csv_path):
    os.remove(csv_path)

with open(csv_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Date", "FII Net Buy/Sell", "DII Net Buy/Sell"])
    
    sorted_dates = sorted(data["data"], reverse=True)
    
    for date_str in sorted_dates:
        day = data["data"][date_str]
        if "cash" in day:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            formatted_date = date_obj.strftime("%d %b %y")
            
            fii_val = int(day["cash"]["fii"]["buy_sell_difference"])
            dii_val = int(day["cash"]["dii"]["buy_sell_difference"])
            
            writer.writerow([formatted_date, f"{fii_val} Cr.", f"{dii_val} Cr."])
