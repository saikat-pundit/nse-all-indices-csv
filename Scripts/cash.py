import requests
import csv
import os

url = "https://oxide.sensibull.com/v1/compute/cache/fii_dii_daily"
response = requests.get(url)
data = response.json()

os.makedirs("Data", exist_ok=True)

with open("Data/Cash.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Date", "FII Net Buy/Sell", "DII Net Buy/Sell"])
    
    for date_str in sorted(data["data"]):
        day = data["data"][date_str]
        if "cash" in day:
            fii_val = int(day["cash"]["fii"]["buy_sell_difference"])
            dii_val = int(day["cash"]["dii"]["buy_sell_difference"])
            writer.writerow([date_str, f"{fii_val} Cr.", f"{dii_val} Cr."])
