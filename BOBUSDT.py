import requests
import json
import time
import csv
from datetime import datetime

BINANCE_P2P_API_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"

def get_best_price(asset="USDT", fiat="BOB", trade_type="BUY"):
    # For BUY, we want ascending price sorting; for SELL, descending.
    sort_type = 1 if trade_type == "BUY" else 2
    
    payload = {
        "page": 1,
        "rows": 10,
        "asset": asset,
        "tradeType": trade_type,
        "fiat": fiat,
        "payTypes": [],
        "publisherType": None,
        "sortBy": "price",
        "sortType": sort_type,
        # optionally set 'transAmount': '1000' if you want the best price
        # at or above a certain trade amount
    }

    try:
        response = requests.post(BINANCE_P2P_API_URL, json=payload)
        data = response.json()
        ads = data.get("data", [])
        
        if not ads:
            return None
        
        # parse all the returned ads
        prices = [float(ad["adv"]["price"]) for ad in ads]
        
        if trade_type == "BUY":
            # best is the lowest price
            return max(prices)
        else:
            # best is the highest price
            return min(prices)
            
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def main():
    fieldnames = ["timestamp", "best_buy_price", "best_sell_price"]
    with open("bob_usdt_prices.csv", "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if csvfile.tell() == 0:
            writer.writeheader()

        while True:
            best_buy_price = get_best_price(trade_type="BUY")
            best_sell_price = get_best_price(trade_type="SELL")

            timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            row = {
                "timestamp": timestamp_str,
                "best_buy_price": best_buy_price,
                "best_sell_price": best_sell_price
            }

            writer.writerow(row)
            print(f"[{timestamp_str}] BUY: {best_buy_price}, SELL: {best_sell_price}")

            time.sleep(10)

if __name__ == "__main__":
    main()
