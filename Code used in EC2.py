import requests
import json
import time
import csv
from datetime import datetime

BINANCE_P2P_API_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"

def get_best_price(asset="USDT", fiat="BOB", trade_type="BUY"):
    prices = []

    for page in range(1, 4):  # Fetch first 3 pages
        payload = {
            "page": page,
            "rows": 20,
            "asset": asset,
            "tradeType": trade_type,
            "fiat": fiat,
            "payTypes": [],
            "publisherType": None,
            "sortBy": "price",
            "sortType": 1 if trade_type == "BUY" else 2,
            "transAmount": "0",
            "isFuzzy": False,
            "isBuyOnline": True
        }

        try:
            response = requests.post(BINANCE_P2P_API_URL, json=payload)
            data = response.json()
            ads = data.get("data", [])

            for order in ads:
                adv = order.get('adv', {})
                advertiser = order.get('advertiser', {})

                if adv.get('advVisibleRet') is None and adv.get('invisibleType') is None:
                    if advertiser.get('monthFinishRate', 0) > 0.8:
                        if advertiser.get('userType') == 'merchant':
                            prices.append(float(adv.get('price', 0)))
        except Exception as e:
            print(f"Error fetching data: {e}")

    if not prices:
        return None

    if trade_type == "BUY":
        return min(prices)
    else:
        return max(prices)

def main():
    fieldnames = ["timestamp", "best_buy_price", "best_sell_price"]
    with open("bob_usdt_prices_2.csv", "a", newline="", encoding="utf-8") as csvfile:
        
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
            print(best_buy_price,best_sell_price)
            writer.writerow(row)
            time.sleep(10)

if __name__ == "__main__":
    main()