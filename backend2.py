#!/usr/bin/env python3
import time
import os
import traceback
import threading

import paramiko
import pandas as pd
import requests
from flask import Flask, jsonify, request, send_from_directory

# Configuration
EC2_HOSTNAME    = "3.95.175.221"
EC2_USERNAME    = "ec2-user"
EC2_KEYFILE     = os.path.expanduser("~/Downloads/javi.pem")
EC2_CSV_PATH    = "/home/ec2-user/bob_usdt_prices_2.csv"
LOCAL_CSV       = "bob_usdt_prices_2_local.csv"
TEMP_CSV        = "bob_usdt_prices_2_temp.csv"

FETCH_INTERVAL  = 5
OUTLIER_THRESH  = 1.5
TF_MAP = {
    "1min":  "1min",
    "5min":  "5min",
    "15min": "15min",
    "30min": "30min",
    "1h":    "1h",
    "1d":    "1D",
}

# Globals
_df_cache       = None
_resample_cache = {}

app = Flask(__name__, static_folder='.')

def _fetch_csv():
    """Fetch the remote CSV via SFTP, load into a DataFrame, compute mid_price."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(EC2_HOSTNAME, username=EC2_USERNAME, key_filename=EC2_KEYFILE)
    sftp = ssh.open_sftp()
    sftp.get(EC2_CSV_PATH, TEMP_CSV)
    sftp.close()
    ssh.close()

    df = pd.read_csv(TEMP_CSV, parse_dates=["timestamp"])
    required = {"timestamp", "best_buy_price", "best_sell_price"}
    if not required.issubset(df.columns):
        raise ValueError("CSV missing required columns")

    df["best_buy_price"]  = pd.to_numeric(df["best_buy_price"],  errors="coerce")
    df["best_sell_price"] = pd.to_numeric(df["best_sell_price"], errors="coerce")
    df["mid_price"]       = (df["best_buy_price"] + df["best_sell_price"]) / 2
    df = df.sort_values("timestamp").set_index("timestamp")

    os.replace(TEMP_CSV, LOCAL_CSV)
    return df

def _background_fetcher():
    """Periodically refresh _df_cache in the background."""
    global _df_cache, _resample_cache
    while True:
        try:
            new_df = _fetch_csv()
            _df_cache = new_df
            _resample_cache.clear()
            print(f"Background refresh at {time.ctime()}")
        except Exception:
            traceback.print_exc()
            time.sleep(FETCH_INTERVAL * 2)
        time.sleep(FETCH_INTERVAL)

def _clean_series(ts: pd.Series) -> pd.Series:
    """Remove outliers via rolling window and forward-fill."""
    window = FETCH_INTERVAL * 12
    rol    = ts.rolling(window=window, min_periods=1)
    mean   = rol.mean()
    std    = rol.std().fillna(0)
    mask   = (ts - mean).abs() <= OUTLIER_THRESH * std
    return ts.where(mask).ffill()

def _get_ohlc(price_col: str, tf: str, timezone: str = 'America/La_Paz'):
    """
    Resample the clean series into OHLC buckets at the given timeframe,
    convert timestamps into UTC‐naive for display.
    """
    cache_key = (price_col, tf, timezone)
    if cache_key not in _resample_cache and _df_cache is not None:
        clean = _clean_series(_df_cache[price_col])
        rule  = TF_MAP.get(tf, "5min")
        ohlc  = clean.resample(rule).ohlc().dropna()

        if timezone:
            try:
                # localize UTC, convert to target zone, then back to naive UTC
                ohlc.index = (
                    ohlc.index
                        .tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
                        .tz_convert(timezone)
                        .tz_localize(None)
                        .tz_localize('UTC')
                )
            except TypeError:
                ohlc.index = (
                    ohlc.index
                        .tz_convert(timezone)
                        .tz_localize(None)
                        .tz_localize('UTC')
                )

        _resample_cache[cache_key] = [
            {
                "time":  int(ts.timestamp()),
                "open":  float(r.open),
                "high":  float(r.high),
                "low":   float(r.low),
                "close": float(r.close),
            }
            for ts, r in ohlc.iterrows()
        ]

    return _resample_cache.get(cache_key, [])

P2P_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"

def fetch_l2(side: str):
    """Fetch Binance P2P order‐book, aggregate by price, compute per‐maker USDT."""
    try:
        agg = {}
        for page in (1, 2, 3):
            payload = {
                "page": page, "rows": 20,
                "asset": "USDT",
                "tradeType": "SELL" if side == "bid" else "BUY",
                "fiat": "BOB", "payTypes": [], "publisherType": None,
                "sortBy": "price", "sortType": 1, "transAmount": "0",
                "isFuzzy": False, "isBuyOnline": True
            }
            data = requests.post(P2P_URL, json=payload, timeout=5).json().get("data", [])
            for o in data:
                adv = o.get("adv", {})
                ad  = o.get("advertiser", {})
                if (adv.get("advVisibleRet") is None and
                    ad.get("monthFinishRate", 0) > 0.8 and
                    ad.get("userType") == "merchant"):

                    price   = float(adv.get("price", 0))
                    size_bs = float(adv.get("dynamicMaxSingleTransAmount", 0))
                    maker   = ad.get("nickName", "")[:4].upper()

                    if price not in agg:
                        agg[price] = {"total_bs": 0.0, "total_usdt": 0.0, "makers": []}

                    agg[price]["total_bs"]  += size_bs
                    agg[price]["total_usdt"]+= size_bs / price
                    agg[price]["makers"].append({"maker": maker, "size": size_bs})

        rows = []
        for price, data in sorted(agg.items(), key=lambda x: x[0], reverse=(side == "bid")):
            rows.append({
                "price":      price,
                "size":       data["total_bs"],
                "maker":      "TOTAL",
                "total_bs":   data["total_bs"],
                "total_usdt": data["total_usdt"],
            })
            for m in data["makers"]:
                msize = m["size"]
                rows.append({
                    "price":       price,
                    "size":        msize,
                    "maker":       m["maker"],
                    "maker_usdt":  msize / price,
                    "total_bs":    None,
                    "total_usdt":  None,
                })

        return rows

    except Exception:
        traceback.print_exc()
        return []

# -------------------------------------------------------------------
# Ensure we have data before serving any requests
try:
    _df_cache = _fetch_csv()
except Exception:
    traceback.print_exc()
    _df_cache = pd.DataFrame(columns=["best_buy_price", "best_sell_price", "mid_price"])

# Start the background refresher
threading.Thread(target=_background_fetcher, daemon=True).start()

# -------------------------------------------------------------------
@app.route("/")
def index():
    return send_from_directory('.', 'index.html')

@app.route("/data/price.json")
def price_endpoint():
    try:
        tf = request.args.get("tf", "5min")
        tz = request.args.get("tz", "America/La_Paz")

        buy_series  = _get_ohlc("best_buy_price",   tf, tz)
        mid_series  = _get_ohlc("mid_price",        tf, tz)
        sell_series = _get_ohlc("best_sell_price", tf, tz)

        # raw last CSV row
        last = _df_cache.iloc[-1]
        raw = {
            "time": int(last.name.timestamp()),
            "buy":  float(last.best_buy_price),
            "mid":  float(last.mid_price),
            "sell": float(last.best_sell_price),
        }

        return jsonify({
            "buy":  buy_series,
            "mid":  mid_series,
            "sell": sell_series,
            "raw":  raw
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/data/<side>.json")
def orderbook(side):
    try:
        if side not in ["bid", "ask"]:
            return jsonify({"error": "Invalid side"}), 400
        return jsonify(fetch_l2(side))
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
