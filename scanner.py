import logging
import os
import time
from datetime import datetime

import pytz
import yfinance as yf

from db import init_db, save_signal
from strategies import prepare_df, scan_all_strategies

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

NY_TZ = pytz.timezone("America/New_York")

WATCHLIST = {
    "US": ["AAPL","TSLA","NVDA","MSFT","AMZN","META","SPY","QQQ","AMD","GOOGL",
           "MARA","SMCI","COIN","PLTR","TQQQ","BBIO"],
}

SCAN_INTERVAL = 60
API_DELAY = 1.0


def get_market_data(symbol, region="US"):
    try:
        df = yf.download(symbol, period="1d", interval="1m", progress=False, auto_adjust=True)
        if df is None or df.empty:
            return None
        df = df.reset_index()
        df = df.rename(columns={
            "Datetime": "t",
            "Open":     "o",
            "High":     "h",
            "Low":      "l",
            "Close":    "c",
            "Volume":   "v",
        })
        df["t"] = df["t"].astype("int64") // 10**6
        return df.to_dict("records")
    except Exception as exc:
        log.error("yFinance error for %s: %s", symbol, exc)
        return None


def run():
    log.info("Day Trading Signal Bot starting...")
    init_db()
    while True:
        now_ny = datetime.now(NY_TZ)
        if now_ny.weekday() >= 5 or not (4 <= now_ny.hour < 20):
            log.info("Off-hours. Sleeping 5 min.")
            time.sleep(300)
            continue
        log.info("Scanning at %s ET", now_ny.strftime("%H:%M:%S"))
        new_count = 0
        for region, symbols in WATCHLIST.items():
            for symbol in symbols:
                raw = get_market_data(symbol, region)
                if raw is None:
                    continue
                df = prepare_df(raw)
                signals = scan_all_strategies(df, symbol)
                for sig in signals:
                    if save_signal(symbol, sig):
                        new_count += 1
                        log.info("NEW: %s %s Entry:%s RR:%.1f", symbol, sig["type"], sig["entry"], sig.get("rr", 0))
                time.sleep(API_DELAY)
        log.info("Done: %d new signal(s). Sleeping 60s.", new_count)
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run()
