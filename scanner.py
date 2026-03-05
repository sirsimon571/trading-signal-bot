import logging, os, time
from datetime import datetime
import pytz, requests
from db import init_db, save_signal
from strategies import prepare_df, scan_all_strategies

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

API_TOKEN = os.environ.get("ITICK_API_KEY", "")
BASE_URL = "https://api.itick.org"
NY_TZ = pytz.timezone("America/New_York")

WATCHLIST = {
    "US": [
        "AAPL", "TSLA", "NVDA", "MSFT", "AMZN",
        "META", "SPY",  "QQQ",  "AMD",  "GOOGL",
        "MARA", "SMCI", "COIN", "PLTR", "TQQQ", "BBIO", "AMD", "AVGO"
    ],
}

def get_market_data(symbol, region="US"):
    url = f"{BASE_URL}/stock/kline"
    params = {"region": region, "code": symbol, "kType": 1, "limit": 50}
    headers = {"token": API_TOKEN}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("data") or []
        return data if data else None
    except requests.RequestException as exc:
        log.error("API error for %s: %s", symbol, exc)
        return None

def run():
    log.info("Day Trading Signal Bot starting...")
    if not API_TOKEN:
        log.error("ITICK_API_KEY not set.")
        return
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
                        log.info("NEW: %s %s Entry:%s RR:%.1f", symbol, sig["type"], sig["entry"], sig.get("rr",0))
                time.sleep(0.5)
        log.info("Done: %d new signal(s). Sleeping 60s.", new_count)
        time.sleep(60)

if __name__ == "__main__":
    run()
