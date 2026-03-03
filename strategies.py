"""
strategies.py — Signal detection: Fair Value Gap, Liquidity Sweep, ORB.

Key fixes vs original:
  - FVG: Added bearish direction, volume confirmation, minimum gap % filter,
         correct SL placement (outside gap, not inside it).
  - Liquidity Sweep: Added bearish direction, wick-ratio filter to avoid
                     false positives on small wicks.
  - ORB: Fully implemented with NY time, 9:30-9:45 range, volume spike
         requirement, signal expiry at 11:30.
  - All strategies: Proper R:R calculation, market-hours gate.
"""

import logging
from datetime import datetime, time as dtime

import pandas as pd
import pytz

log = logging.getLogger(__name__)
NY_TZ = pytz.timezone("America/New_York")


# ─────────────────────────── helpers ────────────────────────────────────────

def _now_ny() -> datetime:
    return datetime.now(NY_TZ)


def is_market_hours() -> bool:
    """True during regular US session 9:30–16:00 ET, Mon–Fri."""
    now = _now_ny()
    return (
        now.weekday() < 5
        and dtime(9, 30) <= now.time() <= dtime(16, 0)
    )


def prepare_df(raw_data: list[dict]) -> pd.DataFrame:
    """
    Normalise an iTick kline payload into a clean DataFrame.
    iTick columns: t (ms timestamp), o, h, l, c, v
    """
    df = pd.DataFrame(raw_data)
    df = df.rename(columns={
        "t": "timestamp",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
    })
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["timestamp"] = (
        pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        .dt.tz_convert(NY_TZ)
    )
    df = df.dropna(subset=["open", "high", "low", "close"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def _rr(entry: float, sl: float, tp: float, direction: str) -> float:
    """Risk-Reward ratio, always positive. Returns 0 on invalid geometry."""
    try:
        if direction == "LONG":
            risk   = entry - sl
            reward = tp - entry
        else:
            risk   = sl - entry
            reward = entry - tp
        return round(reward / risk, 2) if risk > 0 else 0.0
    except Exception:
        return 0.0


# ─────────────────────────── FVG ────────────────────────────────────────────

def detect_fvg(df: pd.DataFrame) -> list[dict]:
    """
    Fair Value Gap (3-candle imbalance pattern).

    Bullish FVG:  C3.low  > C1.high  → price gapped UP, gap may fill later.
                  Signal: wait for retest back into the gap.
    Bearish FVG:  C3.high < C1.low   → price gapped DOWN.
                  Signal: wait for retest up into the gap.

    Filters applied:
      • Gap must be ≥ 0.1% of price  (eliminates micro-noise)
      • C2 volume must be ≥ 1.3× recent avg (confirms institutional move)
    """
    if len(df) < 22:
        return []

    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    avg_vol = df.iloc[-22:-3]["volume"].mean()
    vol_ok  = float(c2["volume"]) > avg_vol * 1.3
    signals = []

    # — Bullish FVG —
    gap_lo = float(c1["high"])
    gap_hi = float(c3["low"])
    if gap_hi > gap_lo and vol_ok:
        gap_size = gap_hi - gap_lo
        if gap_size / gap_lo >= 0.001:                # ≥ 0.1% gap
            entry = gap_lo + gap_size * 0.5           # Mid of gap = fair retest entry
            sl    = gap_lo - gap_size * 0.15          # Just below gap
            tp    = entry + (entry - sl) * 2.5        # 2.5 R minimum
            signals.append({
                "type":        "BULLISH_FVG",
                "instruction": (
                    f"Bullish imbalance: gap {gap_lo:.2f}–{gap_hi:.2f}. "
                    f"WAIT for price to pull back into the gap zone, then BUY "
                    f"on a bullish confirmation candle. "
                    f"Entry ~{entry:.2f} | SL {sl:.2f} | TP {tp:.2f}"
                ),
                "entry": round(entry, 4),
                "sl":    round(sl, 4),
                "tp":    round(tp, 4),
                "rr":    _rr(entry, sl, tp, "LONG"),
            })

    # — Bearish FVG —
    gap_hi2 = float(c1["low"])
    gap_lo2 = float(c3["high"])
    if gap_lo2 < gap_hi2 and vol_ok:
        gap_size = gap_hi2 - gap_lo2
        if gap_size / gap_hi2 >= 0.001:
            entry = gap_hi2 - gap_size * 0.5
            sl    = gap_hi2 + gap_size * 0.15
            tp    = entry - (sl - entry) * 2.5
            signals.append({
                "type":        "BEARISH_FVG",
                "instruction": (
                    f"Bearish imbalance: gap {gap_lo2:.2f}–{gap_hi2:.2f}. "
                    f"WAIT for price to retrace up into the gap, then SELL SHORT "
                    f"on a bearish confirmation candle. "
                    f"Entry ~{entry:.2f} | SL {sl:.2f} | TP {tp:.2f}"
                ),
                "entry": round(entry, 4),
                "sl":    round(sl, 4),
                "tp":    round(tp, 4),
                "rr":    _rr(entry, sl, tp, "SHORT"),
            })

    return signals


# ─────────────────────── Liquidity Sweep ────────────────────────────────────

def detect_liquidity_sweep(df: pd.DataFrame, lookback: int = 20) -> list[dict]:
    """
    Liquidity Sweep / Stop Hunt.

    Smart money raids obvious support/resistance levels to grab liquidity,
    then reverses hard. Identified by:
      • Candle wick that breaks a recent extreme (20-bar high/low)
      • Price closes BACK on the other side of that level
      • Wick accounts for ≥ 35% of the total candle range

    Bullish sweep: wick below 20-bar low, closes back above it  → BUY
    Bearish sweep: wick above 20-bar high, closes back below it → SHORT
    """
    if len(df) < lookback + 2:
        return []

    c     = df.iloc[-1]
    prev  = df.iloc[-lookback:-1]
    close = float(c["close"])
    high  = float(c["high"])
    low   = float(c["low"])
    span  = high - low
    if span == 0:
        return []

    recent_low  = float(prev["low"].min())
    recent_high = float(prev["high"].max())
    signals     = []

    # — Bullish sweep —
    if low < recent_low and close > recent_low:
        wick_below = recent_low - low
        if wick_below / span >= 0.35:
            sl = round(low * 0.9990, 4)        # 0.1% below the wick low
            tp = round(close + (close - sl) * 2.5, 4)
            signals.append({
                "type":        "BULLISH_LIQUIDITY_SWEEP",
                "instruction": (
                    f"Stop hunt below {recent_low:.2f} — price swept lows then reversed. "
                    f"Smart money loaded longs. BUY NOW at market. "
                    f"Entry {close:.2f} | SL {sl} (below wick) | TP {tp}"
                ),
                "entry": round(close, 4),
                "sl":    sl,
                "tp":    tp,
                "rr":    _rr(close, sl, tp, "LONG"),
            })

    # — Bearish sweep —
    if high > recent_high and close < recent_high:
        wick_above = high - recent_high
        if wick_above / span >= 0.35:
            sl = round(high * 1.0010, 4)
            tp = round(close - (sl - close) * 2.5, 4)
            signals.append({
                "type":        "BEARISH_LIQUIDITY_SWEEP",
                "instruction": (
                    f"Stop hunt above {recent_high:.2f} — price swept highs then reversed. "
                    f"Smart money distributed. SELL SHORT NOW at market. "
                    f"Entry {close:.2f} | SL {sl} (above wick) | TP {tp}"
                ),
                "entry": round(close, 4),
                "sl":    sl,
                "tp":    tp,
                "rr":    _rr(close, sl, tp, "SHORT"),
            })

    return signals


# ─────────────────────── Opening Range Breakout ──────────────────────────────

def detect_orb(df: pd.DataFrame) -> list[dict]:
    """
    Opening Range Breakout (ORB).

    Opening range = first 15 min of regular session: 9:30–9:44 ET.
    Signal window = 9:45–11:30 ET (classic ORB window).

    A valid breakout requires:
      • Current candle closes clearly beyond the range
      • Volume on the breakout candle ≥ 1.5× 30-bar average
      • Previous candle was still inside the range (first-touch breakout)
    """
    now = _now_ny()
    if not (dtime(9, 45) <= now.time() <= dtime(11, 30)):
        return []

    orb_candles = df[
        (df["timestamp"].dt.hour == 9) &
        (df["timestamp"].dt.minute >= 30) &
        (df["timestamp"].dt.minute < 45)
    ]
    if len(orb_candles) < 3:
        return []

    orb_high  = float(orb_candles["high"].max())
    orb_low   = float(orb_candles["low"].min())
    orb_range = orb_high - orb_low
    if orb_range <= 0:
        return []

    if len(df) < 4:
        return []

    c_now  = df.iloc[-1]
    c_prev = df.iloc[-2]
    price  = float(c_now["close"])

    avg_vol   = df.iloc[-32:-1]["volume"].mean()
    vol_spike = float(c_now["volume"]) > avg_vol * 1.5

    signals = []

    # — Bullish ORB —
    if (
        price > orb_high
        and float(c_prev["close"]) <= orb_high
        and vol_spike
    ):
        sl = round(orb_high - orb_range * 0.2, 4)   # Re-enter range = invalidated
        tp = round(price + orb_range * 2.0,   4)     # 2× range extension target
        signals.append({
            "type":        "BULLISH_ORB",
            "instruction": (
                f"First-touch breakout above ORB high ({orb_high:.2f}) on strong volume. "
                f"BUY at market. Target 2× ORB range extension. "
                f"Entry {price:.2f} | SL {sl} | TP {tp}"
            ),
            "entry": round(price, 4),
            "sl":    sl,
            "tp":    tp,
            "rr":    _rr(price, sl, tp, "LONG"),
        })

    # — Bearish ORB —
    if (
        price < orb_low
        and float(c_prev["close"]) >= orb_low
        and vol_spike
    ):
        sl = round(orb_low + orb_range * 0.2, 4)
        tp = round(price - orb_range * 2.0,   4)
        signals.append({
            "type":        "BEARISH_ORB",
            "instruction": (
                f"First-touch breakdown below ORB low ({orb_low:.2f}) on strong volume. "
                f"SELL SHORT at market. Target 2× ORB range extension. "
                f"Entry {price:.2f} | SL {sl} | TP {tp}"
            ),
            "entry": round(price, 4),
            "sl":    sl,
            "tp":    tp,
            "rr":    _rr(price, sl, tp, "SHORT"),
        })

    return signals


# ─────────────────────── master scanner ─────────────────────────────────────

def scan_all_strategies(df: pd.DataFrame, symbol: str) -> list[dict]:
    """Run every strategy and return all valid signals."""
    if not is_market_hours():
        return []
    if len(df) < 25:
        log.warning("Insufficient data for %s (%d bars)", symbol, len(df))
        return []

    signals = []
    signals.extend(detect_fvg(df))
    signals.extend(detect_liquidity_sweep(df))
    signals.extend(detect_orb(df))

    # Drop any signals with R:R < 1.5 — not worth taking
    signals = [s for s in signals if s.get("rr", 0) >= 1.5]

    return signals
