"""
core/strategy.py
Inside Bar Breakout — v2 Specification

Changes vs v1:
  - RR target: 1:2 → 1:1
  - Mother range max: 0.5% → 0.40% of entry
  - Force-close: 15:10 → 15:15
  - Daily SL: 2 hits → 2 CONSECUTIVE SLs
  - Position concurrency: unlimited → ONE trade at a time
  - NEW: Regime filter — skip day if 10-day avg daily range > 1.5%
  - REMOVED: EMA20 trend filter
"""

import pandas as pd
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, time
import pytz

IST = pytz.timezone("Asia/Kolkata")

# ─── v2 constants ──────────────────────────────────────────────────────────────

# Time rules
NO_ENTRY_AFTER  = time(15, 0)
FORCE_EXIT_AT   = time(15, 15)   # v2: extended from 15:10
MARKET_OPEN     = time(9, 15)
MARKET_CLOSE    = time(15, 15)

# Trade management — v2
RR_TARGET_MULTIPLE     = 1.0     # v2: 1:1 RR
MIN_MOTHER_RANGE_PTS   = 100     # min mother range in pts
MAX_MOTHER_RANGE_PCT   = 0.40    # max mother range as % of entry
CONSEC_SL_LIMIT        = 2       # stop day after N consecutive SLs
BREAKOUT_WINDOW_MIN    = 30      # 30 min window after baby close

# Regime filter — v2 headline addition
REGIME_FILTER_ENABLED  = True
REGIME_LOOKBACK_DAYS   = 10
REGIME_MAX_AVG_RANGE   = 1.50    # max avg daily range as % of price


# ─── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class Setup:
    index: str
    mother_high: float
    mother_low: float
    baby_close_time: datetime
    range_pts: float = field(init=False)

    def __post_init__(self):
        self.range_pts = round(self.mother_high - self.mother_low, 2)


@dataclass
class Signal:
    setup: Setup
    direction: str                 # "LONG" | "SHORT"
    confirmed_at: datetime
    entry_price: Optional[float] = None


@dataclass
class Trade:
    signal: Signal
    index: str
    entry_price: float
    sl: float
    target: float
    risk: float
    option_symbol: str
    option_order_id: Optional[str] = None
    option_entry_price: Optional[float] = None
    status: str = "OPEN"
    pnl: float = 0.0


# ─── Regime filter ─────────────────────────────────────────────────────────────

def compute_daily_ranges(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute daily range_pct from a daily OHLC dataframe.
    Expects columns: date, open, high, low, close
    """
    if daily_df is None or daily_df.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "range_pct"])
    out = daily_df.copy()
    if "range_pct" not in out.columns:
        out["range_pct"] = (out["high"] - out["low"]) / out["open"] * 100
    return out


def regime_allowed(daily_df: pd.DataFrame, target_date) -> tuple[bool, Optional[float]]:
    """
    Check if regime allows trading on target_date.
    Uses the 10 trading days strictly BEFORE target_date.
    Returns (allowed, avg_range_pct_or_None)
    """
    if not REGIME_FILTER_ENABLED:
        return True, None

    daily = compute_daily_ranges(daily_df)
    if daily.empty:
        return True, None

    if hasattr(target_date, 'date') and len(daily) > 0:
        try:
            sample = daily['date'].iloc[0]
            if not isinstance(target_date, type(sample)):
                target_date = target_date.date()
        except Exception:
            pass

    prior = daily[daily["date"] < target_date].tail(REGIME_LOOKBACK_DAYS)
    if len(prior) < REGIME_LOOKBACK_DAYS:
        return True, None

    avg_range = float(prior["range_pct"].mean())
    return (avg_range <= REGIME_MAX_AVG_RANGE), avg_range


def regime_diagnostics(daily_df: pd.DataFrame, target_date) -> dict:
    """
    Return diagnostic info about the regime gate for UI display.
    """
    daily = compute_daily_ranges(daily_df)
    if daily.empty:
        return {
            "status": "no_data",
            "available_days": 0,
            "prior_days": [],
            "avg": None,
            "needed": REGIME_LOOKBACK_DAYS,
            "threshold": REGIME_MAX_AVG_RANGE,
        }

    if hasattr(target_date, 'date'):
        try:
            target_date = target_date.date()
        except Exception:
            pass

    prior = daily[daily["date"] < target_date].tail(REGIME_LOOKBACK_DAYS)
    avg = float(prior["range_pct"].mean()) if len(prior) > 0 else None

    if avg is None or len(prior) < REGIME_LOOKBACK_DAYS:
        status = "insufficient"
    elif avg <= REGIME_MAX_AVG_RANGE:
        status = "ok"
    else:
        status = "blocked"

    return {
        "status":         status,
        "target_date":    target_date,
        "available_days": int(len(daily)),
        "prior_count":    int(len(prior)),
        "needed":         REGIME_LOOKBACK_DAYS,
        "prior_days":     [(d.isoformat() if hasattr(d, 'isoformat') else str(d),
                            round(r, 3))
                           for d, r in zip(prior["date"], prior["range_pct"])],
        "avg":            round(avg, 3) if avg is not None else None,
        "threshold":      REGIME_MAX_AVG_RANGE,
    }


# ─── Setup detection ───────────────────────────────────────────────────────────

def detect_setups(candles_15m: pd.DataFrame, index: str) -> list[Setup]:
    df = candles_15m.copy().reset_index(drop=True)
    setups: list[Setup] = []

    if len(df) < 2:
        return setups

    for i in range(1, len(df)):
        mother = df.iloc[i - 1]
        baby   = df.iloc[i]

        m_time = _to_ist(mother["datetime"])
        b_time = _to_ist(baby["datetime"])

        if m_time.time() == MARKET_OPEN or b_time.time() == MARKET_OPEN:
            continue
        if b_time.time() >= MARKET_CLOSE:
            continue

        if not (baby["high"] < mother["high"] and baby["low"] > mother["low"]):
            continue

        rng = mother["high"] - mother["low"]
        if rng < MIN_MOTHER_RANGE_PTS:
            continue

        setups.append(Setup(
            index=index,
            mother_high=round(mother["high"], 2),
            mother_low=round(mother["low"], 2),
            baby_close_time=b_time,
        ))

    setups = _dedup_setups(setups)
    return setups


def _dedup_setups(setups: list[Setup]) -> list[Setup]:
    seen: dict[datetime, Setup] = {}
    for s in setups:
        key = s.baby_close_time
        if key not in seen or s.range_pts > seen[key].range_pts:
            seen[key] = s
    return list(seen.values())


# ─── Signal confirmation — v2: NO EMA filter ───────────────────────────────────

def check_breakout(
    candle_1m: dict,
    setup: Setup,
    already_signalled: bool,
) -> Optional[Signal]:
    """
    Returns Signal if the 1-min candle confirms a breakout, else None.
    v2: NO EMA20 filter.
    """
    if already_signalled:
        return None

    now = _to_ist(candle_1m["datetime"])
    if now.time() >= NO_ENTRY_AFTER:
        return None

    elapsed = (now - setup.baby_close_time).total_seconds() / 60
    if elapsed > BREAKOUT_WINDOW_MIN:
        return None

    close = candle_1m["close"]
    broke_up   = close > setup.mother_high
    broke_down = close < setup.mother_low

    if broke_up and broke_down:
        return None

    if broke_up:
        direction = "LONG"
    elif broke_down:
        direction = "SHORT"
    else:
        return None

    return Signal(setup=setup, direction=direction, confirmed_at=now)


# ─── Risk sizing — v2: 1:1 RR + 0.40% max range ────────────────────────────────

def build_trade_params(signal: Signal, entry_price: float):
    """
    Validate v2 max-range filter and return (sl, target, risk, entry).
    Returns (None, None, None, None) if max-range filter rejects.
    """
    setup = signal.setup
    if signal.direction == "LONG":
        sl     = setup.mother_low
        risk   = entry_price - sl
        target = entry_price + RR_TARGET_MULTIPLE * risk
    else:
        sl     = setup.mother_high
        risk   = sl - entry_price
        target = entry_price - RR_TARGET_MULTIPLE * risk

    # v2: max mother range as % of entry (0.40%)
    if risk > entry_price * (MAX_MOTHER_RANGE_PCT / 100.0):
        return None, None, None, None

    return round(sl, 2), round(target, 2), round(risk, 2), entry_price


# ─── Exit logic — v2: force exit at 15:15 ──────────────────────────────────────

def check_exit(trade: Trade, candle_1m: dict) -> Optional[str]:
    now  = _to_ist(candle_1m["datetime"])
    high = candle_1m["high"]
    low  = candle_1m["low"]

    if now.time() >= FORCE_EXIT_AT:
        return "TIME"

    if trade.signal.direction == "LONG":
        if low  <= trade.sl:     return "SL"
        if high >= trade.target: return "TARGET"
    else:
        if high >= trade.sl:     return "SL"
        if low  <= trade.target: return "TARGET"

    return None


# ─── ATM strike calculation ────────────────────────────────────────────────────

INDEX_STEP = {
    "NIFTY":     50,
    "BANKNIFTY": 100,
}


def atm_strike(spot: float, index: str, offset: int = 0) -> int:
    step = INDEX_STEP.get(index, 50)
    base = round(spot / step) * step
    raw  = base + offset
    return int(round(raw / step) * step)


# ─── Helpers ───────────────────────────────────────────────────────────────────

def _to_ist(dt) -> datetime:
    if isinstance(dt, str):
        dt = pd.to_datetime(dt)
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        return dt.astimezone(IST)
    return IST.localize(dt)
