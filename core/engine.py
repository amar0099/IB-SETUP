"""
core/engine.py — v2 spec, event-driven

State machine:
  IDLE      → wake every 15-min candle close, check last 2 candles for inside bar
              if found → WATCHING; else stay IDLE
  WATCHING  → poll 1-min candles, look for breakout within 30 min after baby close
              if breakout → enter trade → ACTIVE; if window expires → IDLE
  ACTIVE    → poll 1-min candles, monitor SL / target / time exit
              on exit → IDLE

This avoids polling 1-min data when no setup exists.
"""

import threading
from pathlib import Path
import time as time_mod
from datetime import datetime, time, timedelta
from typing import Optional
import pandas as pd
import pytz

from .strategy import (
    detect_setups, check_breakout, build_trade_params, check_exit,
    atm_strike,
    regime_allowed, regime_diagnostics,
    Setup, Signal, Trade,
    NO_ENTRY_AFTER, FORCE_EXIT_AT,
    BREAKOUT_WINDOW_MIN, CONSEC_SL_LIMIT, REGIME_FILTER_ENABLED,
)
from .broker     import ZerodhaClient, LOT_SIZE, EXCHANGE
from .fyers_feed import FyersFeed

IST           = pytz.timezone("Asia/Kolkata")
MARKET_OPEN   = time(0, 0)        # broad — strategy module enforces real time rules
MARKET_CLOSE  = time(23, 59)

POLL_IDLE     = 30                # seconds between IDLE checks
POLL_WATCH    = 5                 # seconds between WATCHING / ACTIVE polls

# Engine states
IDLE     = "IDLE"
WATCHING = "WATCHING"
ACTIVE   = "ACTIVE"


class AlgoEngine:
    """
    Event-driven inside-bar engine.
    Data: FyersFeed   Orders: ZerodhaClient
    """

    def __init__(self, fyers: FyersFeed, broker: ZerodhaClient):
        self.fyers  = fyers
        self.broker = broker

        self._thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()

        # configurable
        self.index      = "BANKNIFTY"   # v2: locked — strategy is BankNifty-only
        self.expiry     = None          # auto-picked at entry; user choice ignored
        self.lots       = 1
        self.pe_offset  = 0
        self.ce_offset  = 0
        self.paper_mode = True          # v2: paper-only by default

        # runtime state
        self.state:           str             = IDLE
        self.active_setup:    Optional[Setup] = None
        self.active_trade:    Optional[Trade] = None
        self.consec_sl_today: int             = 0
        self.day_stopped:     bool            = False
        self._last_signal_id: Optional[str]   = None
        self._last_day_reset: Optional[int]   = None

        # 15-min checkpoint tracking (avoid re-checking same closed candle)
        self._last_15m_check: Optional[datetime] = None

        # Regime state
        self._regime_ok:    Optional[bool]  = None
        self._regime_avg:   Optional[float] = None
        self._regime_diag:  dict            = {}
        self._daily_df:     Optional[pd.DataFrame] = None
        self._regime_date:  Optional[int]   = None

        # Paper trade journal (one record per closed trade)
        self.paper_trades: list[dict] = []

        self.log: list[dict] = []
        self._lock = threading.Lock()

    # ── Public controls ──────────────────────────────────────────

    def start(self):
        if self._thread and self._thread.is_alive():
            return

        try:
            self.fyers.set_log_callback(self._log)
            self.fyers.start_feed([self.index])
            self._log("INFO", f"Fyers feed started for {self.index}")
        except Exception as e:
            self._log("ERROR", f"Fyers feed failed to start: {e}")

        self._stop_flag.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="AlgoEngine"
        )
        self._thread.start()
        self._log("INFO",
            f"Algo started (v2 event-driven) | {self.index} | state: {self.state}")

    def stop(self):
        self._stop_flag.set()
        self._log("INFO", "Algo stopped by user.")

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ── Main loop ────────────────────────────────────────────────

    def _loop(self):
        while not self._stop_flag.is_set():
            try:
                now = datetime.now(IST)
                doy = now.timetuple().tm_yday
                if self._last_day_reset != doy:
                    self._daily_reset(now)

                if not (MARKET_OPEN <= now.time() <= MARKET_CLOSE):
                    time_mod.sleep(30)
                    continue

                if not self.fyers.connected:
                    self._log("INFO", "Waiting for Fyers feed…")
                    time_mod.sleep(5)
                    continue

                # Day-stopped → no work
                if self.day_stopped:
                    time_mod.sleep(POLL_IDLE)
                    continue

                # Regime gate (refreshed once per day)
                self._refresh_regime(now)
                if REGIME_FILTER_ENABLED and self._regime_ok is False:
                    time_mod.sleep(POLL_IDLE)
                    continue

                # State machine
                if self.state == IDLE:
                    self.fyers.set_poll_interval(60)   # slow feed in IDLE
                    self._tick_idle(now)
                    sleep_secs = self._secs_to_next_15m_boundary(now) + 5
                    time_mod.sleep(min(sleep_secs, 900))   # cap at 15 min

                elif self.state == WATCHING:
                    self.fyers.set_poll_interval(2)    # fast feed when watching
                    self._tick_watching(now)
                    time_mod.sleep(POLL_WATCH)

                elif self.state == ACTIVE:
                    self.fyers.set_poll_interval(2)    # fast feed during trade
                    self._tick_active(now)
                    time_mod.sleep(POLL_WATCH)

            except Exception as e:
                self._log("ERROR", f"Loop error: {e}")
                time_mod.sleep(POLL_IDLE)

    # ── IDLE tick: check for inside bar on completed 15-min candles ──────

    def _tick_idle(self, now: datetime):
        """
        On each 15-min boundary, fetch true 15-min OHLC from Fyers history API
        and check if last 2 candles form an inside bar.
        """
        try:
            df_15m = self.fyers.get_history_15min(self.index, days_back=3)
        except Exception as e:
            self._log("ERROR", f"15-min history fetch failed: {e}")
            return

        if df_15m.empty or len(df_15m) < 2:
            return

        # Identify the most recent CLOSED 15-min candle
        last_closed_dt = self._to_ist(df_15m.iloc[-1]["datetime"])

        # If we already inspected this candle, skip
        if (self._last_15m_check is not None and
                self._last_15m_check >= last_closed_dt):
            return

        self._last_15m_check = last_closed_dt

        # Run setup detection on full 15-min frame
        setups = detect_setups(df_15m, self.index)
        if not setups:
            self._log("INFO",
                f"15-min check at {last_closed_dt.strftime('%H:%M')} — no inside bar.")
            return

        candidate = setups[-1]
        # Only act if this baby is the latest closed candle (fresh setup)
        if candidate.baby_close_time != last_closed_dt:
            return

        self.active_setup    = candidate
        self._last_signal_id = None
        self.state           = WATCHING
        self._log("SETUP", (
            f"Inside bar formed | mother {candidate.mother_low}–"
            f"{candidate.mother_high} ({candidate.range_pts} pts) | "
            f"baby closed {candidate.baby_close_time.strftime('%H:%M')} | "
            f"now WATCHING for breakout (next {BREAKOUT_WINDOW_MIN} min)"
        ))

    # ── WATCHING tick: poll 1-min, look for breakout ──────────────────────

    def _tick_watching(self, now: datetime):
        if self.active_setup is None:
            self.state = IDLE
            return

        # Window expiry: BREAKOUT_WINDOW_MIN after baby close
        elapsed_min = (now - self.active_setup.baby_close_time).total_seconds() / 60
        if elapsed_min > BREAKOUT_WINDOW_MIN:
            self._log("INFO",
                f"Breakout window expired ({BREAKOUT_WINDOW_MIN} min) — back to IDLE.")
            self._reset_setup()
            return

        if now.time() >= NO_ENTRY_AFTER:
            self._log("INFO", "Past no-entry cutoff — back to IDLE.")
            self._reset_setup()
            return

        # Pull latest 1-min candle (incl. partial)
        df_1m = self.fyers.get_candles(self.index, 1, include_partial=True)
        if df_1m.empty:
            return
        latest_1m = df_1m.iloc[-1].to_dict()

        already = self._last_signal_id == _setup_id(self.active_setup)
        signal  = check_breakout(latest_1m, self.active_setup, already)
        if signal is None:
            return

        self._last_signal_id = _setup_id(self.active_setup)
        self._log("SIGNAL", (
            f"{signal.direction} breakout | close {round(latest_1m['close'],2)} | "
            f"mother {self.active_setup.mother_low}–{self.active_setup.mother_high}"
        ))
        self._enter_trade(signal, latest_1m)

    # ── ACTIVE tick: monitor open trade ───────────────────────────────────

    def _tick_active(self, now: datetime):
        if self.active_trade is None:
            self.state = IDLE
            return

        df_1m = self.fyers.get_candles(self.index, 1, include_partial=True)
        if df_1m.empty:
            return
        latest_1m = df_1m.iloc[-1].to_dict()

        reason = check_exit(self.active_trade, latest_1m)
        if reason:
            self._close_trade(reason)

    # ── Regime refresh (daily) ───────────────────────────────────

    def _refresh_regime(self, now: datetime):
        doy = now.timetuple().tm_yday

        # Already computed today
        if self._regime_date == doy and self._regime_diag:
            return

        # Try disk cache first (survives script reruns)
        cache_path = Path(".tokens") / f"regime_{self.index}_{now.date().isoformat()}.parquet"
        if self._daily_df is None and cache_path.exists():
            try:
                self._daily_df = pd.read_parquet(cache_path)
                self._log("INFO", "Regime: loaded daily OHLC from disk cache.")
            except Exception:
                pass

        # Hit Fyers only if we still have nothing
        if self._daily_df is None or self._daily_df.empty:
            try:
                self._daily_df = self.fyers.get_daily_ohlc(self.index, days=30)
                # Persist for the rest of the day
                try:
                    cache_path.parent.mkdir(exist_ok=True)
                    self._daily_df.to_parquet(cache_path)
                except Exception:
                    pass
            except Exception as e:
                msg = str(e)
                if "429" in msg or "request limit" in msg.lower():
                    self._log("RISK",
                        "Regime: Fyers rate-limited (429). Will retry next tick — defaulting to allow.")
                    self._regime_ok = True
                    return
                self._log("ERROR", f"Regime check failed: {e}")
                self._regime_ok = True   # fail-open
                return

        if self._daily_df is None or self._daily_df.empty:
            self._log("INFO", "Regime: no daily data, defaulting to allow.")
            self._regime_ok  = True
            self._regime_avg = None
            return

        ok, avg = regime_allowed(self._daily_df, now.date())
        diag    = regime_diagnostics(self._daily_df, now.date())

        self._regime_ok   = ok
        self._regime_avg  = avg
        self._regime_diag = diag
        self._regime_date = doy

        status  = diag.get("status", "?")
        avg_str = f"{avg:.2f}%" if avg is not None else "n/a"
        if status == "ok":
            self._log("INFO",
                f"Regime: OK · avg {avg_str} ≤ {diag['threshold']}%")
        elif status == "blocked":
            self._log("RISK",
                f"Regime: BLOCKED · avg {avg_str} > {diag['threshold']}% · skipping day.")
        elif status == "insufficient":
            self._log("INFO",
                f"Regime: insufficient ({diag['prior_count']}/{diag['needed']} days) · default allow.")

    # ── Entry ─────────────────────────────────────────────────────

    def _pick_expiry(self):
        """
        Auto-pick the option expiry for entry.
        - Use current (nearest) expiry.
        - If today IS the expiry day → switch to next expiry.
        """
        try:
            expiries = self.broker.get_expiries(self.index)
        except Exception as e:
            self._log("ERROR", f"Could not fetch expiries: {e}")
            return None
        if not expiries:
            self._log("ERROR", "No upcoming expiries returned by broker.")
            return None

        today = datetime.now(IST).date()
        # If first available expiry is today, jump to next
        if expiries[0] == today and len(expiries) > 1:
            return expiries[1]
        return expiries[0]

    def _enter_trade(self, signal: Signal, latest_1m: dict):
        if self.active_trade is not None:
            return

        entry_price = latest_1m["close"]
        sl, target, risk, _ = build_trade_params(signal, entry_price)
        if sl is None:
            self._log("FILTER", "Max-range filter rejected setup (>0.40%) — back to IDLE.")
            self._reset_setup()
            return

        # v2: auto-pick expiry (nearest, or next if today=expiry)
        expiry = self._pick_expiry()
        if expiry is None:
            self._reset_setup()
            return

        spot     = self.fyers.get_ltp(self.index) or entry_price
        opt_type = "PE" if signal.direction == "LONG" else "CE"
        offset   = self.pe_offset if opt_type == "PE" else self.ce_offset
        strike   = atm_strike(spot, self.index, offset)

        symbol = self.broker.get_option_symbol(
            self.index, expiry, strike, opt_type
        )
        if symbol is None:
            self._log("ERROR",
                f"Symbol lookup failed: {self.index} {strike}{opt_type} {expiry}")
            self._reset_setup()
            return

        # Capture option entry price (best-effort; falls back to None)
        try:
            opt_entry_price = self.broker.get_ltp(EXCHANGE[self.index], symbol)
        except Exception:
            opt_entry_price = None

        qty      = LOT_SIZE[self.index] * self.lots
        order_id = None

        if not self.paper_mode:
            try:
                order_id = self.broker.sell_option(
                    symbol, EXCHANGE[self.index], qty
                )
                self._log("ORDER", f"Sell order → Zerodha | ID: {order_id}")
            except Exception as e:
                self._log("ERROR", f"Zerodha order failed: {e}")
                self._reset_setup()
                return

        self.active_trade = Trade(
            signal=signal,
            index=self.index,
            entry_price=entry_price,
            sl=sl,
            target=target,
            risk=risk,
            option_symbol=symbol,
            option_order_id=order_id,
            option_entry_price=opt_entry_price,
        )
        # Stash expiry on the trade for the journal
        self.active_trade.expiry = expiry

        self.state = ACTIVE
        tag = "[PAPER] " if self.paper_mode else ""
        opt_str = f" | opt entry ~{round(opt_entry_price,2)}" if opt_entry_price else ""
        self._log("ENTRY", (
            f"{tag}{signal.direction} | SELL {symbol} ({expiry}) x{qty} | "
            f"spot ~{round(spot,2)}{opt_str} | SL {sl} | Target {target} (1:1)"
        ))

    # ── Exit ──────────────────────────────────────────────────────

    def _close_trade(self, reason: str):
        trade = self.active_trade
        qty   = LOT_SIZE[self.index] * self.lots

        # Capture option exit price (best-effort)
        try:
            opt_exit_price = self.broker.get_ltp(EXCHANGE[self.index], trade.option_symbol)
        except Exception:
            opt_exit_price = None

        if not self.paper_mode:
            try:
                oid = self.broker.buy_option(
                    trade.option_symbol, EXCHANGE[self.index], qty
                )
                self._log("ORDER", f"Cover order → Zerodha | ID: {oid}")
            except Exception as e:
                self._log("ERROR", f"Exit order failed: {e}")

        # 1:1 RR — index-level pnl (used for circuit breaker tracking)
        if reason == "TARGET":
            trade.pnl = round(trade.risk * 1 * qty, 2)
        elif reason == "SL":
            trade.pnl = round(-trade.risk * qty, 2)
        else:
            trade.pnl = 0
        trade.status = reason

        # Option-level pnl (we SOLD the option, so profit when exit < entry)
        opt_pnl = None
        if trade.option_entry_price is not None and opt_exit_price is not None:
            opt_pnl = round((trade.option_entry_price - opt_exit_price) * qty, 2)

        # Journal entry — captured for both paper and live for traceability
        now_ist = datetime.now(IST)
        self.paper_trades.append({
            "entry_time":     trade.signal.confirmed_at.strftime("%Y-%m-%d %H:%M:%S"),
            "exit_time":      now_ist.strftime("%Y-%m-%d %H:%M:%S"),
            "direction":      trade.signal.direction,
            "index":          trade.index,
            "expiry":         str(getattr(trade, 'expiry', '')),
            "option_symbol":  trade.option_symbol,
            "qty":            qty,
            "spot_entry":     trade.entry_price,
            "spot_sl":        trade.sl,
            "spot_target":    trade.target,
            "opt_entry":      trade.option_entry_price,
            "opt_exit":       opt_exit_price,
            "opt_pnl":        opt_pnl,
            "exit_reason":    reason,
            "mode":           "PAPER" if self.paper_mode else "LIVE",
        })

        # Consecutive-SL bookkeeping
        if reason == "SL":
            self.consec_sl_today += 1
            if self.consec_sl_today >= CONSEC_SL_LIMIT:
                self.day_stopped = True
                self._log("RISK",
                    f"{CONSEC_SL_LIMIT} consecutive SLs — day halted.")
        else:
            self.consec_sl_today = 0

        opt_str = ""
        if opt_pnl is not None:
            opt_str = f" | opt P&L {('+' if opt_pnl >= 0 else '')}{opt_pnl}"

        sign = "+" if trade.pnl >= 0 else ""
        self._log("EXIT",
            f"{reason} | {trade.option_symbol}{opt_str} | "
            f"spot P&L {sign}{trade.pnl} | back to IDLE")

        self._reset_setup()
        self.active_trade = None

    # ── Helpers ──────────────────────────────────────────────────

    def _reset_setup(self):
        """Return to IDLE; keep daily counters intact."""
        self.active_setup    = None
        self._last_signal_id = None
        self.state           = IDLE

    def _daily_reset(self, now: datetime):
        self.consec_sl_today = 0
        self.day_stopped     = False
        self.active_setup    = None
        self.active_trade    = None
        self._last_signal_id = None
        self._last_15m_check = None
        self._last_day_reset = now.timetuple().tm_yday
        self._regime_date    = None
        self.state           = IDLE
        self._log("INFO", f"Daily reset — {now.strftime('%d %b %Y')}")

    def _to_ist(self, dt) -> datetime:
        if isinstance(dt, str):
            dt = pd.to_datetime(dt)
        if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
            return dt.astimezone(IST)
        return IST.localize(dt)

    def _secs_to_next_15m_boundary(self, now: datetime) -> int:
        """
        Seconds until the next 15-min wall-clock boundary
        (HH:00, HH:15, HH:30, HH:45). Used so IDLE only wakes when a
        new 15-min candle has closed.
        """
        minute  = now.minute
        next_15 = ((minute // 15) + 1) * 15
        if next_15 >= 60:
            target = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        else:
            target = now.replace(minute=next_15, second=0, microsecond=0)
        return max(1, int((target - now).total_seconds()))

    # ── Log ───────────────────────────────────────────────────────

    def _log(self, level: str, msg: str):
        ts = datetime.now(IST).strftime("%H:%M:%S")
        with self._lock:
            self.log.append({"time": ts, "level": level, "msg": msg})
            if len(self.log) > 200:
                self.log = self.log[-200:]

    # ── UI summary ────────────────────────────────────────────────

    def status_summary(self) -> dict:
        signal   = "—"
        position = "—"
        if self.active_trade:
            signal   = self.active_trade.signal.direction
            position = self.active_trade.option_symbol
        elif self.state == WATCHING:
            signal = "Watching"
        return {
            "signal":      signal,
            "position":    position,
            "state":       self.state,
            "consec_sl":   self.consec_sl_today,
            "day_stopped": self.day_stopped,
            "regime_ok":   self._regime_ok,
            "regime_avg":  self._regime_avg,
            "ltp":         self.fyers.get_ltp(self.index) or "—",
        }

    def regime_info(self) -> dict:
        return self._regime_diag or {
            "status": "no_data",
            "prior_days": [],
            "avg": None,
            "needed": 10,
            "threshold": 1.50,
        }


def _setup_id(s: Setup) -> str:
    return f"{s.baby_close_time.isoformat()}_{s.mother_high}_{s.mother_low}"
