"""
core/fyers_feed.py

Fyers data layer — OPTIMIZED WebSocket + Smart Caching edition.
Replaces REST polling with WebSocket streaming (99% fewer API calls).
Smart cache for historical data and daily closes.
"""

import threading
import time as time_mod
from datetime import datetime, timedelta
from typing import Optional, Callable
import pytz
import pandas as pd
import requests
from .fyers_optimized import FyersWebSocketManager, SmartCache

IST = pytz.timezone("Asia/Kolkata")

FYERS_SYMBOLS = {
    "NIFTY":     "NSE:NIFTY50-INDEX",
    "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
}

INTERVALS = [1, 15]   # minutes


class CandleBuilder:
    def __init__(self, symbol: str):
        self.symbol   = symbol
        self._lock    = threading.Lock()
        self._candles: dict[int, list[dict]] = {i: [] for i in INTERVALS}
        self._current: dict[int, Optional[dict]] = {i: None for i in INTERVALS}

    def on_tick(self, ltp: float, ts: datetime):
        ts_ist = ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
        for interval in INTERVALS:
            self._update_interval(ltp, ts_ist, interval)

    def _update_interval(self, ltp: float, ts: datetime, interval: int):
        bucket = self._bucket_start(ts, interval)
        with self._lock:
            cur = self._current[interval]
            if cur is None or cur["datetime"] != bucket:
                if cur is not None:
                    self._candles[interval].append(cur)
                    if len(self._candles[interval]) > 200:
                        self._candles[interval] = self._candles[interval][-200:]
                self._current[interval] = {
                    "datetime": bucket,
                    "open":  ltp, "high": ltp,
                    "low":   ltp, "close": ltp,
                    "volume": 0,
                }
            else:
                cur["high"]  = max(cur["high"], ltp)
                cur["low"]   = min(cur["low"],  ltp)
                cur["close"] = ltp

    def get_candles(self, interval: int, include_partial: bool = False) -> pd.DataFrame:
        with self._lock:
            rows = list(self._candles[interval])
            if include_partial and self._current[interval]:
                rows = rows + [dict(self._current[interval])]
        if not rows:
            return pd.DataFrame(columns=["datetime","open","high","low","close","volume"])
        return pd.DataFrame(rows)

    def latest_ltp(self) -> Optional[float]:
        with self._lock:
            cur = self._current.get(1)
            return cur["close"] if cur else None

    @staticmethod
    def _bucket_start(ts: datetime, interval: int) -> datetime:
        total_minutes = ts.hour * 60 + ts.minute
        floored = (total_minutes // interval) * interval
        h, m = divmod(floored, 60)
        return ts.replace(hour=h, minute=m, second=0, microsecond=0)


class FyersFeed:
    """
    Fyers data feed using WebSocket streaming + Smart Caching.
    - WebSocket for live quotes (0 API calls)
    - Smart cache for history and daily closes
    - Polls only as fallback if WebSocket unavailable
    """

    def __init__(self, app_id: str, secret_key: str, redirect_uri: str = "http://127.0.0.1:8501"):
        self.app_id       = app_id
        self.secret_key   = secret_key
        self.redirect_uri = redirect_uri
        self.access_token: Optional[str] = None

        self._poll_thread: Optional[threading.Thread] = None
        self._stop_flag   = threading.Event()
        self._builders: dict[str, CandleBuilder] = {}
        self._on_tick_callbacks: list[Callable] = []
        self._connected   = False
        self._tracked_indices: list[str] = []
        self._log_cb: Optional[Callable] = None
        self._poll_interval = 5

        # WebSocket manager (replaces constant polling)
        self._ws_manager: Optional[FyersWebSocketManager] = None
        
        # Smart cache for historical and daily data
        self._smart_cache = SmartCache()

        # Global Fyers rate limiter — reduced since we use WebSocket for quotes
        self._req_lock      = threading.Lock()
        self._req_timestamps: list[float] = []      # epoch seconds of last calls
        self._req_per_sec   = 3                      # hard cap: 3 requests / second
        self._req_per_min   = 100                    # hard cap: 100 requests / minute

        # In-memory caches to eliminate redundant fetches
        self._history_cache: dict[str, dict] = {}    # key → {"df": df, "fetched_at": ts}
        self._daily_cache:   dict[str, dict] = {}

        for index in FYERS_SYMBOLS:
            self._builders[index] = CandleBuilder(FYERS_SYMBOLS[index])

    def _rate_limit_wait(self):
        """Block until we're below per-sec and per-min caps."""
        import time as _t
        with self._req_lock:
            now = _t.time()
            # purge timestamps older than 60s
            self._req_timestamps = [t for t in self._req_timestamps if now - t < 60]
            # check per-second
            recent_1s = [t for t in self._req_timestamps if now - t < 1]
            if len(recent_1s) >= self._req_per_sec:
                _t.sleep(1.1)
                return self._rate_limit_wait()
            # check per-minute
            if len(self._req_timestamps) >= self._req_per_min:
                oldest = self._req_timestamps[0]
                wait = 60 - (now - oldest) + 0.5
                if wait > 0:
                    _t.sleep(wait)
                return self._rate_limit_wait()
            # record
            self._req_timestamps.append(now)

    # ── Auth ──────────────────────────────────────────────────────────────────

    def login_url(self, redirect_uri: str = None) -> str:
        redir = redirect_uri or self.redirect_uri
        return (
            f"https://api-t1.fyers.in/api/v3/generate-authcode"
            f"?client_id={self.app_id}"
            f"&redirect_uri={redir}"
            f"&response_type=code"
            f"&state=ib_algo"
        )

    def complete_login(self, auth_code: str) -> str:
        import hashlib
        checksum = hashlib.sha256(
            f"{self.app_id}:{self.secret_key}".encode()
        ).hexdigest()
        resp = requests.post(
            "https://api-t1.fyers.in/api/v3/validate-authcode",
            json={"grant_type": "authorization_code", "appIdHash": checksum, "code": auth_code},
            timeout=10,
        )
        data = resp.json()
        if data.get("s") != "ok":
            raise ValueError(f"Fyers token error: {data.get('message', data)}")
        self.access_token = data["access_token"]
        return self.access_token

    def set_access_token(self, token: str):
        self.access_token = token

    @property
    def connected(self) -> bool:
        return (self._poll_thread is not None) and self._poll_thread.is_alive()

    # ── Feed (REST polling) ───────────────────────────────────────────────────
    
    def start_feed(self, indices: list[str]):
        """Start WebSocket feed (optimized) + fallback REST polling.
        Safe to call multiple times; updates tracked indices even if running."""
        # Always refresh tracked indices
        self._tracked_indices = [i for i in indices if i in FYERS_SYMBOLS]

        # Initialize WebSocket if not already done
        if self._ws_manager is None and self.access_token:
            self._ws_manager = FyersWebSocketManager(self.access_token, self.app_id)
            self._ws_manager.connect()
            
            # Subscribe to symbols
            symbols = [FYERS_SYMBOLS[i] for i in self._tracked_indices]
            self._ws_manager.subscribe(symbols)
            
            # Register callback to feed ticks into builders
            self._ws_manager.callbacks.append(self._on_websocket_quote)
            
            self._log("INFO", f"WebSocket initialized for {self._tracked_indices}")
        
        # Start fallback REST polling (only if WebSocket not ready)
        if self._poll_thread and self._poll_thread.is_alive():
            self._log("INFO", f"Feed already running — updated symbols to {self._tracked_indices}")
            return

        self._stop_flag.clear()
        self._poll_thread = threading.Thread(
            target=self._run_rest_poll_fallback, daemon=True, name="FyersRESTFallback"
        )
        self._poll_thread.start()
        self._connected = True
        self._log("INFO", f"Feed started (WebSocket primary, REST fallback) for {self._tracked_indices}")

    def _on_websocket_quote(self, symbol: str, quote_data: dict):
        """Callback from WebSocket — feed tick into builders"""
        ltp = quote_data.get("ltp")
        if ltp is None:
            return
        
        ts = datetime.now(IST)
        for index, fsym in FYERS_SYMBOLS.items():
            if fsym == symbol:
                self._builders[index].on_tick(float(ltp), ts)
        
        # Trigger registered callbacks
        for cb in self._on_tick_callbacks:
            try:
                cb(symbol, float(ltp), ts)
            except Exception:
                pass

    def stop_feed(self):
        self._stop_flag.set()
        self._connected = False
        if self._ws_manager:
            self._ws_manager.close()

    def _run_rest_poll_fallback(self):
        """Fallback REST polling - only used if WebSocket not available.
        Polls every 5 seconds instead of 1 to reduce quota usage."""
        self._log("INFO", "REST fallback poll thread running (WebSocket is primary)")
        from fyers_apiv3 import fyersModel

        # Diagnostic: log token state
        if not self.access_token:
            self._log("ERROR", "access_token is None — login not completed")
            return
        self._log("INFO", f"Token check: app_id={self.app_id}, token_len={len(self.access_token)}, token_start={self.access_token[:10]}***")

        fyers_client = fyersModel.FyersModel(
            client_id=self.app_id,
            token=self.access_token,
            log_path="",
        )
        consecutive_errors = 0

        while not self._stop_flag.is_set():
            try:
                symbols = ",".join(FYERS_SYMBOLS[i] for i in self._tracked_indices)
                self._rate_limit_wait()
                data = fyers_client.quotes({"symbols": symbols})

                if data.get("s") == "ok":
                    if consecutive_errors > 0:
                        self._log("INFO", "REST poll recovered — receiving quotes")
                    consecutive_errors = 0

                    for item in data.get("d", []):
                        sym = item.get("n", "")
                        v   = item.get("v", {})
                        ltp = v.get("lp") or v.get("last_price")
                        if ltp is None:
                            continue
                        ts = datetime.now(IST)
                        for index, fsym in FYERS_SYMBOLS.items():
                            if fsym == sym:
                                self._builders[index].on_tick(float(ltp), ts)
                        for cb in self._on_tick_callbacks:
                            try:
                                cb(sym, float(ltp), ts)
                            except Exception:
                                pass
                else:
                    consecutive_errors += 1
                    msg = data.get("message", str(data))
                    self._log("ERROR", f"Quotes API error: {msg}")
                    time_mod.sleep(min(consecutive_errors * 2, 30))

            except Exception as e:
                consecutive_errors += 1
                self._log("ERROR", f"REST poll exception: {e}")
                time_mod.sleep(min(consecutive_errors * 2, 30))
                continue

            time_mod.sleep(5)  # Fallback: poll every 5 seconds (WebSocket is primary)

        self._log("INFO", "REST poll thread stopped")
        self._connected = False

    # ── Data access ───────────────────────────────────────────────────────────

    def get_candles(self, index: str, interval_minutes: int, include_partial: bool = False) -> pd.DataFrame:
        builder = self._builders.get(index)
        if builder is None:
            return pd.DataFrame()
        return builder.get_candles(interval_minutes, include_partial=include_partial)

    def get_history_15min(self, index: str, days_back: int = 3) -> pd.DataFrame:
        """
        Fetch 15-min OHLC from Fyers history API. Cached for 60 seconds —
        the engine only checks setups every 15 minutes, so this is plenty fresh.
        """
        import time as _t
        cache_key = f"15m_{index}_{days_back}"
        cached    = self._history_cache.get(cache_key)
        if cached and (_t.time() - cached["fetched_at"]) < 60:
            return cached["df"]

        from fyers_apiv3 import fyersModel
        from datetime import date as _date

        symbol    = FYERS_SYMBOLS[index]
        today     = _date.today()
        from_date = today - timedelta(days=days_back * 2 + 7)

        fyers_client = fyersModel.FyersModel(
            client_id=self.app_id,
            token=self.access_token,
            log_path="",
        )

        self._rate_limit_wait()
        resp = fyers_client.history(data={
            "symbol":      symbol,
            "resolution":  "15",
            "date_format": "1",
            "range_from":  from_date.strftime("%Y-%m-%d"),
            "range_to":    today.strftime("%Y-%m-%d"),
            "cont_flag":   "1",
        })
        if resp.get("s") != "ok":
            raise RuntimeError(f"Fyers 15-min history error for {symbol}: {resp}")

        candles = resp.get("candles", [])
        if not candles:
            return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
        df["datetime"] = (
            pd.to_datetime(df["ts"], unit="s", utc=True)
            .dt.tz_convert("Asia/Kolkata")
        )
        df = df.drop(columns=["ts"]).sort_values("datetime").reset_index(drop=True)

        if not df.empty:
            unique_days = sorted(df["datetime"].dt.date.unique())
            keep_days   = unique_days[-days_back:] if len(unique_days) > days_back else unique_days
            df = df[df["datetime"].dt.date.isin(keep_days)].reset_index(drop=True)

        self._history_cache[cache_key] = {"df": df, "fetched_at": _t.time()}
        return df

    def get_ltp(self, index: str) -> Optional[float]:
        builder = self._builders.get(index)
        return builder.latest_ltp() if builder else None

    def add_tick_callback(self, fn: Callable):
        self._on_tick_callbacks.append(fn)

    def set_log_callback(self, fn: Callable):
        """Set a callback(level, msg) to surface REST errors in the engine log."""
        self._log_cb = fn

    def set_poll_interval(self, secs: int):
        """Adjust REST polling frequency. Engine throttles based on state
        (slow when IDLE, fast when WATCHING/ACTIVE)."""
        self._poll_interval = max(1, int(secs))

    def _log(self, level: str, msg: str):
        if self._log_cb:
            try:
                self._log_cb(level, msg)
            except Exception:
                pass
        print(f"[FYERS REST] [{level}] {msg}")

    def get_daily_closes(self, index: str, days: int = 30) -> pd.Series:
        try:
            return self._fetch_daily_closes_rest(index, days)
        except Exception:
            df = self.get_candles(index, 15)
            if df.empty:
                return pd.Series(dtype=float)
            return df["close"].reset_index(drop=True)

    def get_daily_ohlc(self, index: str, days: int = 30) -> pd.DataFrame:
        """
        Returns daily OHLC dataframe. Cached in-memory for the rest of the day.
        Engine also caches to disk so cross-restart calls don't hit Fyers.
        """
        import time as _t
        from datetime import date as _date

        cache_key = f"daily_{index}_{_date.today().isoformat()}"
        cached    = self._daily_cache.get(cache_key)
        if cached:
            return cached["df"]

        from fyers_apiv3 import fyersModel

        symbol    = FYERS_SYMBOLS[index]
        today     = _date.today()
        from_date = today - timedelta(days=days * 2 + 14)

        fyers_client = fyersModel.FyersModel(
            client_id=self.app_id,
            token=self.access_token,
            log_path="",
        )

        self._rate_limit_wait()
        resp = fyers_client.history(data={
            "symbol":      symbol,
            "resolution":  "D",
            "date_format": "1",
            "range_from":  from_date.strftime("%Y-%m-%d"),
            "range_to":    today.strftime("%Y-%m-%d"),
            "cont_flag":   "1",
        })

        if resp.get("s") != "ok":
            raise RuntimeError(f"Fyers daily history error for {symbol}: {resp}")

        candles = resp.get("candles", [])
        if not candles:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "range_pct"])

        df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
        df["date"] = (
            pd.to_datetime(df["ts"], unit="s", utc=True)
            .dt.tz_convert("Asia/Kolkata")
            .dt.date
        )
        df = df.drop(columns=["ts", "volume"]).sort_values("date").reset_index(drop=True)
        df["range_pct"] = (df["high"] - df["low"]) / df["open"] * 100

        if len(df) > days:
            df = df.tail(days).reset_index(drop=True)

        self._daily_cache[cache_key] = {"df": df, "fetched_at": _t.time()}
        return df

    def _fetch_daily_closes_rest(self, index: str, days: int) -> pd.Series:
        symbol  = FYERS_SYMBOLS[index]
        to_dt   = datetime.now(IST)
        from_dt = to_dt - timedelta(days=days + 10)
        headers = {"Authorization": f"{self.app_id}:{self.access_token}"}
        resp = requests.get(
            "https://api-t1.fyers.in/api/v3/history",
            params={
                "symbol":      symbol,
                "resolution":  "D",
                "date_format": "1",
                "range_from":  from_dt.strftime("%Y-%m-%d"),
                "range_to":    to_dt.strftime("%Y-%m-%d"),
                "cont_flag":   "1",
            },
            headers=headers,
            timeout=10,
        )
        data = resp.json()
        if data.get("s") != "ok":
            raise ValueError(data.get("message", "Fyers history error"))
        candles = data.get("candles", [])
        return pd.Series([c[4] for c in candles]).reset_index(drop=True)
