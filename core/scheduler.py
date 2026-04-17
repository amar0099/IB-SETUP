"""
core/scheduler.py

Background scheduler that:
  1. Re-logins both brokers automatically at 09:00 IST every morning
  2. Starts the algo engine at 09:15 IST
  3. Stops the algo engine at 15:30 IST
  4. Handles token expiry gracefully mid-session (retry on auth error)

Runs as a daemon thread — Streamlit app just calls Scheduler.start() once.
All state changes are written back to st.session_state via a callback so
the UI reflects the current connection status.
"""

import threading
import time as time_mod
from datetime import datetime, time
from typing import Callable, Optional
import pytz

IST = pytz.timezone("Asia/Kolkata")

# Daily schedule (IST)
LOGIN_TIME  = time(9, 0)    # re-login both brokers
START_TIME  = time(9, 15)   # start algo engine
STOP_TIME   = time(15, 30)  # stop algo engine
SLEEP_TICK  = 30            # seconds between schedule checks


class DailyScheduler:
    """
    Daemon thread that manages the full daily lifecycle:
      09:00 → re-login Fyers + Zerodha (always fresh token)
      09:15 → start algo engine
      15:30 → stop algo engine
      repeat next day
    """

    def __init__(
        self,
        on_login_success: Callable,   # fn(fyers_token, zerodha_token)
        on_login_failure: Callable,   # fn(error_message)
        on_log:           Callable,   # fn(level, message)
    ):
        self._on_login_success = on_login_success
        self._on_login_failure = on_login_failure
        self._on_log           = on_log

        self._thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()

        # Track which actions have fired today
        self._last_login_day:  Optional[int] = None
        self._last_start_day:  Optional[int] = None
        self._last_stop_day:   Optional[int] = None

        # Credentials (set before starting)
        self.fy_client_id  = ""
        self.fy_secret_key = ""
        self.fy_username   = ""
        self.fy_pin        = ""
        self.fy_totp_key   = ""
        self.zd_api_key    = ""
        self.zd_secret     = ""
        self.zd_user_id    = ""
        self.zd_password   = ""
        self.zd_totp_key   = ""

        # Engine reference (injected after first login)
        self.engine = None
        self.fyers  = None
        self.broker = None

    # ── Public ────────────────────────────────────────────────────────────────

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_flag.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="DailyScheduler"
        )
        self._thread.start()
        self._log("INFO", "Daily scheduler started.")

    def stop(self):
        self._stop_flag.set()

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def trigger_login_now(self):
        """Force immediate re-login regardless of schedule (used by UI button)."""
        threading.Thread(target=self._do_login, daemon=True, name="ForceLogin").start()

    # ── Main loop ─────────────────────────────────────────────────────────────

    def _loop(self):
        while not self._stop_flag.is_set():
            now = datetime.now(IST)
            doy = now.timetuple().tm_yday
            t   = now.time()

            # 09:00 — re-login
            if t >= LOGIN_TIME and self._last_login_day != doy:
                self._last_login_day = doy
                self._do_login()

            # 09:15 — start engine
            if t >= START_TIME and self._last_start_day != doy:
                self._last_start_day = doy
                self._do_start_engine()

            # 15:30 — stop engine
            if t >= STOP_TIME and self._last_stop_day != doy:
                self._last_stop_day = doy
                self._do_stop_engine()

            time_mod.sleep(SLEEP_TICK)

    # ── Login ─────────────────────────────────────────────────────────────────

    def _do_login(self):
        self._log("INFO", "Scheduled daily re-login starting…")
        try:
            from .totp_login import (
                FyersTOTPLogin, ZerodhaTOTPLogin, clear_all_caches
            )
            from .fyers_feed import FyersFeed
            from .broker     import ZerodhaClient

            # Force fresh tokens (clear yesterday's cache)
            clear_all_caches()

            def _status(msg):
                self._log("INFO", msg)

            # Fyers
            fy = FyersTOTPLogin(
                client_id  = self.fy_client_id,
                secret_key = self.fy_secret_key,
                username   = self.fy_username,
                pin        = self.fy_pin,
                totp_key   = self.fy_totp_key,
            )
            fy_token = fy.get_access_token(force=True, status_cb=_status)

            fyers = FyersFeed(self.fy_client_id, self.fy_secret_key)
            fyers.set_access_token(fy_token)
            self.fyers = fyers

            # Zerodha
            zd = ZerodhaTOTPLogin(
                api_key    = self.zd_api_key,
                api_secret = self.zd_secret,
                user_id    = self.zd_user_id,
                password   = self.zd_password,
                totp_key   = self.zd_totp_key,
            )
            zd_token = zd.get_access_token(force=True, status_cb=_status)

            broker = ZerodhaClient(self.zd_api_key, self.zd_secret)
            broker.set_access_token(zd_token)
            self.broker = broker

            self._log("INFO", "Both brokers re-connected successfully.")
            self._on_login_success(fyers, broker)

        except Exception as e:
            self._log("ERROR", f"Scheduled login failed: {e}")
            self._on_login_failure(str(e))
            # Retry in 5 minutes
            time_mod.sleep(300)
            self._last_login_day = None   # allow retry

    # ── Engine start / stop ───────────────────────────────────────────────────

    def _do_start_engine(self):
        if self.engine is None:
            self._log("INFO", "Engine not ready at 09:15 — login may still be in progress.")
            return
        if not self.engine.running:
            self.engine.start()
            self._log("INFO", "Engine auto-started at 09:15.")

    def _do_stop_engine(self):
        if self.engine and self.engine.running:
            self.engine.stop()
            self._log("INFO", "Engine auto-stopped at 15:30.")

    # ── Log ───────────────────────────────────────────────────────────────────

    def _log(self, level: str, msg: str):
        try:
            self._on_log(level, msg)
        except Exception:
            pass
