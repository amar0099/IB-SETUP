"""
core/totp_login.py

Headless TOTP login for BOTH Fyers and Zerodha.
No browser. No redirect URI. No daily manual steps.

Fyers  — 5-step vagator API (same as dashboard.py)
Zerodha — kite-login API + TOTP → request_token → access_token

Credentials priority:
  1. .streamlit/secrets.toml
  2. Environment variables
  3. Constructor arguments

Tokens cached to .tokens/ directory, keyed by broker + today's date.
Warm restarts within the same day skip login entirely.
"""

import base64
import hashlib
import os
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

import pyotp
import requests
import streamlit as st
from fyers_apiv3 import fyersModel

# ── Token cache directory ─────────────────────────────────────────────────────
TOKEN_DIR = Path(".tokens")
TOKEN_DIR.mkdir(exist_ok=True)


def _get_secret(key: str, fallback: str = "") -> str:
    try:
        if key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass
    return os.environ.get(key, fallback)


def _b64(value: str) -> str:
    return base64.b64encode(str(value).encode()).decode()


# ── Generic file-based token cache ───────────────────────────────────────────

class _TokenCache:
    def __init__(self, name: str):
        self._file = TOKEN_DIR / f"{name}.txt"
        self._date = TOKEN_DIR / f"{name}_date.txt"

    def load(self) -> Optional[str]:
        """Return token only if saved today."""
        try:
            if self._date.read_text().strip() == date.today().isoformat():
                t = self._file.read_text().strip()
                return t if t else None
        except Exception:
            pass
        return None

    def save(self, token: str):
        self._file.write_text(token.strip())
        self._date.write_text(date.today().isoformat())

    def clear(self):
        for f in (self._file, self._date):
            if f.exists():
                f.unlink()


# ══════════════════════════════════════════════════════════════════════════════
# FYERS — 5-step vagator headless login
# ══════════════════════════════════════════════════════════════════════════════

def _fyers_login(
    client_id:  str,
    secret_key: str,
    username:   str,
    pin:        str,
    totp_key:   str,
    status_cb=None,
) -> tuple[Optional[str], Optional[str]]:
    """
    Full 5-step Fyers TOTP login.
    Returns (access_token, None) or (None, error).
    """
    def _s(msg):
        if status_cb: status_cb(msg)

    redirect_uri = "http://127.0.0.1:8080/"

    try:
        sess = requests.Session()

        # Clear any existing Fyers session before starting
        try:
            sess.get("https://api-t2.fyers.in/vagator/v2/logout", timeout=5)
        except Exception:
            pass
        
        # Step 1 — send OTP
        # Step 1 — send OTP
        _s("Fyers 1/5 — sending login OTP…")
        r1 = sess.post(
            "https://api-t2.fyers.in/vagator/v2/send_login_otp_v2",
            json={"fy_id": _b64(username), "app_id": "2"}, timeout=10,
        )
        if r1.status_code == 429:
            return None, "Fyers rate-limited (429). Wait ~60 s and retry."
        r1d = r1.json()
        if r1d.get("s") != "ok":
            return None, f"Fyers step 1 failed: {r1d}"

        # Step 2 — verify TOTP
        _s("Fyers 2/5 — verifying TOTP…")
        r2 = sess.post(
            "https://api-t2.fyers.in/vagator/v2/verify_otp",
            json={"request_key": r1d["request_key"], "otp": pyotp.TOTP(totp_key).now()},
            timeout=10,
        )
        r2d = r2.json()
        if r2d.get("s") != "ok":
            return None, f"Fyers step 2 failed: {r2d}"

        # Step 3 — verify PIN
        _s("Fyers 3/5 — verifying PIN…")
        r3 = sess.post(
            "https://api-t2.fyers.in/vagator/v2/verify_pin_v2",
            json={"request_key": r2d["request_key"],
                  "identity_type": "pin", "identifier": _b64(pin)},
            timeout=10,
        )
        r3d = r3.json()
        if r3d.get("s") != "ok":
            return None, f"Fyers step 3 failed: {r3d}"

        # Step 4 — get auth code
        # fyers_id must be the Fyers Client ID (e.g. "XY12345"), NOT email.
        # Step 3 response contains the actual fy_id — use that directly.
        _s("Fyers 4/5 — fetching auth code…")
        app_id_short = client_id.split("-")[-1]

        # Extract the verified fy_id from the Step 3 session data
        # Fyers returns the canonical client ID here regardless of login method

        r4 = sess.post(
            "https://api-t1.fyers.in/api/v3/token",
            json={
                "fyers_id": username,
                "app_id":   client_id.split("-")[0],
                "redirect_uri": redirect_uri,
                "appType": client_id.split("-")[-1],
                "code_challenge": "",
                "state": "algo",
                "scope": "",
                "nonce": "",
                "response_type": "code",
                "create_cookie": True,
            },
            headers={"Authorization": f"Bearer {r3d['data']['access_token']}"},
            timeout=10,
        )
        r4d = r4.json()
        _s(f"Fyers step 4 response: {r4d}")
        if r4d.get("s") != "ok":
            return None, f"Fyers step 4 failed: {r4d}"

        data = r4d.get("data", {})
        auth_code = (
            data.get("auth")
            or parse_qs(urlparse(r4d.get("Url", "")).query).get("auth_code", [None])[0]
            or parse_qs(urlparse(data.get("url", "")).query).get("auth_code", [None])[0]
        )
        if not auth_code:
            return None, f"Fyers step 4: no auth_code in {r4d}"

        # Step 5 — exchange for access token
        # Hash uses app_id_short:secret_key
        _s("Fyers 5/5 — exchanging for access token…")
        app_hash = hashlib.sha256(f"{app_id_short}:{secret_key}".encode()).hexdigest()
        r5 = sess.post(
            "https://api-t1.fyers.in/api/v3/validate-authcode",
            json={"grant_type": "authorization_code",
                  "appIdHash": app_hash, "code": auth_code},
            timeout=10,
        )
        r5d   = r5.json()
        token = r5d.get("access_token")

        # Fallback: legacy SDK
        if not token:
            _s("Fyers 5/5 — trying legacy SDK fallback…")
            sdk = fyersModel.SessionModel(
                client_id=client_id, secret_key=secret_key,
                redirect_uri=redirect_uri,
                response_type="code", grant_type="authorization_code",
            )
            sdk.set_token(auth_code)
            r5d   = sdk.generate_token()
            token = r5d.get("access_token")

        if not token:
            return None, f"Fyers step 5 failed: {r5d}"

        _s("Fyers login complete.")
        return token, None

    except Exception as e:
        return None, f"Fyers exception: {e}"


# ══════════════════════════════════════════════════════════════════════════════
# ZERODHA — headless TOTP login via Kite login API
# ══════════════════════════════════════════════════════════════════════════════

def _zerodha_login(
    api_key:    str,
    api_secret: str,
    user_id:    str,
    password:   str,
    totp_key:   str,
    status_cb=None,
) -> tuple[Optional[str], Optional[str]]:
    """
    Headless Zerodha login using Kite's web login API + TOTP.
    Returns (access_token, None) or (None, error).

    Flow:
      1. POST /api/login  with user_id + password  → request_id
      2. POST /api/twofa  with request_id + TOTP   → enctoken (session cookie)
      3. GET  /connect/login?api_key=...            → redirects with request_token
      4. POST kiteconnect generate_session          → access_token
    """
    def _s(msg):
        if status_cb: status_cb(msg)

    try:
        sess = requests.Session()
        sess.headers.update({"X-Kite-Version": "3"})

        # Step 1 — username + password login
        _s("Zerodha 1/4 — logging in with credentials…")
        r1 = sess.post(
            "https://kite.zerodha.com/api/login",
            data={"user_id": user_id, "password": password},
            timeout=10,
        )
        r1d = r1.json()
        if r1d.get("status") != "success":
            return None, f"Zerodha step 1 failed: {r1d.get('message', r1d)}"

        request_id = r1d["data"]["request_id"]

        # Step 2 — TOTP 2FA
        _s("Zerodha 2/4 — verifying TOTP…")
        totp_val = pyotp.TOTP(totp_key).now()
        r2 = sess.post(
            "https://kite.zerodha.com/api/twofa",
            data={
                "user_id":    user_id,
                "request_id": request_id,
                "twofa_value": totp_val,
                "twofa_type": "totp",
                "skip_session": "",
            },
            timeout=10,
        )
        r2d = r2.json()
        if r2d.get("status") != "success":
            return None, f"Zerodha step 2 failed: {r2d.get('message', r2d)}"

        # Step 3 — get request_token via connect/login redirect
        _s("Zerodha 3/4 — fetching request token…")
        r3 = sess.get(
            f"https://kite.zerodha.com/connect/login?api_key={api_key}&v=3",
            allow_redirects=True,
            timeout=10,
        )
        # The final URL after redirects contains request_token
        final_url   = r3.url
        parsed      = urlparse(final_url)
        params      = parse_qs(parsed.query)
        req_token   = params.get("request_token", [None])[0]

        if not req_token:
            # Sometimes it's in the fragment
            frag_params = parse_qs(parsed.fragment)
            req_token   = frag_params.get("request_token", [None])[0]

        if not req_token:
            return None, (
                f"Zerodha step 3: request_token not found in redirect.\n"
                f"Final URL: {final_url}"
            )

        # Step 4 — exchange request_token for access_token
        _s("Zerodha 4/4 — generating access token…")
        from kiteconnect import KiteConnect
        kite      = KiteConnect(api_key=api_key)
        session   = kite.generate_session(req_token, api_secret=api_secret)
        token     = session.get("access_token")

        if not token:
            return None, f"Zerodha step 4: no access_token in {session}"

        _s("Zerodha login complete.")
        return token, None

    except Exception as e:
        return None, f"Zerodha exception: {e}"


# ══════════════════════════════════════════════════════════════════════════════
# HIGH-LEVEL WRAPPERS  (used by app.py)
# ══════════════════════════════════════════════════════════════════════════════

class FyersTOTPLogin:
    _cache = _TokenCache("fyers")

    def __init__(self, client_id="", secret_key="", username="", pin="", totp_key=""):
        self.client_id  = client_id  or _get_secret("FYERS_CLIENT_ID")
        self.secret_key = secret_key or _get_secret("FYERS_SECRET_KEY")
        self.username   = username   or _get_secret("FYERS_USERNAME")
        self.pin        = pin        or _get_secret("FYERS_PIN")
        self.totp_key   = totp_key   or _get_secret("FYERS_TOTP_KEY")

    @property
    def credentials_complete(self) -> bool:
        return all([self.client_id, self.secret_key,
                    self.username, self.pin, self.totp_key])

    def get_access_token(self, force: bool = False, status_cb=None) -> str:
        if not force:
            cached = self._cache.load()
            if cached:
                if status_cb: status_cb("Fyers: reusing today's cached token.")
                return cached
        token, err = _fyers_login(
            self.client_id, self.secret_key,
            self.username, self.pin, self.totp_key,
            status_cb=status_cb,
        )
        if not token:
            raise RuntimeError(err)
        self._cache.save(token)
        return token

    def get_fyers_model(self, **kw) -> fyersModel.FyersModel:
        token = self.get_access_token(**kw)
        return fyersModel.FyersModel(
            client_id=self.client_id, token=token, log_path=""
        )

    @classmethod
    def clear_cache(cls):
        cls._cache.clear()


class ZerodhaTOTPLogin:
    _cache = _TokenCache("zerodha")

    def __init__(self, api_key="", api_secret="", user_id="", password="", totp_key=""):
        self.api_key    = api_key    or _get_secret("ZERODHA_API_KEY")
        self.api_secret = api_secret or _get_secret("ZERODHA_SECRET")
        self.user_id    = user_id    or _get_secret("ZERODHA_USER_ID")
        self.password   = password   or _get_secret("ZERODHA_PASSWORD")
        self.totp_key   = totp_key   or _get_secret("ZERODHA_TOTP_KEY")

    @property
    def credentials_complete(self) -> bool:
        return all([self.api_key, self.api_secret,
                    self.user_id, self.password, self.totp_key])

    def get_access_token(self, force: bool = False, status_cb=None) -> str:
        if not force:
            cached = self._cache.load()
            if cached:
                if status_cb: status_cb("Zerodha: reusing today's cached token.")
                return cached
        token, err = _zerodha_login(
            self.api_key, self.api_secret,
            self.user_id, self.password, self.totp_key,
            status_cb=status_cb,
        )
        if not token:
            raise RuntimeError(err)
        self._cache.save(token)
        return token

    @classmethod
    def clear_cache(cls):
        cls._cache.clear()


# ── Convenience: clear both caches at once ───────────────────────────────────

def clear_all_caches():
    FyersTOTPLogin.clear_cache()
    ZerodhaTOTPLogin.clear_cache()
