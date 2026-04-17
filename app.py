"""
app.py  –  Inside Bar Breakout Algo v6
Fully autonomous on Streamlit Cloud.

On first deploy:
  - Fill .streamlit/secrets.toml with credentials
  - Deploy to Streamlit Cloud

Every day after that (zero human action needed):
  09:00 → scheduler re-logins both brokers via TOTP
  09:15 → algo engine starts automatically
  15:30 → algo engine stops automatically
  repeat forever

Run locally:  streamlit run app.py
"""

import os
import time as _t
from datetime import datetime

import pandas as pd
import pytz
import streamlit as st

IST = pytz.timezone("Asia/Kolkata")

st.set_page_config(
    page_title="IB Algo",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
[data-testid="stAppViewContainer"] { background:#0f1117; }
[data-testid="stHeader"]           { background:#0f1117; }
[data-testid="block-container"]    { padding-top:1rem; }
.sh { font-size:11px;font-weight:600;letter-spacing:.08em;
      text-transform:uppercase;color:#6b7280;margin:1.2rem 0 .4rem; }
.scard  { background:#1a1d27;border:1px solid #2d3142;border-radius:8px;padding:12px 16px; }
.slabel { font-size:11px;color:#6b7280;margin-bottom:2px; }
.sval   { font-size:20px;font-weight:600;color:#e5e7eb; }
.green  { color:#22c55e!important; }
.red    { color:#ef4444!important; }
.amber  { color:#f59e0b!important; }
.blue   { color:#38bdf8!important; }
.badge-long  { background:#14532d;color:#86efac;padding:2px 10px;border-radius:20px;font-size:12px;font-weight:600; }
.badge-short { background:#450a0a;color:#fca5a5;padding:2px 10px;border-radius:20px;font-size:12px;font-weight:600; }
.badge-idle  { background:#1f2937;color:#9ca3af;padding:2px 10px;border-radius:20px;font-size:12px; }
.info-banner { background:#1e293b;border:1px solid #334155;border-radius:8px;
               padding:10px 16px;font-size:13px;color:#94a3b8;margin-bottom:12px; }
</style>
""", unsafe_allow_html=True)

# ── Imports ───────────────────────────────────────────────────────────────────
try:
    from core.broker     import ZerodhaClient
    from core.fyers_feed import FyersFeed
    from core.engine     import AlgoEngine
    from core.totp_login import FyersTOTPLogin, ZerodhaTOTPLogin, clear_all_caches
    from core.scheduler  import DailyScheduler
except ImportError as e:
    st.error(f"Missing dependency: {e}\nRun: pip install -r requirements.txt")
    st.stop()

# ── Secret helper ─────────────────────────────────────────────────────────────
def _sec(key: str, fallback: str = "") -> str:
    try:
        if key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass
    return os.environ.get(key, fallback)

# ── Session-state bootstrap ───────────────────────────────────────────────────
_DEFAULTS = {
    # Fyers creds (pre-filled from secrets)
    "fy_client_id":  _sec("FYERS_CLIENT_ID"),
    "fy_secret_key": _sec("FYERS_SECRET_KEY"),
    "fy_username":   _sec("FYERS_USERNAME"),
    "fy_pin":        _sec("FYERS_PIN"),
    "fy_totp_key":   _sec("FYERS_TOTP_KEY"),
    # Zerodha creds
    "zd_api_key":    _sec("ZERODHA_API_KEY"),
    "zd_secret":     _sec("ZERODHA_SECRET"),
    "zd_user_id":    _sec("ZERODHA_USER_ID"),
    "zd_password":   _sec("ZERODHA_PASSWORD"),
    "zd_totp_key":   _sec("ZERODHA_TOTP_KEY"),
    # Strategy defaults
    "algo_index":      "NIFTY",
    "algo_lots":       1,
    "algo_pe_offset":  0,
    "algo_ce_offset":  0,
    "algo_paper_mode": True,   # safe default — switch to Live when ready
    # Runtime objects
    "scheduler":     None,
    "engine":        None,
    "fyers":         None,
    "broker":        None,
    "fy_connected":  False,
    "zd_connected":  False,
    "sched_log":     [],       # shared log between scheduler + engine
    "login_error":   "",
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ─────────────────────────────────────────────────────────────────────────────
# BOOTSTRAP — runs once per Streamlit process start
# Creates the scheduler and engine; scheduler self-manages daily lifecycle
# ─────────────────────────────────────────────────────────────────────────────

def _append_log(level: str, msg: str):
    """Thread-safe log append — called from scheduler/engine background threads."""
    ts = datetime.now(IST).strftime("%H:%M:%S")
    entry = {"time": ts, "level": level, "msg": msg}
    # Session state writes from background threads are safe in Streamlit ≥ 1.27
    try:
        st.session_state.sched_log.append(entry)
        if len(st.session_state.sched_log) > 300:
            st.session_state.sched_log = st.session_state.sched_log[-300:]
    except Exception:
        pass


def _on_login_success(fyers: FyersFeed, broker: ZerodhaClient):
    """Called by scheduler after successful daily re-login."""
    st.session_state.fyers        = fyers
    st.session_state.broker       = broker
    st.session_state.fy_connected = True
    st.session_state.zd_connected = True
    st.session_state.login_error  = ""

    # Rebuild engine with fresh broker objects
    engine = AlgoEngine(fyers, broker)
    engine.index      = st.session_state.algo_index
    engine.lots       = st.session_state.algo_lots
    engine.pe_offset  = st.session_state.algo_pe_offset
    engine.ce_offset  = st.session_state.algo_ce_offset
    engine.paper_mode = st.session_state.algo_paper_mode

    # Inject into scheduler so it can auto-start at 09:15
    st.session_state.scheduler.engine = engine
    st.session_state.engine = engine


def _on_login_failure(error: str):
    st.session_state.login_error  = error
    st.session_state.fy_connected = False
    st.session_state.zd_connected = False


def _bootstrap():
    """
    Called once per app process. Creates the DailyScheduler and starts it.
    The scheduler fires its first login immediately if credentials are present
    and it's before market close (so cold deploys mid-day connect right away).
    """
    if st.session_state.scheduler is not None:
        return   # already bootstrapped

    creds_ok = all([
        st.session_state.fy_client_id, st.session_state.fy_secret_key,
        st.session_state.fy_username,  st.session_state.fy_pin,
        st.session_state.fy_totp_key,
        st.session_state.zd_api_key,   st.session_state.zd_secret,
        st.session_state.zd_user_id,   st.session_state.zd_password,
        st.session_state.zd_totp_key,
    ])

    if not creds_ok:
        return   # wait for user to fill credentials form

    sched = DailyScheduler(
        on_login_success = _on_login_success,
        on_login_failure = _on_login_failure,
        on_log           = _append_log,
    )

    # Inject credentials
    sched.fy_client_id  = st.session_state.fy_client_id
    sched.fy_secret_key = st.session_state.fy_secret_key
    sched.fy_username   = st.session_state.fy_username
    sched.fy_pin        = st.session_state.fy_pin
    sched.fy_totp_key   = st.session_state.fy_totp_key
    sched.zd_api_key    = st.session_state.zd_api_key
    sched.zd_secret     = st.session_state.zd_secret
    sched.zd_user_id    = st.session_state.zd_user_id
    sched.zd_password   = st.session_state.zd_password
    sched.zd_totp_key   = st.session_state.zd_totp_key

    st.session_state.scheduler = sched
    sched.start()

    # If we're starting mid-day (e.g. fresh deploy at 10 AM),
    # trigger login immediately without waiting for 09:00 tomorrow
    now = datetime.now(IST)
    from core.scheduler import LOGIN_TIME, STOP_TIME
    if LOGIN_TIME <= now.time() <= STOP_TIME:
        _append_log("INFO", "Mid-day start detected — triggering immediate login.")
        sched.trigger_login_now()


_bootstrap()

# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("## Inside Bar Breakout Algo")

now_ist = datetime.now(IST)
st.caption(
    f"Fully autonomous · {now_ist.strftime('%d %b %Y  %H:%M:%S IST')}  |  "
    f"Auto-login 09:00 · Auto-start 09:15 · Auto-stop 15:30"
)

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 — CREDENTIALS (only shown if not in secrets.toml)
# ─────────────────────────────────────────────────────────────────────────────
creds_from_secrets = all([
    st.session_state.fy_client_id, st.session_state.fy_secret_key,
    st.session_state.fy_username,  st.session_state.fy_pin,
    st.session_state.fy_totp_key,
    st.session_state.zd_api_key,   st.session_state.zd_secret,
    st.session_state.zd_user_id,   st.session_state.zd_password,
    st.session_state.zd_totp_key,
])

if not creds_from_secrets:
    st.markdown("<div class='sh'>Credentials (one-time setup)</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='info-banner'>Fill these once. On Streamlit Cloud add them to "
        "<b>App Settings → Secrets</b> and they'll never appear here again.</div>",
        unsafe_allow_html=True,
    )

    with st.form("creds"):
        st.markdown("**Fyers**")
        fc1, fc2 = st.columns(2)
        st.session_state.fy_client_id  = fc1.text_input("Client ID",   value=st.session_state.fy_client_id,  placeholder="XXXX-100")
        st.session_state.fy_secret_key = fc2.text_input("Secret key",  value=st.session_state.fy_secret_key, placeholder="••••••••", type="password")
        fc3, fc4, fc5 = st.columns(3)
        st.session_state.fy_username   = fc3.text_input("Username",    value=st.session_state.fy_username,   placeholder="user@email.com")
        st.session_state.fy_pin        = fc4.text_input("PIN",         value=st.session_state.fy_pin,        placeholder="1234", type="password")
        st.session_state.fy_totp_key   = fc5.text_input("TOTP secret", value=st.session_state.fy_totp_key,  placeholder="BASE32…", type="password")

        st.markdown("**Zerodha**")
        zc1, zc2 = st.columns(2)
        st.session_state.zd_api_key  = zc1.text_input("API key",    value=st.session_state.zd_api_key,  placeholder="api_key_xxx", type="password")
        st.session_state.zd_secret   = zc2.text_input("API secret", value=st.session_state.zd_secret,   placeholder="••••••••",   type="password")
        zc3, zc4, zc5 = st.columns(3)
        st.session_state.zd_user_id  = zc3.text_input("User ID",    value=st.session_state.zd_user_id,  placeholder="AB1234")
        st.session_state.zd_password = zc4.text_input("Password",   value=st.session_state.zd_password, placeholder="••••••••",   type="password")
        st.session_state.zd_totp_key = zc5.text_input("TOTP secret",value=st.session_state.zd_totp_key, placeholder="BASE32…",    type="password")

        if st.form_submit_button("Save & connect", use_container_width=True, type="primary"):
            _bootstrap()
            st.rerun()
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 — CONNECTION STATUS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("<div class='sh'>Connection status</div>", unsafe_allow_html=True)

fy_ok  = st.session_state.fy_connected
zd_ok  = st.session_state.zd_connected
ws_ok  = st.session_state.fyers.connected if st.session_state.fyers else False
sched  = st.session_state.scheduler

cs1, cs2, cs3, cs4, cs5 = st.columns(5)

def _conn_card(col, label, ok, ok_text="● Connected", fail_text="○ Waiting"):
    col.markdown(
        f"<div class='scard'><div class='slabel'>{label}</div>"
        f"<div class='sval {'green' if ok else 'amber'}' style='font-size:13px'>"
        f"{''+ok_text if ok else fail_text}</div></div>",
        unsafe_allow_html=True,
    )

_conn_card(cs1, "Fyers login",   fy_ok)
_conn_card(cs2, "Zerodha login", zd_ok)
_conn_card(cs3, "Fyers WS feed", ws_ok, "● Live", "○ Offline")
_conn_card(cs4, "Scheduler",     sched and sched.running, "● Running", "○ Stopped")

# Show login error if any
if st.session_state.login_error:
    cs5.markdown(
        f"<div class='scard' style='border-color:#ef4444'>"
        f"<div class='slabel'>Last error</div>"
        f"<div style='font-size:11px;color:#ef4444;margin-top:2px'>"
        f"{st.session_state.login_error[:80]}</div></div>",
        unsafe_allow_html=True,
    )
else:
    next_login = "09:00 tomorrow"
    now_t = datetime.now(IST).time()
    from core.scheduler import LOGIN_TIME
    if now_t < LOGIN_TIME:
        next_login = "09:00 today"
    cs5.markdown(
        f"<div class='scard'><div class='slabel'>Next auto-login</div>"
        f"<div class='sval' style='font-size:13px;color:#6b7280'>{next_login}</div></div>",
        unsafe_allow_html=True,
    )

# Manual override buttons
mb1, mb2, mb3 = st.columns([1, 1, 4])
with mb1:
    if st.button("Re-login now", use_container_width=True):
        if sched:
            clear_all_caches()
            sched.trigger_login_now()
            st.toast("Re-login triggered…")
with mb2:
    if st.button("Clear token cache", use_container_width=True):
        clear_all_caches()
        st.toast("Token cache cleared. Next login will run fresh TOTP.")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — STRATEGY CONFIG
# (saved to session_state so scheduler can inject into engine after re-login)
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("<div class='sh'>Strategy config</div>", unsafe_allow_html=True)

cfg1, cfg2, cfg3, cfg4 = st.columns([1.4, 1.8, 1, 1])

with cfg1:
    index = st.selectbox("Index", ["NIFTY", "BANKNIFTY"],
                         index=["NIFTY","BANKNIFTY"].index(st.session_state.algo_index))
    st.session_state.algo_index = index

with cfg2:
    selected_expiry = None
    expiry_labels   = ["(not connected)"]
    if st.session_state.broker:
        try:
            expiries      = st.session_state.broker.get_expiries(index)
            expiry_labels = [str(e) for e in expiries]
            exp_sel       = st.selectbox("Expiry", expiry_labels)
            selected_expiry = expiries[expiry_labels.index(exp_sel)]
        except Exception:
            st.selectbox("Expiry", ["(fetch failed)"])
    else:
        st.selectbox("Expiry", expiry_labels)

with cfg3:
    lots = st.number_input("Lots", min_value=1, max_value=50,
                           value=st.session_state.algo_lots)
    st.session_state.algo_lots = lots

with cfg4:
    mode_idx = 1 if st.session_state.algo_paper_mode else 0
    mode = st.selectbox("Mode", ["Live", "Paper"], index=mode_idx)
    st.session_state.algo_paper_mode = (mode == "Paper")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 — STRIKE SELECTION
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("<div class='sh'>Strike selection</div>", unsafe_allow_html=True)

OFFSETS       = list(range(-1000, 1001, 100))
OFFSET_LABELS = {o: ("ATM" if o == 0 else f"ATM{'+' if o > 0 else ''}{o}") for o in OFFSETS}

sk1, sk2 = st.columns(2)
with sk1:
    st.markdown("**Long signal → Sell PE at**")
    pe_offset = st.select_slider("PE strike", options=OFFSETS,
                                 value=st.session_state.algo_pe_offset,
                                 format_func=lambda x: OFFSET_LABELS[x],
                                 label_visibility="collapsed")
    st.session_state.algo_pe_offset = pe_offset
    st.caption(f"Sells **{OFFSET_LABELS[pe_offset]}** PE on Zerodha on LONG")

with sk2:
    st.markdown("**Short signal → Sell CE at**")
    ce_offset = st.select_slider("CE strike", options=OFFSETS,
                                 value=st.session_state.algo_ce_offset,
                                 format_func=lambda x: OFFSET_LABELS[x],
                                 label_visibility="collapsed")
    st.session_state.algo_ce_offset = ce_offset
    st.caption(f"Sells **{OFFSET_LABELS[ce_offset]}** CE on Zerodha on SHORT")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 — MANUAL ENGINE CONTROLS
# (engine auto-starts at 09:15 but these allow manual override)
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("<div class='sh'>Engine controls (manual override)</div>", unsafe_allow_html=True)

engine = st.session_state.engine
eng_ready = engine is not None

b1, b2, b3, _ = st.columns([1, 1, 1, 2])

with b1:
    if st.button("▶  Start now", use_container_width=True,
                 disabled=(not eng_ready) or (engine and engine.running) or not selected_expiry):
        engine.expiry     = selected_expiry
        engine.index      = index
        engine.lots       = lots
        engine.pe_offset  = pe_offset
        engine.ce_offset  = ce_offset
        engine.paper_mode = (mode == "Paper")
        engine.start()
        st.rerun()

with b2:
    if st.button("⏹  Stop now", use_container_width=True,
                 disabled=(not eng_ready) or (engine and not engine.running)):
        engine.stop()
        st.rerun()

with b3:
    if st.button("↺  Apply config", use_container_width=True,
                 help="Push updated index/lots/offsets/mode to running engine"):
        if engine:
            engine.index      = index
            engine.lots       = lots
            engine.pe_offset  = pe_offset
            engine.ce_offset  = ce_offset
            engine.paper_mode = (mode == "Paper")
            if selected_expiry:
                engine.expiry = selected_expiry
            st.toast("Config applied to engine.")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 — LIVE STATUS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("<div class='sh'>Live status</div>", unsafe_allow_html=True)

summary = engine.status_summary() if engine else {
    "signal": "—", "position": "—", "ltp": "—", "ema20": "—", "sl_hits": 0
}

c1, c2, c3, c4, c5 = st.columns(5)

def _stat(col, label, val, css=""):
    col.markdown(
        f"<div class='scard'><div class='slabel'>{label}</div>"
        f"<div class='sval {css}'>{val}</div></div>",
        unsafe_allow_html=True,
    )

sig = summary["signal"]
sig_html = (
    "<span class='badge-long'>LONG</span>"     if sig == "LONG"     else
    "<span class='badge-short'>SHORT</span>"   if sig == "SHORT"    else
    "<span class='badge-idle'>Watching</span>" if sig == "Watching" else
    "<span class='badge-idle'>Idle</span>"
)
c1.markdown(
    f"<div class='scard'><div class='slabel'>Signal</div>"
    f"<div style='padding-top:4px'>{sig_html}</div></div>",
    unsafe_allow_html=True,
)
_stat(c2, "Position",    summary["position"])
_stat(c3, "LTP (Fyers)", summary["ltp"],   "blue")
_stat(c4, "EMA20",       summary["ema20"])
sl = summary["sl_hits"]
_stat(c5, "SL hits today", f"{sl} / 2",
      "red" if sl >= 2 else ("amber" if sl == 1 else "green"))

eng_color = "green" if (engine and engine.running) else "red"
eng_txt   = "● Running" if (engine and engine.running) else "○ Stopped"
st.markdown(
    f"<div style='margin-top:8px;font-size:12px;color:#6b7280'>"
    f"Engine: <span class='{eng_color}'>{eng_txt}</span>"
    f"{'&nbsp;&nbsp;|&nbsp;&nbsp;Paper mode' if (engine and engine.paper_mode) else ''}"
    f"</div>", unsafe_allow_html=True,
)

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 7 — LIVE CANDLES
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("<div class='sh'>Live 1-min candles (Fyers)</div>", unsafe_allow_html=True)
fyers = st.session_state.fyers
if fyers:
    df_live = fyers.get_candles(index, 1, include_partial=True)
    if not df_live.empty:
        show = df_live.tail(20).copy()
        show["datetime"] = show["datetime"].astype(str)
        for col in ["open","high","low","close"]:
            show[col] = show[col].round(2)
        st.dataframe(show[["datetime","open","high","low","close"]],
                     use_container_width=True, height=240, hide_index=True)
    else:
        st.caption("Waiting for Fyers tick data…")
else:
    st.caption("Not connected yet — login scheduled for 09:00.")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 8 — UNIFIED ACTIVITY LOG (scheduler + engine combined)
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("<div class='sh'>Activity log</div>", unsafe_allow_html=True)

LOG_COLORS = {
    "INFO":   "#6b7280", "SETUP":  "#818cf8", "SIGNAL": "#38bdf8",
    "ENTRY":  "#22c55e", "EXIT":   "#f59e0b", "ORDER":  "#a78bfa",
    "RISK":   "#ef4444", "FILTER": "#f59e0b", "ERROR":  "#ef4444",
}

# Merge scheduler log + engine log
sched_log  = list(st.session_state.sched_log)
engine_log = list(engine.log) if engine else []
all_logs   = sorted(sched_log + engine_log, key=lambda x: x["time"], reverse=True)[:100]

if all_logs:
    df_log = pd.DataFrame(all_logs)[["time","level","msg"]]
    df_log.columns = ["Time","Level","Message"]
    def _style(row):
        return [f"color:{LOG_COLORS.get(row['Level'], '#9ca3af')}"] * len(row)
    st.dataframe(df_log.style.apply(_style, axis=1),
                 use_container_width=True, height=320, hide_index=True)
else:
    st.caption("No activity yet.")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 9 — DEBUG
# ─────────────────────────────────────────────────────────────────────────────
with st.expander("Debug — active setup / trade / candles"):
    s = engine.active_setup if engine else None
    t = engine.active_trade if engine else None
    if s:
        st.json({"index": s.index, "mother_high": s.mother_high,
                 "mother_low": s.mother_low, "range_pts": s.range_pts,
                 "baby_close_time": str(s.baby_close_time)})
    if t:
        st.json({"direction": t.signal.direction, "entry": t.entry_price,
                 "sl": t.sl, "target": t.target, "risk": t.risk,
                 "symbol": t.option_symbol, "order_id": t.option_order_id})
    if not s and not t:
        st.caption("No active setup or trade.")
    if fyers:
        df15 = fyers.get_candles(index, 15)
        if not df15.empty:
            st.markdown("**Last 5 × 15-min candles**")
            st.dataframe(df15.tail(5).round(2), hide_index=True)

# ─────────────────────────────────────────────────────────────────────────────
# Auto-refresh: fast while engine running, slow otherwise
# ─────────────────────────────────────────────────────────────────────────────
if engine and engine.running:
    _t.sleep(0.5)
    st.rerun()
else:
    _t.sleep(10)
    st.rerun()
