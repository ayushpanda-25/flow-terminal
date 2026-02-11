#!/usr/bin/env python3
"""
Flow Terminal — Local LSEG-to-WebSocket Bridge

Connects to LSEG Workspace Desktop (running locally) via refinitiv-data,
subscribes to streaming equity prices and polls options chain data,
then relays normalized JSON to browser clients over ws://localhost:8765.

Usage:
    1. Ensure LSEG Workspace is running and signed in.
    2. pip3 install refinitiv-data websockets
    3. python3 local_bridge.py
    4. Open options-flow-dashboard.html → select "Local Mode"
"""

import asyncio
import json
import logging
import math
import os
import signal
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

import websockets

# ─── Monkey-patch refinitiv-data httpx proxy issue ────────────────────────────
# refinitiv-data 1.6.2 passes a dict to httpx's proxy= kwarg, but httpx 0.28+
# expects a string URL. Patch before importing refinitiv.data.
try:
    import refinitiv.data._core.session.http_service as _hs
    import httpx as _httpx

    def _patched_get_httpx_client(proxies, **kwargs):
        proxy_url = None
        if isinstance(proxies, dict):
            for _k, _v in proxies.items():
                if _v:
                    proxy_url = _v
                    break
        elif isinstance(proxies, str):
            proxy_url = proxies
        return _httpx.Client(proxy=proxy_url, **kwargs)

    _hs.get_httpx_client = _patched_get_httpx_client
except Exception:
    pass  # If patch fails, proceed anyway — may work on other httpx versions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("flow_bridge")

# ─── Config ──────────────────────────────────────────────────────────────────

APP_KEY = os.environ.get(
    "EIKON_APP_KEY", "d5f36229fb7344078aa2fa3de7e5fca8be6e11a6"
)
WS_PORT = int(os.environ.get("FLOW_WS_PORT", "8765"))
HTTP_PORT = int(os.environ.get("FLOW_HTTP_PORT", "8766"))

# ── RIC resolution: try multiple exchange suffixes for real-time entitlements ──
# .P = Primary, .A = NYSE Arca, .O/.OQ = NASDAQ, .N = NYSE
# The first one that returns data in open_pricing_stream() wins.
# We'll resolve these at startup by testing which suffixes are entitled.
TICKER_RIC_CANDIDATES = {
    "SPY":  ["SPY.A", "SPY.P", "SPY"],
    "QQQ":  ["QQQ.O", "QQQ.OQ", "QQQ.P"],
    "AAPL": ["AAPL.O", "AAPL.OQ", "AAPL.P"],
    "MSFT": ["MSFT.O", "MSFT.OQ", "MSFT.P"],
    "NVDA": ["NVDA.O", "NVDA.OQ", "NVDA.P"],
    "TSLA": ["TSLA.O", "TSLA.OQ", "TSLA.P"],
    "AMZN": ["AMZN.O", "AMZN.OQ", "AMZN.P"],
    "GOOG": ["GOOG.O", "GOOG.OQ", "GOOG.P"],
    "META": ["META.O", "META.OQ", "META.P"],
}

# Resolved at startup by resolve_rics()
EQUITY_RICS: list = []
RIC_TO_TICKER: dict = {}
TICKER_TO_RIC: dict = {}

EQUITY_FIELDS = [
    "BID", "ASK", "TRDPRC_1", "ACVOL_1",
    "HIGH_1", "LOW_1", "NETCHNG_1", "PCTCHNG",
    "QUOTIM", "TRADE_DATE",  # For delayed-vs-RT detection
]
OPTION_FIELDS = [
    "CF_BID", "CF_ASK", "CF_LAST", "CF_VOLUME",
    "OPEN_INT", "IMPL_VOL", "DELTA",
]

# Per-ticker chain config: (strike_step, num_strikes)
# num_strikes passed to generate_strike_range; half on each side of ATM
# SPY/QQQ: 30 → ±15 strikes → 31 rows; others: 21 → ±10 → 21 rows
CHAIN_CFG = {
    "SPY": (1.0, 30), "QQQ": (1.0, 30),
    "AAPL": (2.5, 21), "MSFT": (2.5, 21),
    "NVDA": (2.5, 21), "TSLA": (5.0, 21),
    "AMZN": (5.0, 21), "GOOG": (5.0, 21),
    "META": (5.0, 21),
}

POLL_CHAINS_SEC = 30   # seconds between chain snapshots
POLL_FLOW_SEC = 15     # seconds between flow/unusual refreshes


# ─── Utility Functions ───────────────────────────────────────────────────────

def _safe_float(val) -> Optional[float]:
    """Safely convert any value to float, returning None on failure or NaN."""
    if val is None:
        return None
    try:
        import pandas as pd
        if pd.isna(val):
            return None
    except Exception:
        pass
    try:
        f = float(val)
        return None if f != f else f  # NaN check
    except (TypeError, ValueError):
        return None


def build_option_ric(underlying: str, expiration: str, option_type: str, strike: float) -> str:
    """Build an LSEG OPRA option RIC from components.

    Format: {UNDERLYING}{MonthCode}{DD}{YY}{StrikeNum}.U
    Uppercase month codes for strike < 1000, lowercase for >= 1000.
    """
    CALL_UPPER = "ABCDEFGHIJKL"
    PUT_UPPER = "MNOPQRSTUVWX"
    CALL_LOWER = "abcdefghijkl"
    PUT_LOWER = "mnopqrstuvwx"

    dt = datetime.strptime(expiration, "%Y-%m-%d")
    mi = dt.month - 1
    opt = option_type.upper()[0]
    day = f"{dt.day:02d}"
    yr = dt.strftime("%y")

    if strike >= 1000:
        mc = CALL_LOWER[mi] if opt == "C" else PUT_LOWER[mi]
        sn = int(round(strike * 10))
    else:
        mc = CALL_UPPER[mi] if opt == "C" else PUT_UPPER[mi]
        sn = int(round(strike * 100))

    return f"{underlying.upper()}{mc}{day}{yr}{sn}.U"


# US market holidays (month, day) — fixed-date holidays
# Floating holidays (MLK, Presidents, Memorial, Labor, Thanksgiving) use weekday rules
def _us_market_holidays(year: int) -> set:
    """Return set of date objects for US market holidays in a given year."""
    from datetime import date
    holidays = set()
    # New Year's Day
    holidays.add(date(year, 1, 1))
    # MLK Day — 3rd Monday of January
    d = date(year, 1, 1)
    mon_count = 0
    while mon_count < 3:
        if d.weekday() == 0:
            mon_count += 1
            if mon_count == 3:
                holidays.add(d)
        d += timedelta(days=1)
    # Presidents' Day — 3rd Monday of February
    d = date(year, 2, 1)
    mon_count = 0
    while mon_count < 3:
        if d.weekday() == 0:
            mon_count += 1
            if mon_count == 3:
                holidays.add(d)
        d += timedelta(days=1)
    # Good Friday — 2 days before Easter (approximate with Anonymous Gregorian algorithm)
    a = year % 19
    b = year // 100
    c = year % 100
    dd = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - dd - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    easter = date(year, month, day)
    holidays.add(easter - timedelta(days=2))
    # Memorial Day — last Monday of May
    d = date(year, 5, 31)
    while d.weekday() != 0:
        d -= timedelta(days=1)
    holidays.add(d)
    # Juneteenth
    holidays.add(date(year, 6, 19))
    # Independence Day
    holidays.add(date(year, 7, 4))
    # Labor Day — 1st Monday of September
    d = date(year, 9, 1)
    while d.weekday() != 0:
        d += timedelta(days=1)
    holidays.add(d)
    # Thanksgiving — 4th Thursday of November
    d = date(year, 11, 1)
    thu_count = 0
    while thu_count < 4:
        if d.weekday() == 3:
            thu_count += 1
            if thu_count == 4:
                holidays.add(d)
        d += timedelta(days=1)
    # Christmas
    holidays.add(date(year, 12, 25))
    return holidays


def get_next_expirations(n: int = 8) -> List[str]:
    """Return the next N option expiration dates as YYYY-MM-DD strings.

    SPY/QQQ have daily (0DTE) expirations every business day.
    We return every weekday (Mon-Fri) going forward, excluding US market holidays.
    """
    today = datetime.now().date()
    holidays = _us_market_holidays(today.year) | _us_market_holidays(today.year + 1)
    candidates = []
    d = today
    for _ in range(60):
        if d.weekday() < 5 and d not in holidays:  # Mon-Fri, not a holiday
            if d > today or (d == today and datetime.now().hour < 16):
                candidates.append(d.strftime("%Y-%m-%d"))
                if len(candidates) >= n:
                    break
        d += timedelta(days=1)
    return candidates


def generate_strike_range(center: float, num: int = 15, step: float = 1.0) -> List[float]:
    """Generate strikes centered on ATM."""
    half = num // 2
    return [round(center + i * step, 2) for i in range(-half, half + 1)]


def resolve_rics():
    """Resolve which RIC suffix works for each ticker by testing with rd.get_data().

    Some LSEG subscriptions return delayed data for certain exchange suffixes (e.g.
    .P = Primary) but real-time for others (e.g. .O = NASDAQ, .A = Arca). This
    function tests each candidate and picks the first one that returns valid data.
    """
    global EQUITY_RICS, RIC_TO_TICKER, TICKER_TO_RIC
    import refinitiv.data as rd

    resolved = {}
    for ticker, candidates in TICKER_RIC_CANDIDATES.items():
        best_ric = candidates[0]  # fallback to first candidate
        for ric in candidates:
            try:
                df = rd.get_data([ric], ["TRDPRC_1"])
                if df is not None and not df.empty:
                    val = _safe_float(df.iloc[0].get("TRDPRC_1") if hasattr(df.iloc[0], 'get') else df.iloc[0]["TRDPRC_1"])
                    if val and val > 0:
                        best_ric = ric
                        logger.info("  %s → %s (price=%.2f) ✓", ticker, ric, val)
                        break
            except Exception:
                continue
        else:
            logger.info("  %s → %s (fallback)", ticker, best_ric)
        resolved[ticker] = best_ric

    EQUITY_RICS = list(resolved.values())
    RIC_TO_TICKER = {v: k for k, v in resolved.items()}
    TICKER_TO_RIC = resolved
    logger.info("Resolved RICs: %s", ", ".join(f"{t}={r}" for t, r in resolved.items()))


def detect_delayed(quote_time_str) -> bool:
    """Detect if streaming data is delayed by comparing QUOTIM to wall clock.

    QUOTIM format is "HH:MM:SS" in exchange local time (ET for US equities).
    If the quote timestamp lags wall clock by more than ~60 seconds during
    market hours, we're almost certainly on a delayed feed.
    """
    if not quote_time_str:
        return False
    try:
        from datetime import timezone
        now_utc = datetime.now(timezone.utc)
        # US market time is ET (UTC-5 in winter, UTC-4 in summer)
        # Rough: subtract 5 hours from UTC for EST
        et_offset = timedelta(hours=-5)
        now_et = now_utc + et_offset
        # Parse quote time
        parts = str(quote_time_str).split(":")
        if len(parts) >= 2:
            qh, qm = int(parts[0]), int(parts[1])
            qs = int(parts[2]) if len(parts) > 2 else 0
            qt = now_et.replace(hour=qh, minute=qm, second=qs, microsecond=0)
            diff = abs((now_et - qt).total_seconds())
            # If quote is more than 5 minutes behind, it's delayed
            # (allowing slack for network + processing)
            if 300 < diff < 86000:  # 5 min to ~24h (avoid midnight wraparound)
                return True
    except Exception:
        pass
    return False


def is_market_open() -> bool:
    """Check if US equity markets are currently open (9:30-16:00 ET, Mon-Fri)."""
    try:
        from datetime import timezone
        now_utc = datetime.now(timezone.utc)
        et_offset = timedelta(hours=-5)
        now_et = now_utc + et_offset
        if now_et.weekday() > 4:  # Sat/Sun
            return False
        market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
        return market_open <= now_et <= market_close
    except Exception:
        return True  # Assume open on error


# ─── Shared State ────────────────────────────────────────────────────────────

clients: set = set()                     # connected browser WebSocket sessions
latest_quotes: Dict[str, dict] = {}      # ticker -> {last, bid, ask, vol, hi, lo, chg, pctChg}
latest_chains: Dict[str, Dict[str, dict]] = {}  # ticker -> {exp_date: {c:[...], p:[...]}}
latest_flow: Dict[str, dict] = {}        # ticker -> {cv, pv, pc, unusual:[...]}
stream_ref = None
is_delayed = False
event_loop = None                        # set in main(), used by sync callbacks


# ─── WebSocket Broadcast ─────────────────────────────────────────────────────

async def broadcast(msg: dict):
    """Send JSON message to all connected browser clients."""
    if not clients:
        return
    payload = json.dumps(msg, default=str)
    dead = set()
    for ws in clients:
        try:
            await ws.send(payload)
        except websockets.exceptions.ConnectionClosed:
            dead.add(ws)
        except Exception:
            dead.add(ws)
    clients.difference_update(dead)


def schedule_broadcast(msg: dict):
    """Thread-safe: schedule broadcast from sync LSEG callback."""
    if event_loop and event_loop.is_running():
        asyncio.run_coroutine_threadsafe(broadcast(msg), event_loop)


# ─── LSEG Session ────────────────────────────────────────────────────────────

def open_lseg_session():
    """Open refinitiv-data Desktop session."""
    import refinitiv.data as rd
    try:
        rd.open_session(app_key=APP_KEY)
        logger.info("LSEG Workspace session opened (app_key=%s...)", APP_KEY[:8])
        return True
    except Exception as e:
        logger.error("Failed to open LSEG session: %s", e)
        logger.error("Make sure LSEG Workspace is running and signed in.")
        return False


def close_lseg_session():
    """Close refinitiv-data session."""
    try:
        import refinitiv.data as rd
        rd.close_session()
        logger.info("LSEG session closed.")
    except Exception:
        pass


# ─── Equity Streaming ────────────────────────────────────────────────────────

_delay_check_count = 0
_delay_check_interval = 50  # check every N ticks to avoid spamming

def on_equity_tick(data, instrument, fields):
    """Callback on every equity price tick from LSEG.

    Called from LSEG's internal thread — must be thread-safe.
    `data` is a pandas DataFrame with instrument names as index.
    `fields` is a PricingStream object (not a dict).
    """
    global is_delayed, _delay_check_count

    def _f(name):
        try:
            if isinstance(fields, dict):
                return _safe_float(fields.get(name))
            # data is a DataFrame — extract via .loc[instrument][field]
            if hasattr(data, 'loc'):
                return _safe_float(data.loc[instrument][name])
            return _safe_float(data[instrument][name])
        except Exception:
            return None

    def _raw(name):
        """Get raw (non-float) field value (e.g. timestamp strings)."""
        try:
            if isinstance(fields, dict):
                return fields.get(name)
            if hasattr(data, 'loc'):
                return data.loc[instrument][name]
            return data[instrument][name]
        except Exception:
            return None

    ticker = RIC_TO_TICKER.get(instrument)
    if not ticker:
        return

    update = {
        "last": _f("TRDPRC_1"),
        "bid": _f("BID"),
        "ask": _f("ASK"),
        "vol": _f("ACVOL_1"),
        "hi": _f("HIGH_1"),
        "lo": _f("LOW_1"),
        "chg": _f("NETCHNG_1"),
        "pctChg": _f("PCTCHNG"),
    }

    # Merge into latest (only overwrite non-None fields)
    prev = latest_quotes.get(ticker, {})
    for k, v in update.items():
        if v is not None:
            prev[k] = v
    latest_quotes[ticker] = prev

    # ── Delayed / market-closed detection (periodic, not every tick) ──
    _delay_check_count += 1
    if _delay_check_count % _delay_check_interval == 1:
        quotim = _raw("QUOTIM")
        market_open = is_market_open()
        if quotim or not market_open:
            was_delayed = is_delayed
            is_delayed = detect_delayed(quotim) if quotim else is_delayed
            effectively_delayed = is_delayed or not market_open
            was_effectively = was_delayed or False  # simplified
            if effectively_delayed != was_effectively or (not market_open and _delay_check_count <= 2):
                status = "delayed" if effectively_delayed else "connected"
                if not market_open:
                    msg = "Market closed"
                elif is_delayed:
                    msg = "15-min delayed feed"
                else:
                    msg = "Real-time feed"
                logger.info("Feed status changed: %s (QUOTIM=%s)", status.upper(), quotim)
                schedule_broadcast({
                    "type": "status",
                    "status": status,
                    "msg": msg,
                })

    schedule_broadcast({
        "type": "quote_update",
        "ticker": ticker,
        "data": prev,
    })


def start_equity_stream():
    """Open streaming subscription for the 6 equity underlyings."""
    global stream_ref
    import refinitiv.data as rd

    try:
        stream_ref = rd.open_pricing_stream(
            universe=EQUITY_RICS,
            fields=EQUITY_FIELDS,
            on_data=on_equity_tick,
        )
        stream_ref.open()
        logger.info("Equity stream opened for %s", ", ".join(EQUITY_RICS))
        return True
    except Exception as e:
        logger.error("Failed to open equity stream: %s", e)
        return False


# ─── Options Chain Polling ────────────────────────────────────────────────────

def fetch_chain_snapshot(ticker: str, expiration: str) -> Optional[dict]:
    """Fetch options chain for a ticker/expiration via rd.get_data()."""
    import refinitiv.data as rd

    price = latest_quotes.get(ticker, {}).get("last")
    if not price:
        # Try a snapshot
        try:
            ric = TICKER_TO_RIC[ticker]
            df = rd.get_data([ric], ["TRDPRC_1"])
            if df is not None and not df.empty:
                rec = df.iloc[0]
                price = _safe_float(rec.get("TRDPRC_1") if hasattr(rec, 'get') else rec["TRDPRC_1"])
        except Exception as e:
            logger.debug("Snapshot price fetch failed for %s: %s", ticker, e)
    if not price:
        logger.warning("No price for %s, skipping chain", ticker)
        return None

    step, num = CHAIN_CFG.get(ticker, (5.0, 11))
    center = round(price / step) * step  # snap to nearest step
    strikes = generate_strike_range(center, num, step)

    # Build RICs
    underlying = ticker  # e.g. "SPY"
    call_rics = [build_option_ric(underlying, expiration, "C", s) for s in strikes]
    put_rics = [build_option_ric(underlying, expiration, "P", s) for s in strikes]

    all_rics = call_rics + put_rics
    logger.info("Fetching chain: %s exp=%s (%d RICs)", ticker, expiration, len(all_rics))
    try:
        df = rd.get_data(all_rics, OPTION_FIELDS)
    except Exception as e:
        logger.warning("Chain fetch failed for %s: %s", ticker, e)
        return None

    if df is None or df.empty:
        return None

    def parse_row(row, strike_val):
        return {
            "s": strike_val,
            "b": _safe_float(row.get("CF_BID")),
            "a": _safe_float(row.get("CF_ASK")),
            "l": _safe_float(row.get("CF_LAST")),
            "v": _safe_float(row.get("CF_VOLUME")),
            "d": _safe_float(row.get("DELTA")),
            "iv": _safe_float(row.get("IMPL_VOL")),
            "oi": _safe_float(row.get("OPEN_INT")),
        }

    records = df.to_dict(orient="records") if hasattr(df, "to_dict") else []

    calls, puts = [], []
    n = len(strikes)
    for i, rec in enumerate(records):
        # First n records are calls, next n are puts
        strike_idx = i if i < n else i - n
        strike_val = strikes[strike_idx] if strike_idx < len(strikes) else 0
        parsed = parse_row(rec, strike_val)
        if i < n:
            calls.append(parsed)
        else:
            puts.append(parsed)

    return {"c": calls, "p": puts}


async def poll_chains_loop():
    """Periodically fetch options chains for all tickers across multiple expirations."""
    CHAIN_EXPS = 5  # Fetch chains for nearest N expirations (5 × 9 tickers = 45 fetches)

    while True:
        try:
            all_expirations = get_next_expirations(8)
            chain_expirations = all_expirations[:CHAIN_EXPS]
            tickers = list(RIC_TO_TICKER.values())
            logger.info("Chain poll: %d expirations × %d tickers = %d fetches",
                        len(chain_expirations), len(tickers), len(chain_expirations) * len(tickers))

            for exp in chain_expirations:
                for ticker in tickers:
                    try:
                        chain = await asyncio.wait_for(
                            asyncio.to_thread(fetch_chain_snapshot, ticker, exp),
                            timeout=15
                        )
                    except asyncio.TimeoutError:
                        logger.warning("Chain fetch timed out: %s %s", ticker, exp)
                        chain = None
                    if chain:
                        if ticker not in latest_chains:
                            latest_chains[ticker] = {}
                        latest_chains[ticker][exp] = chain
                        schedule_broadcast({
                            "type": "chain_update",
                            "ticker": ticker,
                            "expiration": exp,
                            "data": chain,
                        })
                logger.info("Chains fetched for %s (%d tickers)", exp, len(tickers))

            # Broadcast only the expirations we fetched chains for (not extras with no data)
            schedule_broadcast({
                "type": "expirations_update",
                "expirations": chain_expirations,
            })
            logger.info("Chain poll complete — %d expirations broadcast", len(chain_expirations))

        except Exception as e:
            logger.error("Chain poll error: %s", e, exc_info=True)

        await asyncio.sleep(POLL_CHAINS_SEC)


# ─── Flow / Unusual Activity Polling ─────────────────────────────────────────

def compute_flow(ticker: str) -> Optional[dict]:
    """Compute flow stats from latest chain data (scans ALL expirations)."""
    chain_exps = latest_chains.get(ticker, {})
    if not chain_exps:
        return None

    cv, pv = 0, 0
    unusual = []

    for exp_date, chain in chain_exps.items():
        if not chain:
            continue

        for c in chain.get("c", []):
            vol = c.get("v") or 0
            last = c.get("l") or 0
            delta = c.get("d") or 0
            oi = c.get("oi")
            cv += vol
            if vol > 0 and last > 0:
                unusual.append({
                    "s": c["s"], "t": "C", "v": int(vol),
                    "l": last, "d": round(delta, 4),
                    "exp": exp_date, "oi": oi,
                })

        for p in chain.get("p", []):
            vol = p.get("v") or 0
            last = p.get("l") or 0
            delta = p.get("d") or 0
            oi = p.get("oi")
            pv += vol
            if vol > 0 and last > 0:
                unusual.append({
                    "s": p["s"], "t": "P", "v": int(vol),
                    "l": last, "d": round(delta, 4),
                    "exp": exp_date, "oi": oi,
                })

    # Sort by premium descending, take top 20
    unusual.sort(key=lambda x: x["v"] * x["l"] * 100, reverse=True)
    unusual = unusual[:20]

    pc = round(pv / cv, 3) if cv > 0 else 0.0

    return {
        "cv": int(cv), "pv": int(pv), "pc": pc,
        "unusual": unusual,
    }


async def poll_flow_loop():
    """Periodically compute and broadcast flow stats."""
    while True:
        try:
            for ticker in RIC_TO_TICKER.values():
                flow = await asyncio.to_thread(compute_flow, ticker)
                if flow:
                    latest_flow[ticker] = flow
                    schedule_broadcast({
                        "type": "flow_update",
                        "ticker": ticker,
                        "data": flow,
                    })
        except Exception as e:
            logger.error("Flow poll error: %s", e)

        await asyncio.sleep(POLL_FLOW_SEC)


# ─── WebSocket Server ─────────────────────────────────────────────────────────

async def ws_handler(websocket):
    """Handle a new browser WebSocket connection."""
    clients.add(websocket)
    remote = websocket.remote_address
    logger.info("Browser connected: %s", remote)

    # Send initial snapshot
    try:
        # Build expirations list — only send expirations we actually fetch chains for
        all_exps = get_next_expirations(5)

        # Determine initial status
        init_status = "delayed" if (is_delayed or not is_market_open()) else "connected"
        init_msg = "Market closed" if not is_market_open() else ("15-min delayed feed" if is_delayed else "Real-time feed")

        snapshot = {
            "type": "snapshot",
            "quotes": latest_quotes,
            "chains": dict(latest_chains),
            "expirations": all_exps,
            "flow": latest_flow,
            "status": init_status,
            "msg": init_msg,
        }
        await websocket.send(json.dumps(snapshot, default=str))
    except Exception:
        pass

    # Keep alive — just listen for messages (heartbeats, commands)
    try:
        async for message in websocket:
            # Future: handle browser commands (change expiration, etc.)
            try:
                msg = json.loads(message)
                logger.debug("Browser message: %s", msg)
            except json.JSONDecodeError:
                pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clients.discard(websocket)
        logger.info("Browser disconnected: %s", remote)


# ─── HTTP file server (for Safari / cross-browser support) ────────────────────

def start_http_server(directory: str, port: int):
    """Start a simple HTTP file server in a daemon thread."""
    import http.server
    import threading

    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=directory, **kwargs)
        def end_headers(self):
            self.send_header('Access-Control-Allow-Origin', '*')
            super().end_headers()
        def log_message(self, fmt, *args):
            pass  # Suppress noisy per-request logs

    server = http.server.HTTPServer(("localhost", port), QuietHandler)
    server.daemon_threads = True
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    global event_loop
    event_loop = asyncio.get_running_loop()

    logger.info("=" * 60)
    logger.info("  FLOW TERMINAL — Local Bridge")
    logger.info("  Connecting to LSEG Workspace...")
    logger.info("=" * 60)

    # 1. Open LSEG session
    if not open_lseg_session():
        logger.error("Cannot start without LSEG Workspace. Exiting.")
        sys.exit(1)

    # 1b. Resolve best RIC suffix for each ticker (real-time vs delayed)
    logger.info("Resolving RIC entitlements...")
    resolve_rics()

    if not EQUITY_RICS:
        logger.error("No RICs could be resolved. Check LSEG Workspace connection.")
        close_lseg_session()
        sys.exit(1)

    # 2. Start equity stream
    if not start_equity_stream():
        logger.warning("Equity stream failed — will retry via polling.")

    # 3. Start background polling tasks
    chain_task = asyncio.create_task(poll_chains_loop())
    flow_task = asyncio.create_task(poll_flow_loop())

    # 4. Start WebSocket server
    logger.info("Starting WebSocket server on ws://localhost:%d", WS_PORT)
    stop_event = asyncio.Event()

    # Graceful shutdown
    def _shutdown():
        logger.info("Shutting down...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        event_loop.add_signal_handler(sig, _shutdown)

    # 5. Start HTTP file server (so Safari and other browsers can connect)
    http_dir = os.path.dirname(os.path.abspath(__file__))
    http_srv = start_http_server(http_dir, HTTP_PORT)

    async with websockets.serve(ws_handler, "localhost", WS_PORT, origins=None):
        logger.info("")
        logger.info("  Bridge ready!")
        logger.info("  WebSocket:  ws://localhost:%d", WS_PORT)
        logger.info("  Dashboard:  http://localhost:%d/options-flow-dashboard.html", HTTP_PORT)
        logger.info("  Press Ctrl+C to stop.")
        logger.info("")
        await stop_event.wait()

    # Cleanup
    chain_task.cancel()
    flow_task.cancel()
    if stream_ref:
        try:
            stream_ref.close()
        except Exception:
            pass
    close_lseg_session()
    logger.info("Bridge stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
