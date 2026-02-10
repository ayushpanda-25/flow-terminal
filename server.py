#!/usr/bin/env python3
"""
Flow Terminal — FastAPI Cloud Relay Server (Mode 2)

For users without LSEG Desktop running locally. Each connected browser client
sends their own LSEG Platform credentials over WebSocket. The server authenticates
on their behalf, opens streaming subscriptions, and relays data back.

NO credentials are stored anywhere — memory only, garbage collected on disconnect.

Usage:
    pip install -r requirements.txt
    python server.py
    # Then open options-flow-dashboard.html → select "Server Mode"
    # Enter server URL: ws://localhost:8000/ws
"""

import asyncio
import json
import logging
import math
import os
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
import uvicorn

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
    pass

import refinitiv.data as rd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("flow_server")

# ─── Config ──────────────────────────────────────────────────────────────────

HOST = os.environ.get("FLOW_HOST", "0.0.0.0")
PORT = int(os.environ.get("FLOW_PORT", "8000"))

# ── RIC resolution candidates (same as local_bridge.py) ──
TICKER_RIC_CANDIDATES = {
    "SPY":  ["SPY.A", "SPY.P", "SPY"],
    "AAPL": ["AAPL.O", "AAPL.OQ", "AAPL.P"],
    "NVDA": ["NVDA.O", "NVDA.OQ", "NVDA.P"],
    "TSLA": ["TSLA.O", "TSLA.OQ", "TSLA.P"],
    "QQQ":  ["QQQ.O", "QQQ.OQ", "QQQ.P"],
    "AMZN": ["AMZN.O", "AMZN.OQ", "AMZN.P"],
}

# Default RICs (will be resolved per-user session using their entitlements)
EQUITY_RICS = ["SPY.A", "AAPL.O", "NVDA.O", "TSLA.O", "QQQ.O", "AMZN.O"]
RIC_TO_TICKER = {
    "SPY.A": "SPY", "AAPL.O": "AAPL", "NVDA.O": "NVDA",
    "TSLA.O": "TSLA", "QQQ.O": "QQQ", "AMZN.O": "AMZN",
}
TICKER_TO_RIC = {v: k for k, v in RIC_TO_TICKER.items()}

EQUITY_FIELDS = [
    "BID", "ASK", "TRDPRC_1", "ACVOL_1",
    "HIGH_1", "LOW_1", "NETCHNG_1", "PCTCHNG",
    "QUOTIM", "TRADE_DATE",  # For delayed-vs-RT detection
]
OPTION_FIELDS = [
    "CF_BID", "CF_ASK", "CF_LAST", "CF_VOLUME",
    "OPEN_INT", "IMPL_VOL", "DELTA",
]

CHAIN_CFG = {
    "SPY": (1.0, 15), "QQQ": (1.0, 15),
    "AAPL": (5.0, 11), "NVDA": (5.0, 11),
    "TSLA": (5.0, 11), "AMZN": (5.0, 11),
}

POLL_CHAINS_SEC = 30
POLL_FLOW_SEC = 15

# ─── Utility Functions ───────────────────────────────────────────────────────

def _safe_float(val) -> Optional[float]:
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
        return None if f != f else f
    except (TypeError, ValueError):
        return None


def build_option_ric(underlying: str, expiration: str, option_type: str, strike: float) -> str:
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


def get_next_expirations(n: int = 3) -> List[str]:
    today = datetime.now().date()
    candidates = []
    d = today
    for _ in range(30):
        if d.weekday() in (0, 2, 4):
            if d > today or (d == today and datetime.now().hour < 16):
                candidates.append(d.strftime("%Y-%m-%d"))
                if len(candidates) >= n:
                    break
        d += timedelta(days=1)
    return candidates


def generate_strike_range(center: float, num: int = 15, step: float = 1.0) -> List[float]:
    half = num // 2
    return [round(center + i * step, 2) for i in range(-half, half + 1)]


# ─── Per-User Session ────────────────────────────────────────────────────────

class UserSession:
    """Isolated LSEG session + data state for a single connected browser client."""

    def __init__(self, websocket: WebSocket, app_key: str, username: str, password: str):
        self.ws = websocket
        self.app_key = app_key
        self.username = username
        self.password = password
        self.session = None
        self.stream_ref = None
        self.latest_quotes: Dict[str, dict] = {}
        self.latest_chains: Dict[str, dict] = {}
        self.latest_flow: Dict[str, dict] = {}
        self.is_delayed = False
        self._running = True
        self._loop = None  # set in run() via get_running_loop()
        self._tasks: List[asyncio.Task] = []
        self._cleaned_up = False
        self._rics: list = list(EQUITY_RICS)
        self._ric_to_ticker: dict = dict(RIC_TO_TICKER)
        self._ticker_to_ric: dict = dict(TICKER_TO_RIC)

    async def send(self, msg: dict):
        """Send JSON message to this user's browser."""
        try:
            await self.ws.send_json(msg)
        except Exception:
            pass

    def _schedule_send(self, msg: dict):
        """Thread-safe: schedule send from sync LSEG callback."""
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self.send(msg), self._loop)

    def open_lseg(self) -> bool:
        """Open a Platform session with the user's credentials."""
        try:
            self.session = rd.session.platform.Definition(
                app_key=self.app_key,
                grant=rd.session.platform.GrantPassword(
                    username=self.username,
                    password=self.password,
                ),
                deployed_platform_host="",
            ).get_session()
            self.session.open()
            logger.info("Platform session opened for user %s", self.username[:6] + "***")
            return True
        except Exception as e:
            logger.error("Failed to open platform session for %s: %s", self.username[:6] + "***", e)
            return False

    def start_equity_stream(self) -> bool:
        """Open streaming subscription for equities using this user's session."""
        try:
            self.stream_ref = rd.content.pricing.Definition(
                universe=self._rics,
                fields=EQUITY_FIELDS,
            ).get_stream(self.session)

            self.stream_ref.on_update(self._on_equity_tick)
            self.stream_ref.open()
            logger.info("Equity stream opened for %s", self.username[:6] + "***")
            return True
        except Exception as e:
            logger.error("Equity stream failed: %s", e)
            return False

    def _on_equity_tick(self, streaming, instrument, fields):
        """Callback from LSEG streaming — fires on their internal thread."""
        def _f(name):
            try:
                if isinstance(fields, dict):
                    return _safe_float(fields.get(name))
                if hasattr(fields, '__getitem__'):
                    return _safe_float(fields[name])
                return None
            except Exception:
                return None

        ticker = self._ric_to_ticker.get(instrument)
        if not ticker:
            return

        update = {
            "last": _f("TRDPRC_1"), "bid": _f("BID"), "ask": _f("ASK"),
            "vol": _f("ACVOL_1"), "hi": _f("HIGH_1"), "lo": _f("LOW_1"),
            "chg": _f("NETCHNG_1"), "pctChg": _f("PCTCHNG"),
        }

        prev = self.latest_quotes.get(ticker, {})
        for k, v in update.items():
            if v is not None:
                prev[k] = v
        self.latest_quotes[ticker] = prev

        self._schedule_send({
            "type": "quote_update",
            "ticker": ticker,
            "data": prev,
        })

    def fetch_chain_snapshot(self, ticker: str, expiration: str) -> Optional[dict]:
        """Fetch options chain using this user's session."""
        price = self.latest_quotes.get(ticker, {}).get("last")
        if not price:
            try:
                ric = self._ticker_to_ric.get(ticker, ticker)
                df = rd.get_data([ric], ["TRDPRC_1"], session=self.session)
                if df is not None and not df.empty:
                    rec = df.iloc[0]
                    price = _safe_float(rec.get("TRDPRC_1") if hasattr(rec, 'get') else rec["TRDPRC_1"])
            except Exception:
                pass
        if not price:
            return None

        step, num = CHAIN_CFG.get(ticker, (5.0, 11))
        center = round(price / step) * step
        strikes = generate_strike_range(center, num, step)

        call_rics = [build_option_ric(ticker, expiration, "C", s) for s in strikes]
        put_rics = [build_option_ric(ticker, expiration, "P", s) for s in strikes]

        try:
            df = rd.get_data(call_rics + put_rics, OPTION_FIELDS, session=self.session)
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
            strike_idx = i if i < n else i - n
            strike_val = strikes[strike_idx] if strike_idx < len(strikes) else 0
            parsed = parse_row(rec, strike_val)
            if i < n:
                calls.append(parsed)
            else:
                puts.append(parsed)

        return {"c": calls, "p": puts}

    def compute_flow(self, ticker: str) -> Optional[dict]:
        chain = self.latest_chains.get(ticker, {}).get("data")
        if not chain:
            return None

        cv, pv = 0, 0
        unusual = []

        for c in chain.get("c", []):
            vol = c.get("v") or 0
            last = c.get("l") or 0
            delta = c.get("d") or 0
            cv += vol
            if vol > 0 and last > 0:
                unusual.append({"s": c["s"], "t": "C", "v": int(vol), "l": last, "d": round(delta, 4)})

        for p in chain.get("p", []):
            vol = p.get("v") or 0
            last = p.get("l") or 0
            delta = p.get("d") or 0
            pv += vol
            if vol > 0 and last > 0:
                unusual.append({"s": p["s"], "t": "P", "v": int(vol), "l": last, "d": round(delta, 4)})

        unusual.sort(key=lambda x: x["v"] * x["l"] * 100, reverse=True)
        unusual = unusual[:20]
        pc = round(pv / cv, 3) if cv > 0 else 0.0

        return {"cv": int(cv), "pv": int(pv), "pc": pc, "unusual": unusual}

    async def poll_chains_loop(self):
        expirations = get_next_expirations(3)
        exp = expirations[0] if expirations else "2026-02-14"

        while self._running:
            try:
                expirations = get_next_expirations(3)
                if expirations:
                    exp = expirations[0]

                for ticker in self._ric_to_ticker.values():
                    if not self._running:
                        break
                    chain = await asyncio.to_thread(self.fetch_chain_snapshot, ticker, exp)
                    if chain:
                        self.latest_chains[ticker] = {"expiration": exp, "data": chain}
                        await self.send({
                            "type": "chain_update",
                            "ticker": ticker,
                            "expiration": exp,
                            "data": chain,
                        })

                # Also send multi-exp data
                await self.send({
                    "type": "expirations_update",
                    "expirations": expirations,
                })

            except Exception as e:
                logger.error("Chain poll error for %s: %s", self.username[:6] + "***", e)

            await asyncio.sleep(POLL_CHAINS_SEC)

    async def poll_flow_loop(self):
        while self._running:
            try:
                for ticker in self._ric_to_ticker.values():
                    if not self._running:
                        break
                    flow = await asyncio.to_thread(self.compute_flow, ticker)
                    if flow:
                        self.latest_flow[ticker] = flow
                        await self.send({
                            "type": "flow_update",
                            "ticker": ticker,
                            "data": flow,
                        })
            except Exception as e:
                logger.error("Flow poll error for %s: %s", self.username[:6] + "***", e)

            await asyncio.sleep(POLL_FLOW_SEC)

    def resolve_rics(self):
        """Resolve best RIC suffix for each ticker using this user's session."""
        resolved = {}
        for ticker, candidates in TICKER_RIC_CANDIDATES.items():
            best_ric = candidates[0]
            for ric in candidates:
                try:
                    df = rd.get_data([ric], ["TRDPRC_1"], session=self.session)
                    if df is not None and not df.empty:
                        val = _safe_float(df.iloc[0].get("TRDPRC_1") if hasattr(df.iloc[0], 'get') else df.iloc[0]["TRDPRC_1"])
                        if val and val > 0:
                            best_ric = ric
                            break
                except Exception:
                    continue
            resolved[ticker] = best_ric

        self._rics = list(resolved.values())
        self._ric_to_ticker = {v: k for k, v in resolved.items()}
        self._ticker_to_ric = resolved
        logger.info("Resolved RICs for %s: %s", self.username[:6] + "***",
                     ", ".join(f"{t}={r}" for t, r in resolved.items()))

    async def run(self):
        """Main lifecycle: auth → stream → poll → cleanup."""
        self._loop = asyncio.get_running_loop()

        # 1. Open LSEG platform session
        success = await asyncio.to_thread(self.open_lseg)
        if not success:
            await self.send({"type": "error", "msg": "LSEG authentication failed. Check credentials."})
            return

        await self.send({"type": "status", "status": "connected", "msg": "Authenticated to LSEG Platform"})

        # 1b. Resolve best RIC suffixes for this user's entitlements
        await asyncio.to_thread(self.resolve_rics)

        # 2. Start equity streaming
        await asyncio.to_thread(self.start_equity_stream)

        # 3. Start polling tasks
        chain_task = asyncio.create_task(self.poll_chains_loop())
        flow_task = asyncio.create_task(self.poll_flow_loop())
        self._tasks = [chain_task, flow_task]

        # 4. Send initial snapshot after a brief delay for data to flow
        await asyncio.sleep(3)
        await self.send({
            "type": "snapshot",
            "quotes": self.latest_quotes,
            "chains": {tk: ch.get("data", {}) for tk, ch in self.latest_chains.items()},
            "expirations": {tk: ch.get("expiration", "") for tk, ch in self.latest_chains.items()},
            "flow": self.latest_flow,
            "status": "delayed" if self.is_delayed else "connected",
        })

        # 5. Keep alive — listen for browser messages
        try:
            while True:
                msg = await self.ws.receive_text()
                try:
                    data = json.loads(msg)
                    logger.debug("Browser msg from %s: %s", self.username[:6] + "***", data)
                except json.JSONDecodeError:
                    pass
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Close everything. Credentials garbage collected after this."""
        if self._cleaned_up:
            return
        self._cleaned_up = True
        self._running = False
        for task in self._tasks:
            task.cancel()

        if self.stream_ref:
            try:
                self.stream_ref.close()
            except Exception:
                pass

        if self.session:
            try:
                self.session.close()
            except Exception:
                pass

        logger.info("Session closed and cleaned up for %s", self.username[:6] + "***")


# ─── FastAPI App ─────────────────────────────────────────────────────────────

app = FastAPI(title="Flow Terminal Relay Server")


@app.get("/health")
async def health():
    return JSONResponse({"status": "ok", "time": datetime.now().isoformat()})


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("Browser connected: %s", websocket.client)

    # Step 1: Receive credentials from browser
    try:
        creds = await asyncio.wait_for(websocket.receive_json(), timeout=30)
    except asyncio.TimeoutError:
        await websocket.send_json({"type": "error", "msg": "Timeout waiting for credentials."})
        await websocket.close()
        return
    except Exception:
        await websocket.close()
        return

    app_key = creds.get("app_key", "")
    username = creds.get("username", "")
    password = creds.get("password", "")

    if not app_key or not username or not password:
        await websocket.send_json({"type": "error", "msg": "Missing credentials. Need app_key, username, password."})
        await websocket.close()
        return

    # Step 2: Create isolated user session
    user_session = UserSession(websocket, app_key, username, password)

    try:
        await user_session.run()
    except Exception as e:
        logger.error("User session error: %s", e)
    finally:
        await user_session.cleanup()
        # Credentials are now garbage collected
        del user_session
        logger.info("Browser disconnected: %s", websocket.client)


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("  FLOW TERMINAL — Cloud Relay Server")
    logger.info("  Listening on %s:%d", HOST, PORT)
    logger.info("  WebSocket endpoint: ws://%s:%d/ws", HOST, PORT)
    logger.info("=" * 60)

    uvicorn.run(
        "server:app",
        host=HOST,
        port=PORT,
        log_level="info",
        ws_ping_interval=20,
        ws_ping_timeout=20,
    )
