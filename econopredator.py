# ===================================================================
# APEX ECONOPREDATOR INGESTION NODE v3.0 (Port 8000)
# Centralized data ingestion: funding, OI, long/short ratio,
# macro indicators (DXY/VIX/US10Y), on-chain flows, whale alerts, ATR
# ===================================================================

from __future__ import annotations

import asyncio
import math
import os
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import httpx
import numpy as np
from dotenv import load_dotenv
from fastapi import FastAPI

from apex_common.logging import get_logger
from apex_common.metrics import instrument_app
from apex_common.security import check_env_file_permissions

load_dotenv()
log = get_logger("econopredator")
_ok, _msg = check_env_file_permissions(".env")
if not _ok:
    log.warning(_msg)

# ────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────
def _env(n: str, d: str) -> str:
    return os.getenv(n, d)

def _f(n: str, d: float) -> float:
    try:
        return float(os.getenv(n, str(d)))
    except Exception:
        return d

def _i(n: str, d: int) -> int:
    try:
        return int(os.getenv(n, str(d)))
    except Exception:
        return d


BINANCE_FAPI = _env("ECONO_BINANCE_FAPI", "https://fapi.binance.com")
DEFAULT_SYMBOLS = [s.strip().upper() for s in _env("ECONO_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]
FUNDING_POLL_S = _f("ECONO_FUNDING_POLL_S", 60.0)
OI_POLL_S = _f("ECONO_OI_POLL_S", 30.0)
LS_RATIO_POLL_S = _f("ECONO_LS_RATIO_POLL_S", 300.0)
MACRO_POLL_S = _f("ECONO_MACRO_POLL_S", 900.0)
ATR_POLL_S = _f("ECONO_ATR_POLL_S", 60.0)
ATR_PERIOD = _i("ECONO_ATR_PERIOD", 14)
ATR_KLINE_INTERVAL = _env("ECONO_ATR_KLINE_INTERVAL", "1h")
GLASSNODE_API_KEY = _env("GLASSNODE_API_KEY", "").strip()
WHALE_ALERT_KEY = _env("WHALE_ALERT_KEY", "").strip()
ONCHAIN_POLL_S = _f("ECONO_ONCHAIN_POLL_S", 600.0)


# ────────────────────────────────────────────────────
# Data store
# ────────────────────────────────────────────────────
@dataclass
class FundingSnapshot:
    symbol: str
    mark_price: float = 0.0
    funding_rate: float = 0.0
    next_funding_time: int = 0
    ts: float = 0.0


@dataclass
class OISnapshot:
    symbol: str
    open_interest: float = 0.0
    open_interest_value: float = 0.0
    ts: float = 0.0


@dataclass
class LSRatioSnapshot:
    symbol: str
    long_account: float = 0.5
    short_account: float = 0.5
    long_short_ratio: float = 1.0
    ts: float = 0.0


@dataclass
class ATRData:
    symbol: str
    atr: float = 0.0
    atr_pct: float = 0.0  # ATR as % of current price
    current_price: float = 0.0
    period: int = 14
    interval: str = "1h"
    ts: float = 0.0


@dataclass
class MacroSnapshot:
    dxy: float = 0.0
    us10y: float = 0.0
    vix: float = 0.0
    sp500: float = 0.0
    ts: float = 0.0


@dataclass
class OnChainSnapshot:
    symbol: str
    exchange_netflow_btc: float = 0.0
    exchange_reserve_btc: float = 0.0
    whale_tx_count_1h: int = 0
    whale_total_usd_1h: float = 0.0
    ts: float = 0.0


class DataStore:
    """Thread-safe centralized data store for all ingested data."""

    def __init__(self):
        self._lock = asyncio.Lock()
        self.funding: Dict[str, FundingSnapshot] = {}
        self.oi: Dict[str, OISnapshot] = {}
        self.ls_ratio: Dict[str, LSRatioSnapshot] = {}
        self.atr: Dict[str, ATRData] = {}
        self.macro: MacroSnapshot = MacroSnapshot()
        self.onchain: Dict[str, OnChainSnapshot] = {}
        self.funding_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.oi_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=200))

    async def update_funding(self, sym: str, snap: FundingSnapshot):
        async with self._lock:
            self.funding[sym] = snap
            self.funding_history[sym].append(snap)

    async def update_oi(self, sym: str, snap: OISnapshot):
        async with self._lock:
            self.oi[sym] = snap
            self.oi_history[sym].append(snap)

    async def update_ls_ratio(self, sym: str, snap: LSRatioSnapshot):
        async with self._lock:
            self.ls_ratio[sym] = snap

    async def update_atr(self, sym: str, data: ATRData):
        async with self._lock:
            self.atr[sym] = data

    async def update_macro(self, snap: MacroSnapshot):
        async with self._lock:
            self.macro = snap

    async def update_onchain(self, sym: str, snap: OnChainSnapshot):
        async with self._lock:
            self.onchain[sym] = snap

    async def get_market_data(self, sym: str) -> dict:
        async with self._lock:
            f = self.funding.get(sym)
            o = self.oi.get(sym)
            ls = self.ls_ratio.get(sym)
            a = self.atr.get(sym)
            oc = self.onchain.get(sym)

            # OI delta (last 10 snapshots)
            oi_delta = 0.0
            oi_hist = list(self.oi_history.get(sym, []))
            if len(oi_hist) >= 2:
                oi_delta = oi_hist[-1].open_interest - oi_hist[-2].open_interest

            return {
                "symbol": sym,
                "funding": {
                    "mark_price": f.mark_price if f else 0.0,
                    "funding_rate": f.funding_rate if f else 0.0,
                    "next_funding_time": f.next_funding_time if f else 0,
                } if f else None,
                "open_interest": {
                    "oi": o.open_interest if o else 0.0,
                    "oi_value_usd": o.open_interest_value if o else 0.0,
                    "oi_delta": oi_delta,
                } if o else None,
                "long_short_ratio": {
                    "long_account": ls.long_account if ls else 0.5,
                    "short_account": ls.short_account if ls else 0.5,
                    "ratio": ls.long_short_ratio if ls else 1.0,
                } if ls else None,
                "atr": {
                    "value": a.atr if a else 0.0,
                    "pct": a.atr_pct if a else 0.0,
                    "price": a.current_price if a else 0.0,
                    "period": a.period if a else ATR_PERIOD,
                    "interval": a.interval if a else ATR_KLINE_INTERVAL,
                } if a else None,
                "onchain": {
                    "exchange_netflow": oc.exchange_netflow_btc if oc else 0.0,
                    "exchange_reserve": oc.exchange_reserve_btc if oc else 0.0,
                    "whale_tx_count_1h": oc.whale_tx_count_1h if oc else 0,
                    "whale_total_usd_1h": oc.whale_total_usd_1h if oc else 0.0,
                } if oc else None,
            }

    async def get_funding_heatmap(self) -> dict:
        async with self._lock:
            return {
                sym: {
                    "funding_rate": snap.funding_rate,
                    "mark_price": snap.mark_price,
                    "intensity": "HIGH" if abs(snap.funding_rate) > 0.0005 else "MED" if abs(snap.funding_rate) > 0.0002 else "LOW",
                }
                for sym, snap in self.funding.items()
            }

    async def get_macro(self) -> dict:
        async with self._lock:
            m = self.macro
            return {
                "dxy": m.dxy,
                "us10y": m.us10y,
                "vix": m.vix,
                "sp500": m.sp500,
                "ts": m.ts,
                "risk_environment": (
                    "RISK_OFF" if m.vix > 25 else
                    "CAUTIOUS" if m.vix > 18 else
                    "RISK_ON"
                ),
            }

    async def get_atr(self, sym: str) -> dict:
        async with self._lock:
            a = self.atr.get(sym)
            if not a:
                return {"symbol": sym, "atr": 0.0, "error": "no data"}
            return {
                "symbol": sym,
                "atr": a.atr,
                "atr_pct": a.atr_pct,
                "price": a.current_price,
                "period": a.period,
                "interval": a.interval,
            }


store = DataStore()


# ────────────────────────────────────────────────────
# ATR computation
# ────────────────────────────────────────────────────
def compute_atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    """Compute Average True Range (ATR) using Wilder's smoothing."""
    n = len(highs)
    if n < period + 1 or n != len(lows) or n != len(closes):
        return 0.0

    true_ranges = []
    for i in range(1, n):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i - 1])
        lc = abs(lows[i] - closes[i - 1])
        true_ranges.append(max(hl, hc, lc))

    if len(true_ranges) < period:
        return 0.0

    # Wilder smoothing: first ATR is SMA, then EMA-like
    atr = sum(true_ranges[:period]) / period
    for tr in true_ranges[period:]:
        atr = (atr * (period - 1) + tr) / period

    return atr


# ────────────────────────────────────────────────────
# Pollers
# ────────────────────────────────────────────────────
http_client: httpx.AsyncClient | None = None


async def poll_funding(stop: asyncio.Event):
    """Poll Binance premiumIndex for funding rates."""
    while not stop.is_set():
        try:
            if http_client:
                for sym in DEFAULT_SYMBOLS:
                    try:
                        r = await http_client.get(
                            f"{BINANCE_FAPI}/fapi/v1/premiumIndex",
                            params={"symbol": sym},
                            timeout=5.0,
                        )
                        r.raise_for_status()
                        d = r.json()
                        await store.update_funding(sym, FundingSnapshot(
                            symbol=sym,
                            mark_price=float(d.get("markPrice", 0)),
                            funding_rate=float(d.get("lastFundingRate", 0)),
                            next_funding_time=int(d.get("nextFundingTime", 0)),
                            ts=time.time(),
                        ))
                    except Exception as e:
                        log.warning(f"funding poll {sym}: {e}")
        except Exception as e:
            log.error(f"funding poller error: {e}")
        try:
            await asyncio.wait_for(stop.wait(), timeout=FUNDING_POLL_S)
            break
        except asyncio.TimeoutError:
            pass


async def poll_oi(stop: asyncio.Event):
    """Poll Binance open interest."""
    while not stop.is_set():
        try:
            if http_client:
                for sym in DEFAULT_SYMBOLS:
                    try:
                        r = await http_client.get(
                            f"{BINANCE_FAPI}/fapi/v1/openInterest",
                            params={"symbol": sym},
                            timeout=5.0,
                        )
                        r.raise_for_status()
                        d = r.json()
                        oi = float(d.get("openInterest", 0))
                        # Get mark price for value
                        f = store.funding.get(sym)
                        price = f.mark_price if f else 0.0
                        await store.update_oi(sym, OISnapshot(
                            symbol=sym,
                            open_interest=oi,
                            open_interest_value=oi * price,
                            ts=time.time(),
                        ))
                    except Exception as e:
                        log.warning(f"OI poll {sym}: {e}")
        except Exception as e:
            log.error(f"OI poller error: {e}")
        try:
            await asyncio.wait_for(stop.wait(), timeout=OI_POLL_S)
            break
        except asyncio.TimeoutError:
            pass


async def poll_ls_ratio(stop: asyncio.Event):
    """Poll Binance global long/short account ratio."""
    while not stop.is_set():
        try:
            if http_client:
                for sym in DEFAULT_SYMBOLS:
                    try:
                        r = await http_client.get(
                            f"{BINANCE_FAPI}/futures/data/globalLongShortAccountRatio",
                            params={"symbol": sym, "period": "5m", "limit": 1},
                            timeout=5.0,
                        )
                        r.raise_for_status()
                        data = r.json()
                        if data:
                            d = data[0]
                            await store.update_ls_ratio(sym, LSRatioSnapshot(
                                symbol=sym,
                                long_account=float(d.get("longAccount", 0.5)),
                                short_account=float(d.get("shortAccount", 0.5)),
                                long_short_ratio=float(d.get("longShortRatio", 1.0)),
                                ts=time.time(),
                            ))
                    except Exception as e:
                        log.warning(f"LS ratio poll {sym}: {e}")
        except Exception as e:
            log.error(f"LS ratio poller error: {e}")
        try:
            await asyncio.wait_for(stop.wait(), timeout=LS_RATIO_POLL_S)
            break
        except asyncio.TimeoutError:
            pass


async def poll_atr(stop: asyncio.Event):
    """Poll Binance klines and compute ATR."""
    while not stop.is_set():
        try:
            if http_client:
                for sym in DEFAULT_SYMBOLS:
                    try:
                        r = await http_client.get(
                            f"{BINANCE_FAPI}/fapi/v1/klines",
                            params={"symbol": sym, "interval": ATR_KLINE_INTERVAL, "limit": ATR_PERIOD + 5},
                            timeout=5.0,
                        )
                        r.raise_for_status()
                        klines = r.json()
                        if len(klines) >= ATR_PERIOD + 1:
                            highs = [float(k[2]) for k in klines]
                            lows = [float(k[3]) for k in klines]
                            closes = [float(k[4]) for k in klines]
                            atr_val = compute_atr(highs, lows, closes, ATR_PERIOD)
                            last_price = closes[-1]
                            atr_pct = (atr_val / last_price * 100) if last_price > 0 else 0.0
                            await store.update_atr(sym, ATRData(
                                symbol=sym,
                                atr=atr_val,
                                atr_pct=atr_pct,
                                current_price=last_price,
                                period=ATR_PERIOD,
                                interval=ATR_KLINE_INTERVAL,
                                ts=time.time(),
                            ))
                    except Exception as e:
                        log.warning(f"ATR poll {sym}: {e}")
        except Exception as e:
            log.error(f"ATR poller error: {e}")
        try:
            await asyncio.wait_for(stop.wait(), timeout=ATR_POLL_S)
            break
        except asyncio.TimeoutError:
            pass


async def poll_macro(stop: asyncio.Event):
    """Poll macro indicators via Yahoo Finance fallback (DXY, VIX, US10Y, SPX)."""
    TICKERS = {"DXY": "DX-Y.NYB", "VIX": "^VIX", "US10Y": "^TNX", "SP500": "^GSPC"}
    while not stop.is_set():
        try:
            if http_client:
                vals = {}
                for name, ticker in TICKERS.items():
                    try:
                        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d"
                        r = await http_client.get(url, timeout=8.0, headers={"User-Agent": "ApexEcono/3.0"})
                        if r.status_code == 200:
                            data = r.json()
                            meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
                            vals[name] = float(meta.get("regularMarketPrice", 0))
                    except Exception:
                        vals[name] = 0.0

                await store.update_macro(MacroSnapshot(
                    dxy=vals.get("DXY", 0.0),
                    us10y=vals.get("US10Y", 0.0),
                    vix=vals.get("VIX", 0.0),
                    sp500=vals.get("SP500", 0.0),
                    ts=time.time(),
                ))
        except Exception as e:
            log.error(f"macro poller error: {e}")
        try:
            await asyncio.wait_for(stop.wait(), timeout=MACRO_POLL_S)
            break
        except asyncio.TimeoutError:
            pass


async def poll_onchain(stop: asyncio.Event):
    """Poll on-chain data (Glassnode if key available, else stub)."""
    while not stop.is_set():
        try:
            if http_client and GLASSNODE_API_KEY:
                try:
                    # Exchange netflow
                    r = await http_client.get(
                        "https://api.glassnode.com/v1/metrics/transactions/transfers_volume_exchanges_net",
                        params={"a": "BTC", "api_key": GLASSNODE_API_KEY, "i": "1h"},
                        timeout=10.0,
                    )
                    if r.status_code == 200:
                        data = r.json()
                        if data:
                            latest = data[-1]
                            await store.update_onchain("BTCUSDT", OnChainSnapshot(
                                symbol="BTCUSDT",
                                exchange_netflow_btc=float(latest.get("v", 0)),
                                ts=time.time(),
                            ))
                except Exception as e:
                    log.warning(f"onchain poll: {e}")
        except Exception as e:
            log.error(f"onchain poller error: {e}")
        try:
            await asyncio.wait_for(stop.wait(), timeout=ONCHAIN_POLL_S)
            break
        except asyncio.TimeoutError:
            pass


# ────────────────────────────────────────────────────
# FastAPI
# ────────────────────────────────────────────────────
stop_event = asyncio.Event()
poller_tasks: List[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(headers={"User-Agent": "ApexEconoPredator/3.0"})
    pollers = [poll_funding, poll_oi, poll_ls_ratio, poll_atr, poll_macro, poll_onchain]
    for p in pollers:
        poller_tasks.append(asyncio.create_task(p(stop_event)))
    log.info(f"EconoPredator online: {len(pollers)} pollers, symbols={DEFAULT_SYMBOLS}")
    yield
    stop_event.set()
    for t in poller_tasks:
        t.cancel()
    if http_client:
        await http_client.aclose()


app = FastAPI(title="Apex EconoPredator Ingestion", version="3.0.0", lifespan=lifespan)
instrument_app(app)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "econopredator",
        "version": app.version,
        "symbols": DEFAULT_SYMBOLS,
        "pollers": ["funding", "oi", "ls_ratio", "atr", "macro", "onchain"],
        "glassnode": bool(GLASSNODE_API_KEY),
        "whale_alert": bool(WHALE_ALERT_KEY),
    }


@app.get("/market_data/{symbol}")
async def market_data(symbol: str):
    return await store.get_market_data(symbol.upper())


@app.get("/funding_heatmap")
async def funding_heatmap():
    return await store.get_funding_heatmap()


@app.get("/macro_indicators")
async def macro_indicators():
    return await store.get_macro()


@app.get("/atr/{symbol}")
async def atr_endpoint(symbol: str):
    return await store.get_atr(symbol.upper())


@app.get("/onchain/{symbol}")
async def onchain_endpoint(symbol: str):
    async with store._lock:
        oc = store.onchain.get(symbol.upper())
        if not oc:
            return {"symbol": symbol.upper(), "error": "no data"}
        return {
            "symbol": symbol.upper(),
            "exchange_netflow_btc": oc.exchange_netflow_btc,
            "exchange_reserve_btc": oc.exchange_reserve_btc,
            "whale_tx_count_1h": oc.whale_tx_count_1h,
            "whale_total_usd_1h": oc.whale_total_usd_1h,
        }
