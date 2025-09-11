import os
import time
import random
import json
from websocket import create_connection
import requests
from datetime import datetime
from typing import Dict, Any, List

from dotenv import load_dotenv
load_dotenv()

# This worker simulates a global residual feed. Replace the stub in detectResiduals()
# with real sources (e.g., 1inch APIs, mempool listeners, exchange reports).


def _detect_luno_zar() -> List[Dict[str, Any]]:
    pairs_csv = os.getenv("LUNO_PAIRS", "XBTZAR,ETHZAR,USDTZAR").strip()
    if not pairs_csv:
        return []
    pairs = [p.strip() for p in pairs_csv.split(",") if p.strip()]
    events: List[Dict[str, Any]] = []
    for pair in pairs:
        try:
            # Try single-pair endpoint for reliability
            r = requests.get("https://api.luno.com/api/1/ticker", params={"pair": pair}, timeout=3)
            if r.status_code != 200:
                continue
            t = r.json() or {}
            bid = float(t.get("bid") or 0.0)
            ask = float(t.get("ask") or 0.0)
            last = float(t.get("last_trade") or 0.0)
            if bid <= 0 or ask <= 0 or last <= 0:
                continue
            spread = max(ask - bid, 0.0)
            # Normalize to a tiny USDT-equivalent micro-residual
            # scale by relative spread; cap to [0.01 .. 0.50] USDT
            rel = spread / max(last, 1e-9)
            amt = min(max(round(rel * 50.0, 2), 0.01), 0.50)
            events.append({
                "source": "luno_zar",
                "external_ref": f"LUNO-{pair}-{int(time.time())}",
                "echo_tag": os.getenv("ECHO_TAG", "LIVE-001"),
                "amount": float(amt),
                "currency": "USDT",
            })
        except Exception:
            continue
    return events


def _detect_binance_usdt() -> List[Dict[str, Any]]:
    symbols_csv = os.getenv("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT").strip()
    if not symbols_csv:
        return []
    symbols = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]
    events: List[Dict[str, Any]] = []
    for sym in symbols:
        try:
            r = requests.get(
                "https://api.binance.com/api/v3/ticker/bookTicker",
                params={"symbol": sym},
                timeout=3,
            )
            if r.status_code != 200:
                continue
            t = r.json() or {}
            bid = float(t.get("bidPrice") or 0.0)
            ask = float(t.get("askPrice") or 0.0)
            if bid <= 0 or ask <= 0:
                continue
            mid = (bid + ask) / 2.0
            spread = max(ask - bid, 0.0)
            rel = spread / max(mid, 1e-9)
            amt = min(max(round(rel * 100.0, 2), 0.01), 0.50)
            events.append({
                "source": "binance_usdt",
                "external_ref": f"BIN-{sym}-{int(time.time())}",
                "echo_tag": os.getenv("ECHO_TAG", "LIVE-001"),
                "amount": float(amt),
                "currency": "USDT",
            })
        except Exception:
            continue
    return events


def detectResiduals() -> List[Dict[str, Any]]:
    # If POLYGON_API_KEY is set, connect to Polygon delayed stocks feed as a demo
    # and translate message rates into residual micro-events (USDT-equivalent).
    if (os.getenv("LOCATION_CURRENCY", "").upper() == "ZAR") or os.getenv("LUNO_ENABLE", "").lower() in ("1","true","yes","y"):
        luno_events = _detect_luno_zar()
        if luno_events:
            return luno_events
    if os.getenv("BINANCE_ENABLE", "").lower() in ("1","true","yes","y") or os.getenv("LOCATION_CURRENCY", "").upper() in ("USD","USDT","USDUSDT"):
        bin_events = _detect_binance_usdt()
        if bin_events:
            return bin_events
    key = os.getenv("POLYGON_API_KEY")
    if key:
        try:
            wsurl = os.getenv("POLYGON_WS_URL", "wss://delayed.polygon.io/stocks")
            ws = create_connection(wsurl, timeout=5)
            # auth
            ws.send(json.dumps({"action": "auth", "params": key}))
            # subscribe to a small symbol set to reduce load
            ws.send(json.dumps({"action": "subscribe", "params": "T.AAPL,T.MSFT"}))
            # read a couple messages quickly
            raw = ws.recv()
            events: List[Dict[str, Any]] = []
            if raw:
                # each non-status tick becomes a tiny residual
                try:
                    msgs = json.loads(raw)
                    ticks = [m for m in msgs if m.get("ev") not in ("status",)]
                    for _ in ticks[:3]:  # cap per poll
                        amt = round(random.uniform(0.01, 0.10), 2)
                        events.append({
                            "source": "polygon_demo",
                            "external_ref": f"POLY-{int(time.time())}",
                            "echo_tag": os.getenv("ECHO_TAG", "LIVE-001"),
                            "amount": float(amt),
                            "currency": "USDT",
                        })
                except Exception:
                    pass
            try:
                ws.close()
            except Exception:
                pass
            if events:
                return events
        except Exception:
            pass
    # Fallback STUB: generate random residuals to prove plumbing; return USDT-equivalent
    events: List[Dict[str, Any]] = []
    if random.random() < 0.4:  # ~40% chance per tick
        amt = round(random.uniform(0.05, 1.5), 2)
        events.append({
            "source": "global_stub",
            "external_ref": f"STUB-{int(time.time())}",
            "echo_tag": os.getenv("ECHO_TAG", "LIVE-001"),
            "amount": float(amt),
            "currency": "USDT",
        })
    return events


def recordPending(db_session_factory, events: List[Dict[str, Any]]) -> int:
    from .invoke import ResidualEvent  # reuse model
    if not events:
        return 0
    session = db_session_factory()
    try:
        created = 0
        for e in events:
            ev = ResidualEvent(
                source=str(e.get("source")),
                external_ref=str(e.get("external_ref" or "")),
                echo_tag=str(e.get("echo_tag" or "")),
                amount=float(e.get("amount" or 0.0)),
                currency=str(e.get("currency" or "USDT")),
                status="pending",
                detected_at=datetime.utcnow(),
            )
            session.add(ev)
            created += 1
        session.commit()
        return created
    finally:
        session.close()


def run_loop(db_session_factory, poll_seconds: int = 10) -> None:
    print({"ok": True, "msg": "residual worker started", "poll": poll_seconds})
    while True:
        try:
            events = detectResiduals()
            n = recordPending(db_session_factory, events)
            if n:
                print({"detected": len(events), "recorded": n, "ts": datetime.utcnow().isoformat()})
        except Exception as exc:
            print({"ok": False, "error": str(exc)})
        time.sleep(poll_seconds)


if __name__ == "__main__":
    # Lazy import to avoid circulars
    from .invoke import DB_ENABLED, SessionLocal
    if not DB_ENABLED:
        raise SystemExit("DB not enabled; install SQLAlchemy and configure DATABASE_URL")
    run_loop(SessionLocal)

