import os
import hmac
import hashlib
import time
import json
from typing import Any, Dict, Optional

import requests
from datetime import datetime
from flask import Flask, request, jsonify, Response, Blueprint
import threading
from datetime import timedelta

# Optional database layer (disabled automatically if SQLAlchemy isn't available)
DB_ENABLED = True
try:
    from sqlalchemy import (
        Column,
        DateTime,
        Float,
        Integer,
        String,
        Text,
        create_engine,
    )
    from sqlalchemy.orm import declarative_base, sessionmaker
except Exception:
    DB_ENABLED = False

# Web3 / blockchain (optional)
try:
    from web3 import Web3  # type: ignore
except Exception:
    Web3 = None  # type: ignore

try:
    # Web3 v5 style import
    from web3.middleware import geth_poa_middleware as _poa_middleware  # type: ignore
except Exception:  # pragma: no cover - fallback for other versions
    try:
        # Some versions expose a PoA middleware module
        from web3.middleware.geth_poa import geth_poa_middleware as _poa_middleware  # type: ignore
    except Exception:
        _poa_middleware = None

# Load environment from .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///xke.db")

if DB_ENABLED:
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base = declarative_base()

    class LedgerEntry(Base):
        __tablename__ = "ledger_entries"
        id = Column(Integer, primary_key=True)
        tx_id = Column(String, unique=True)
        amount = Column(Float)
        currency = Column(String)
        recipient = Column(String)
        echo_tag = Column(String)
        echo_score = Column(Float)
        echo_interest = Column(Float)
        timestamp = Column(DateTime)
        status = Column(String)
        identity_mark = Column(String)

    class PoolEvent(Base):
        __tablename__ = "pool_events"
        id = Column(Integer, primary_key=True)
        kind = Column(String)  # 'add' or 'withdraw'
        amount = Column(Float)
        currency = Column(String)
        note = Column(String)
        timestamp = Column(DateTime)

    class ResidualEvent(Base):
        __tablename__ = "residual_events"
        id = Column(Integer, primary_key=True)
        source = Column(String)          # e.g., 'global_stub', 'oneinch', 'uniswap'
        external_ref = Column(String)    # optional external id/hash
        echo_tag = Column(String)
        amount = Column(Float)           # USDT-equivalent
        currency = Column(String)
        status = Column(String)          # 'pending' | 'confirmed' | 'cancelled'
        detected_at = Column(DateTime)
        confirmed_tx = Column(String)    # tx id after payout

    class Payout(Base):
        __tablename__ = "payouts"
        id = Column(Integer, primary_key=True)
        idem_key = Column(String, unique=True)
        recipient = Column(String)
        echo_tag = Column(String)
        amount_usdt = Column(Float)
        amount_units = Column(Integer)
        status = Column(String)  # REQUESTED|QUOTED|SENT|CONFIRMED|FAILED
        tx_id = Column(String)
        error_code = Column(String)
        created_at = Column(DateTime)
        updated_at = Column(DateTime)

    # -------------- New trace-ledger and anchoring models -------------- #
    class IngestEvent(Base):
        __tablename__ = "ingest_events"
        id = Column(Integer, primary_key=True)
        source = Column(String)              # e.g., 'swift', 'visa', 'marine_traffic', 'coingecko'
        event_type = Column(String)          # free-form type/name
        external_id = Column(String)         # upstream id/hash if present
        trace_id = Column(String, index=True)  # link events across domains
        amount = Column(Float)
        currency = Column(String)
        amount_usd = Column(Float)           # converted snapshot
        fx_rate_to_usd = Column(Float)       # FX used
        geo = Column(String)                 # optional geo/loc code
        merchant = Column(String)
        merchant_lang = Column(String)
        mcc = Column(String)
        country_from = Column(String)
        country_to = Column(String)
        is_cross_border = Column(String)     # 'Y' or 'N' for SQLite simplicity
        source_tz = Column(String)
        status = Column(String)              # 'received'|'processed'|'error'
        received_at = Column(DateTime)
        raw_json = Column(Text)              # original payload for audit

        def as_public(self) -> dict:
            # Derive amount/currency at read time if missing, using saved raw_json
            derived_amount = self.amount
            derived_currency = self.currency
            if (derived_amount is None or derived_currency is None) and self.raw_json:
                try:
                    raw = json.loads(self.raw_json)
                    if isinstance(raw, dict):
                        # Top-level fallbacks
                        if derived_amount is None:
                            for key in ["amount", "amount_total", "value", "gross_amount", "payment_amount"]:
                                v = raw.get(key)
                                if isinstance(v, (int, float)):
                                    derived_amount = float(v)
                                    break
                                if isinstance(v, str) and v.strip() != "":
                                    try:
                                        derived_amount = float(v)
                                        break
                                    except Exception:
                                        pass
                        if derived_currency is None:
                            for key in ["currency", "currency_code", "ccy"]:
                                v = raw.get(key)
                                if isinstance(v, str) and v.strip() != "":
                                    derived_currency = v
                                    break
                        # PayPal-style nested resource.amount
                        if (derived_amount is None or derived_currency is None) and isinstance(raw.get("resource"), dict):
                            ra = raw["resource"].get("amount")
                            if isinstance(ra, dict):
                                if derived_amount is None:
                                    total = ra.get("total") or ra.get("value")
                                    if isinstance(total, (int, float)):
                                        derived_amount = float(total)
                                    elif isinstance(total, str) and total.strip() != "":
                                        try:
                                            derived_amount = float(total)
                                        except Exception:
                                            pass
                                if derived_currency is None:
                                    ccy = ra.get("currency") or ra.get("currency_code")
                                    if isinstance(ccy, str) and ccy.strip() != "":
                                        derived_currency = ccy
                except Exception:
                    pass
            return {
                "id": self.id,
                "source": self.source,
                "event_type": self.event_type,
                "external_id": self.external_id,
                "trace_id": self.trace_id,
                "amount": derived_amount,
                "currency": derived_currency,
                "amount_usd": self.amount_usd,
                "fx_rate_to_usd": self.fx_rate_to_usd,
                "mcc": self.mcc,
                "merchant": self.merchant,
                "country_from": self.country_from,
                "country_to": self.country_to,
                "is_cross_border": self.is_cross_border,
                "geo": self.geo,
                "status": self.status,
                "received_at": (self.received_at.isoformat() if self.received_at else None),
            }

    class TraceEdge(Base):
        __tablename__ = "trace_edges"
        id = Column(Integer, primary_key=True)
        trace_id = Column(String, index=True)
        from_event_id = Column(Integer)      # FK to IngestEvent.id (not enforced in SQLite)
        to_event_id = Column(Integer)
        relation = Column(String)            # e.g., 'follows', 'settles', 'ships'
        created_at = Column(DateTime)

    class Residual(Base):
        __tablename__ = "residuals"
        id = Column(Integer, primary_key=True)
        trace_id = Column(String, index=True)
        source_event_id = Column(Integer)    # originating IngestEvent.id
        kind = Column(String)                # rounding|fee_rebate|dust|interest|mismatch
        amount_usdt = Column(Float)
        currency = Column(String)
        credited_pool_event_id = Column(Integer)  # PoolEvent.id when credited
        detected_at = Column(DateTime)
        status = Column(String)              # 'pending'|'credited'|'cancelled'

    class AnchorRecord(Base):
        __tablename__ = "anchor_records"
        id = Column(Integer, primary_key=True)
        anchor_hash = Column(String, index=True)  # hash of off-chain window/ledger
        chain_id = Column(Integer)
        tx_id = Column(String)
        confirmations = Column(Integer)
        status = Column(String)              # 'queued'|'sent'|'confirmed'|'failed'
        created_at = Column(DateTime)
        anchored_at = Column(DateTime)

    Base.metadata.create_all(engine)
else:
    # Minimal shims so the rest of the module can import without DB
    engine = None
    SessionLocal = lambda: None  # type: ignore
    Base = object  # type: ignore

app = Flask(__name__)
def _get_account_address() -> Optional[str]:
    try:
        if not w3 or Web3 is None or not PRIVATE_KEY:
            return None
        return w3.eth.account.from_key(PRIVATE_KEY).address
    except Exception:
        return None


############################
# Etherscan micro-transfer harvester
############################

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
ETHERSCAN_POLL_INTERVAL = int(os.getenv("ETHERSCAN_POLL_INTERVAL", "30"))
RESIDUAL_MICRO_THRESHOLD_USDT = float(os.getenv("RESIDUAL_MICRO_THRESHOLD_USDT", "0.5"))
ETHERSCAN_ENABLED = (os.getenv("ETHERSCAN_ENABLED", "true").lower() in ["1","true","yes","y"]) and bool(ETHERSCAN_API_KEY)

_etherscan_thread_started = False
_etherscan_last_ts: Optional[float] = None
_etherscan_last_error: Optional[str] = None
_etherscan_last_found: Optional[int] = None
_etherscan_last_fromblock: Optional[int] = None


def _rpc_block_number() -> Optional[int]:
    try:
        j = _rpc_json(RPC_URL, "eth_blockNumber", []) if RPC_URL else None
        if j and isinstance(j.get("result"), str):
            return int(j["result"], 16)
    except Exception:
        return None
    return None


############################
# BTC mempool dust harvester (polling)
############################

MEMPOOL_ENABLED = (os.getenv("MEMPOOL_ENABLED", "true").lower() in ["1","true","yes","y"])
MEMPOOL_POLL_INTERVAL = int(os.getenv("MEMPOOL_POLL_INTERVAL", "30"))
BTC_DUST_THRESHOLD_SATS = int(os.getenv("BTC_DUST_THRESHOLD_SATS", "546"))

_mempool_thread_started = False
_mempool_last_ts: Optional[float] = None
_mempool_last_error: Optional[str] = None
_mempool_last_found: Optional[int] = None


def _mempool_recent_txids(limit: int = 25) -> Optional[list[str]]:
    try:
        j = _fetch_json("https://mempool.space/api/mempool/recent")
        if isinstance(j, list):
            # recent list items include txid under key 'txid'
            out: list[str] = []
            for item in j[: max(min(limit, 50), 1)]:
                try:
                    txid = item.get("txid") if isinstance(item, dict) else None
                    if isinstance(txid, str) and len(txid) == 64:
                        out.append(txid)
                except Exception:
                    continue
            return out
    except Exception:
        return None
    return None


def _mempool_tx(txid: str) -> Optional[dict]:
    try:
        j = _fetch_json(f"https://mempool.space/api/tx/{txid}")
        if isinstance(j, dict):
            return j
    except Exception:
        return None
    return None


def _sats_to_btc(sats: int) -> float:
    try:
        return float(sats) / 1e8
    except Exception:
        return 0.0


def _mempool_worker_loop():
    global _mempool_last_ts, _mempool_last_error, _mempool_last_found
    while True:
        try:
            _mempool_last_ts = time.time()
            _mempool_last_error = None
            _mempool_last_found = 0
            txids = _mempool_recent_txids(limit=20) or []
            if not DB_ENABLED or not txids:
                time.sleep(max(MEMPOOL_POLL_INTERVAL, 10))
                continue
            # price for conversion
            btc_usdt = _binance_price("BTCUSDT") or 0.0
            s = SessionLocal()
            found = 0
            try:
                for txid in txids:
                    try:
                        # dedupe by external_id
                        exists = s.query(IngestEvent).filter(
                            IngestEvent.source == "mempool",
                            IngestEvent.external_id == txid,
                        ).first()
                        if exists:
                            continue
                        tx = _mempool_tx(txid)
                        if not isinstance(tx, dict):
                            continue
                        vout = tx.get("vout") or []
                        # Some mempool.space responses structure use outputs under 'vout' with 'value' in sats
                        dust_usdt_total = 0.0
                        for o in vout:
                            try:
                                sats = o.get("value")
                                if isinstance(sats, int) and sats > 0 and sats <= BTC_DUST_THRESHOLD_SATS:
                                    btc_amt = _sats_to_btc(sats)
                                    if btc_usdt and btc_amt > 0:
                                        dust_usdt_total += (btc_amt * btc_usdt)
                            except Exception:
                                continue
                        if dust_usdt_total > 0.0:
                            now = datetime.utcnow()
                            ie = IngestEvent(
                                source="mempool",
                                event_type="poll",
                                external_id=txid,
                                trace_id=txid,
                                amount=float(dust_usdt_total),
                                currency="USDT",
                                amount_usd=float(dust_usdt_total),
                                fx_rate_to_usd=1.0,
                                geo=None,
                                merchant=None,
                                merchant_lang=None,
                                mcc=None,
                                country_from=None,
                                country_to=None,
                                is_cross_border="N",
                                source_tz=None,
                                status="received",
                                received_at=now,
                                raw_json=json.dumps(tx)[:200000],
                            )
                            s.add(ie)
                            pe = PoolEvent(kind="add", amount=float(dust_usdt_total), currency="USDT", note=f"btc_dust {txid}", timestamp=now)
                            s.add(pe)
                            found += 1
                    except Exception:
                        continue
                s.commit()
                _mempool_last_found = found
            finally:
                s.close()
        except Exception as e:
            _mempool_last_error = str(e)
        time.sleep(max(MEMPOOL_POLL_INTERVAL, 15))


@app.route("/api/v1/mempool/status", methods=["GET"])
def api_mempool_status():
    return _json_ok({
        "enabled": bool(MEMPOOL_ENABLED),
        "last_ts": int(_mempool_last_ts or 0),
        "last_error": _mempool_last_error,
        "last_found": int(_mempool_last_found or 0),
        "interval_sec": MEMPOOL_POLL_INTERVAL,
        "dust_sats": BTC_DUST_THRESHOLD_SATS,
    })

# Direct route alias to avoid 404 if blueprint mounting is delayed
@app.route("/api/v1/mempool/status", methods=["GET"])
def api_mempool_status_alias():  # type: ignore
    return api_mempool_status()

def _etherscan_getlogs(from_block: int, to_block: Optional[int]) -> Optional[list]:
    try:
        usdt = os.getenv("USDT_ADDRESS")
        if not usdt:
            return []
        base = "https://api.etherscan.io/api"
        params = {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": str(from_block),
            "toBlock": (str(to_block) if to_block is not None else "latest"),
            "address": usdt,
            # keccak256("Transfer(address,address,uint256)")
            "topic0": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "apikey": ETHERSCAN_API_KEY,
        }
        r = requests.get(base, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        if isinstance(j, dict) and j.get("status") in ("1", 1) and isinstance(j.get("result"), list):
            return j["result"]
        # status "0" with result [] means no logs
        if isinstance(j, dict) and isinstance(j.get("result"), list):
            return j["result"]
    except Exception as e:
        pass
    return None


def _decode_topic_address(topic_hex: str) -> Optional[str]:
    try:
        if isinstance(topic_hex, str) and topic_hex.startswith("0x") and len(topic_hex) == 66:
            return "0x" + topic_hex[-40:]
    except Exception:
        return None
    return None


def _etherscan_worker_loop():
    global _etherscan_last_ts, _etherscan_last_error, _etherscan_last_found, _etherscan_last_fromblock
    # Start from recent window
    from_block = None
    while True:
        try:
            _etherscan_last_ts = time.time()
            _etherscan_last_error = None
            _etherscan_last_found = 0
            latest = _rpc_block_number() or 0
            if from_block is None:
                # back 600 blocks (~2 hours on Ethereum) as initial sweep, bounded >= 1
                from_block = max(latest - 600, 1)
            # keep range tight for rate limits
            to_block = latest
            logs = _etherscan_getlogs(from_block, to_block)
            _etherscan_last_fromblock = from_block
            if logs is None:
                _etherscan_last_error = "getlogs_failed"
            else:
                found = 0
                if DB_ENABLED and logs:
                    s = SessionLocal()
                    try:
                        for ev in logs:
                            try:
                                txh = ev.get("transactionHash") or ev.get("transactionHash".lower())
                                idx = ev.get("logIndex")
                                ext_id = f"{txh}:{idx}"
                                # dedupe
                                exists = s.query(IngestEvent).filter(
                                    IngestEvent.source == "etherscan",
                                    IngestEvent.external_id == str(ext_id),
                                ).first()
                                if exists:
                                    continue
                                topics = ev.get("topics") or []
                                from_addr = _decode_topic_address(topics[1]) if len(topics) > 1 else None
                                to_addr = _decode_topic_address(topics[2]) if len(topics) > 2 else None
                                data_hex = ev.get("data") or "0x0"
                                value = int(data_hex, 16)
                                amount_usdt = float(value) / float(10 ** int(os.getenv("USDT_DECIMALS") or 6))
                                # Only micro amounts
                                if amount_usdt <= RESIDUAL_MICRO_THRESHOLD_USDT and amount_usdt > 0:
                                    now = datetime.utcnow()
                                    ie = IngestEvent(
                                        source="etherscan",
                                        event_type="poll",
                                        external_id=str(ext_id),
                                        trace_id=str(txh or ""),
                                        amount=amount_usdt,
                                        currency="USDT",
                                        amount_usd=amount_usdt,
                                        fx_rate_to_usd=1.0,
                                        geo=None,
                                        merchant=None,
                                        merchant_lang=None,
                                        mcc=None,
                                        country_from=None,
                                        country_to=None,
                                        is_cross_border="N",
                                        source_tz=None,
                                        status="received",
                                        received_at=now,
                                        raw_json=json.dumps(ev)[:200000],
                                    )
                                    s.add(ie)
                                    # credit pool
                                    pe = PoolEvent(kind="add", amount=amount_usdt, currency="USDT", note=f"etherscan_micro {txh}", timestamp=now)
                                    s.add(pe)
                                    found += 1
                            except Exception:
                                continue
                        s.commit()
                    finally:
                        s.close()
                _etherscan_last_found = found
                # advance from_block conservatively to avoid gaps
                from_block = max(to_block - 5, 1)
        except Exception as e:
            _etherscan_last_error = str(e)
        time.sleep(max(ETHERSCAN_POLL_INTERVAL, 10))


@app.route("/api/v1/etherscan/status", methods=["GET"])
def api_etherscan_status():
    return _json_ok({
        "enabled": bool(ETHERSCAN_ENABLED),
        "last_ts": int(_etherscan_last_ts or 0),
        "last_error": _etherscan_last_error,
        "last_found": int(_etherscan_last_found or 0),
        "last_from_block": int(_etherscan_last_fromblock or 0),
        "threshold_usdt": RESIDUAL_MICRO_THRESHOLD_USDT,
        "interval_sec": ETHERSCAN_POLL_INTERVAL,
    })

# Direct route alias to avoid 404 if blueprint mounting is delayed
@app.route("/api/v1/etherscan/status", methods=["GET"])
def api_etherscan_status_alias():  # type: ignore
    return api_etherscan_status()

def _read_usdt_balance_human() -> Optional[float]:
    try:
        if not w3 or Web3 is None:
            return None
        token = os.getenv("USDT_ADDRESS")
        if not token or not CONTRACT_ADDRESS:
            return None
        erc20_abi = [
            {"name": "decimals", "outputs": [{"type": "uint8"}], "inputs": [], "stateMutability": "view", "type": "function"},
            {"name": "balanceOf", "outputs": [{"type": "uint256"}], "inputs": [{"name": "", "type": "address"}], "stateMutability": "view", "type": "function"},
        ]
        t = w3.eth.contract(address=Web3.to_checksum_address(token), abi=erc20_abi)  # type: ignore
        try:
            decimals = int(os.getenv("USDT_DECIMALS") or t.functions.decimals().call())
        except Exception:
            decimals = 18
        raw = t.functions.balanceOf(Web3.to_checksum_address(CONTRACT_ADDRESS)).call()
        return float(raw) / float(10 ** decimals)
    except Exception:
        return None


def _read_last_payout_timestamp() -> Optional[str]:
    if not DB_ENABLED:
        return None
    session = SessionLocal()
    try:
        entry = (
            session.query(LedgerEntry)
            .filter((LedgerEntry.status == "confirmed") | (LedgerEntry.status == "CONFIRMED"))
            .order_by(LedgerEntry.timestamp.desc())
            .first()
        )
        return entry.timestamp.isoformat() if entry and entry.timestamp else None
    except Exception:
        return None
    finally:
        session.close()


def _read_last_echo_info() -> Dict[str, Any]:
    """Return last echo info: tag, residual (echo_interest), timestamp."""
    info: Dict[str, Any] = {"echo_tag": None, "residual": None, "timestamp": None}
    if not DB_ENABLED:
        return info
    session = SessionLocal()
    try:
        entry = (
            session.query(LedgerEntry)
            .order_by(LedgerEntry.timestamp.desc())
            .first()
        )
        if entry:
            info["echo_tag"] = entry.echo_tag
            info["residual"] = entry.echo_interest
            info["timestamp"] = entry.timestamp.isoformat() if entry.timestamp else None
        return info
    except Exception:
        return info
    finally:
        session.close()



def _compute_echo_totals() -> Dict[str, float]:
    """Return total USDT sent and total recovered Echo; cache for 15 minutes."""
    totals = {"total_usdt": 0.0, "total_residual": 0.0}
    # 15-minute cache
    now_ts = int(time.time())
    try:
        if hasattr(_compute_echo_totals, "_cache") and hasattr(_compute_echo_totals, "_cache_ts"):
            if isinstance(_compute_echo_totals._cache_ts, int) and (now_ts - _compute_echo_totals._cache_ts) < (15*60):
                return _compute_echo_totals._cache  # type: ignore
    except Exception:
        pass
    if not DB_ENABLED:
        return totals
    session = SessionLocal()
    try:
        ledgers = session.query(LedgerEntry).all()
        totals["total_usdt"] = round(sum(float(l.amount or 0.0) for l in ledgers), 6)
        totals["total_residual"] = round(sum(float(l.echo_interest or 0.0) for l in ledgers), 6)
        # pool = sum(add) - sum(withdraw)
        adds = session.query(PoolEvent).filter(PoolEvent.kind == "add").all()
        subs = session.query(PoolEvent).filter(PoolEvent.kind == "withdraw").all()
        pool_total = sum(float(e.amount or 0.0) for e in adds) - sum(
            float(e.amount or 0.0) for e in subs
        )
        totals["pool_total"] = round(pool_total, 6)
        # update cache
        try:
            _compute_echo_totals._cache = totals  # type: ignore
            _compute_echo_totals._cache_ts = now_ts  # type: ignore
        except Exception:
            pass
        return totals
    except Exception:
        return totals
    finally:
        session.close()


############################
# API helpers and fallback #
############################

def _is_mock_mode() -> bool:
    try:
        flag = (os.getenv("XKE_API_MOCK") or os.getenv("XKE_MOCK_MODE") or "").lower()
        if flag in ("1", "true", "yes", "y"):
            return True
        try:
            return (request.args.get("mock") or "").lower() in ("1", "true", "yes", "y")
        except Exception:
            return False
    except Exception:
        return False


def _json_ok(data: Dict[str, Any], status_code: int = 200):
    payload = {"ok": True}
    payload.update(data)
    return jsonify(payload), status_code


def _json_err(message: str, status_code: int = 400, **extra):
    payload = {"ok": False, "error": message}
    if extra:
        payload.update(extra)
    return jsonify(payload), status_code


def _rpc_eth_call(rpc_url: str, to: str, data: str) -> Optional[str]:
    try:
        body = {"jsonrpc": "2.0", "id": 1, "method": "eth_call", "params": [{"to": to, "data": data}, "latest"]}
        r = requests.post(rpc_url, json=body, timeout=30)
        r.raise_for_status()
        out = r.json().get("result")
        if isinstance(out, str) and out.startswith("0x"):
            return out
    except Exception:
        pass
    return None


def _rpc_json(rpc_url: str, method: str, params: list) -> Optional[dict]:
    try:
        body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        r = requests.post(rpc_url, json=body, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def _rpc_block_number(rpc_url: str) -> Optional[int]:
    j = _rpc_json(rpc_url, "eth_blockNumber", [])
    try:
        if j and isinstance(j.get("result"), str):
            return int(j["result"], 16)
    except Exception:
        return None
    return None


def _rpc_tx_receipt(rpc_url: str, tx_hash: str) -> Optional[dict]:
    j = _rpc_json(rpc_url, "eth_getTransactionReceipt", [tx_hash])
    try:
        res = j.get("result") if j else None
        return res if isinstance(res, dict) else None
    except Exception:
        return None


def _confirmations_for_tx(rpc_url: Optional[str], tx_hash: Optional[str]) -> Optional[int]:
    try:
        if not rpc_url or not tx_hash:
            return None
        receipt = _rpc_tx_receipt(rpc_url, tx_hash)
        if not receipt or not receipt.get("blockNumber"):
            return 0
        tx_block = int(str(receipt["blockNumber"]), 16)
        head = _rpc_block_number(rpc_url)
        if head is None:
            return None
        # confirmations = head - tx_block + 1 (at least 1 when mined)
        return max((head - tx_block + 1), 0)
    except Exception:
        return None


def _read_decimals_via_rpc(rpc_url: str, token: str) -> Optional[int]:
    out = _rpc_eth_call(rpc_url, token, "0x313ce567")
    if not out:
        return None
    try:
        return int(out, 16)
    except Exception:
        return None


def _read_balance_raw_via_rpc(rpc_url: str, token: str, holder: str) -> Optional[int]:
    try:
        addr = holder.lower().replace("0x", "").rjust(64, "0")
        data = "0x70a08231" + addr
        out = _rpc_eth_call(rpc_url, token, data)
        return int(out, 16) if out else None
    except Exception:
        return None


# -------- Security & limits -------- #
API_HMAC_SECRET = os.getenv("API_HMAC_SECRET") or os.getenv("WEBHOOK_SIGNING_SECRET")
HMAC_TTL_SECONDS = int(os.getenv("API_HMAC_TTL", "60"))

RATE_LIMIT_WINDOW_SEC = int(os.getenv("API_RATE_WINDOW", "60"))
RATE_LIMIT_MAX = int(os.getenv("API_RATE_MAX", "30"))
_RATE_BUCKETS: Dict[str, list] = {}


def _rate_limit_key() -> str:
    try:
        return request.headers.get("X-Client-Id") or request.remote_addr or "anon"
    except Exception:
        return "anon"


def _rate_limited() -> bool:
    key = _rate_limit_key()
    now = time.time()
    bucket = _RATE_BUCKETS.get(key) or []
    # prune
    bucket = [t for t in bucket if now - t < RATE_LIMIT_WINDOW_SEC]
    allowed = len(bucket) < RATE_LIMIT_MAX
    if allowed:
        bucket.append(now)
    _RATE_BUCKETS[key] = bucket
    return not allowed


def _verify_api_hmac() -> Optional[Response]:
    if not API_HMAC_SECRET:
        return None  # open for local dev
    try:
        ts_raw = request.headers.get("X-XKE-Timestamp") or "0"
        sig = request.headers.get("X-XKE-Signature") or ""
        ts = int(ts_raw)
        if abs(time.time() - ts) > HMAC_TTL_SECONDS:
            return _json_err("expired", 401)
        # Important: keep request body cached so downstream handlers can still parse JSON
        body = request.get_data(cache=True) or b""
        msg = f"{request.method}\n{request.path}\n{ts_raw}\n".encode("utf-8") + body
        mac = hmac.new(API_HMAC_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(mac, sig):
            return _json_err("unauthorized", 401)
        if _rate_limited():
            return _json_err("rate_limited", 429)
        return None
    except Exception:
        return _json_err("unauthorized", 401)


def _require_api_auth() -> Optional[Response]:
    return _verify_api_hmac()


# -------- Canary & breakers -------- #
CANARY_ENABLED = (os.getenv("CANARY", "true").lower() in ["1","true","yes","y"])
PER_TX_CAP_USDT = float(os.getenv("PER_TX_CAP_USDT", "10.0"))
DAILY_CAP_USDT = float(os.getenv("DAILY_CAP_USDT", "100.0"))
WHITELIST_RECIPIENTS = [s.strip().lower() for s in (os.getenv("WHITELIST_RECIPIENTS", "").split(",")) if s.strip()]
MAX_GAS_GWEI = float(os.getenv("MAX_GAS_GWEI", "60"))
BREAKER_ERR_RATE = float(os.getenv("BREAKER_ERR_RATE", "0.5"))
BREAKER_WINDOW = int(os.getenv("BREAKER_WINDOW", "60"))
_ERR_EVENTS: list = []


def _record_error_event(ok: bool) -> None:
    now = time.time()
    _ERR_EVENTS.append((now, ok))
    # prune
    cutoff = now - BREAKER_WINDOW
    while _ERR_EVENTS and _ERR_EVENTS[0][0] < cutoff:
        _ERR_EVENTS.pop(0)


def _breaker_tripped() -> bool:
    if not _ERR_EVENTS:
        return False
    now = time.time()
    recent = [ok for t, ok in _ERR_EVENTS if now - t <= BREAKER_WINDOW]
    if not recent:
        return False
    err_rate = 1.0 - (sum(1 for ok in recent if ok) / float(len(recent)))
    return err_rate >= BREAKER_ERR_RATE


# -------- Handshake & reconciliation config -------- #
HANDSHAKE_URL = os.getenv("HANDSHAKE_URL", "")
HANDSHAKE_ENABLED = (os.getenv("HANDSHAKE_ENABLED", "true").lower() in ["1","true","yes","y"]) if HANDSHAKE_URL else False
HANDSHAKE_INTERVAL_SECONDS = int(os.getenv("HANDSHAKE_INTERVAL_SECONDS", "30"))

CONFIRM_TARGET = int(os.getenv("CONFIRM_TARGET", "2"))

_handshake_last_ts: Optional[float] = None
_handshake_last_code: Optional[int] = None
_handshake_last_error: Optional[str] = None
_handshake_thread_started = False

_reconcile_last_ts: Optional[float] = None
_reconcile_last_checked: Optional[int] = None
_reconcile_thread_started = False


def _gas_price_gwei(rpc_url: Optional[str]) -> Optional[float]:
    try:
        if not rpc_url:
            return None
        body = {"jsonrpc":"2.0","id":1,"method":"eth_gasPrice","params":[]}
        r = requests.post(rpc_url, json=body, timeout=15)
        r.raise_for_status()
        hexv = (r.json() or {}).get("result") or "0x0"
        wei = int(hexv, 16)
        return wei / 1e9
    except Exception:
        return None


def _sum_payouts_today(session) -> float:
    try:
        if not DB_ENABLED:
            return 0.0
        since = datetime.utcnow() - timedelta(days=1)
        rows = session.query(Payout).filter(Payout.created_at >= since).all()
        return round(sum(float(r.amount_usdt or 0.0) for r in rows), 6)
    except Exception:
        return 0.0


def _enforce_canary(session, recipient: str, amount_usdt: float) -> Optional[Response]:
    r_lower = (recipient or "").lower()
    if CANARY_ENABLED:
        if WHITELIST_RECIPIENTS and r_lower not in WHITELIST_RECIPIENTS:
            return _json_err("recipient_not_whitelisted", 403)
        if amount_usdt > PER_TX_CAP_USDT:
            return _json_err("per_tx_cap_exceeded", 400, cap=PER_TX_CAP_USDT)
        total_today = _sum_payouts_today(session)
        if (total_today + amount_usdt) > DAILY_CAP_USDT:
            return _json_err("daily_cap_exceeded", 400, used=total_today, cap=DAILY_CAP_USDT)
    return None


api_v1 = Blueprint("api_v1", __name__, url_prefix="/api/v1")

# --------------- Market helpers (24/7 international/crypto) --------------- #
BINANCE_ENABLE = (os.getenv("BINANCE_ENABLE", "true").lower() in ["1","true","yes","y"])
BINANCE_SYMBOLS = [s.strip().upper() for s in (os.getenv("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT").split(",")) if s.strip()]
FX_USD_ZAR_URL = os.getenv("FX_USD_ZAR_URL", "https://api.exchangerate.host/latest?base=USD&symbols=ZAR")
USD_ZAR_OVERRIDE = os.getenv("USD_ZAR_OVERRIDE")
FX_ALLOW_INSECURE = (os.getenv("FX_ALLOW_INSECURE", "true").lower() in ["1","true","yes","y"])  # fallback verify=False for dev


_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}


def _fetch_json(url: str, timeout: int = 10, allow_insecure_fallback: bool = True) -> Optional[dict]:
    try:
        r = requests.get(url, timeout=timeout, headers=_DEFAULT_HEADERS)
        r.raise_for_status()
        j = r.json()
        if isinstance(j, dict) or isinstance(j, list):
            return j  # type: ignore
    except Exception:
        # Optional insecure retry for HTTPS endpoints in constrained environments
        try:
            if allow_insecure_fallback and FX_ALLOW_INSECURE and url.lower().startswith("https://"):
                r = requests.get(url, timeout=timeout, verify=False, headers=_DEFAULT_HEADERS)  # type: ignore[arg-type]
                r.raise_for_status()
                j = r.json()
                if isinstance(j, dict) or isinstance(j, list):
                    return j  # type: ignore
        except Exception:
            return None
        return None
    return None


def _binance_price(symbol: str) -> Optional[float]:
    try:
        if not BINANCE_ENABLE:
            return None
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol.upper()}"
        j = _fetch_json(url)
        if isinstance(j, dict) and "price" in j:
            return float(j["price"])  # price in quote currency (e.g., USDT)
    except Exception:
        return None
    return None


def _binance_prices(symbols: list[str]) -> dict:
    out: dict[str, Optional[float]] = {}
    for s in symbols:
        out[s.upper()] = _binance_price(s)
    return out


def _fx_usd_zar() -> Optional[float]:
    # Manual override for environments with blocked FX APIs
    try:
        if USD_ZAR_OVERRIDE is not None:
            val = float(USD_ZAR_OVERRIDE)
            if val > 0:
                return val
    except Exception:
        pass

    # Try exchangerate.host convert endpoint first (direct 1 USD → ZAR)
    try:
        j = _fetch_json("https://api.exchangerate.host/convert?from=USD&to=ZAR&amount=1")
        if isinstance(j, dict):
            val = j.get("result")
            if isinstance(val, (int, float)) and val > 0:
                return float(val)
    except Exception:
        pass

    # Fallback to configured URL (default: latest?base=USD&symbols=ZAR)
    try:
        j = _fetch_json(FX_USD_ZAR_URL)
        if isinstance(j, dict):
            rates = j.get("rates") or {}
            val = rates.get("ZAR")
            if isinstance(val, (int, float)) and val > 0:
                return float(val)
    except Exception:
        pass

    # Fallback to frankfurter.app
    try:
        j = _fetch_json("https://api.frankfurter.app/latest?from=USD&to=ZAR")
        if isinstance(j, dict):
            rates = j.get("rates") or {}
            val = rates.get("ZAR")
            if isinstance(val, (int, float)) and val > 0:
                return float(val)
    except Exception:
        pass

    # Fallback to open.er-api.com
    try:
        j = _fetch_json("https://open.er-api.com/v6/latest/USD")
        if isinstance(j, dict):
            rates = j.get("rates") or {}
            val = rates.get("ZAR")
            if isinstance(val, (int, float)) and val > 0:
                return float(val)
    except Exception:
        pass

    # Fallback to jsdelivr community rates
    try:
        j = _fetch_json("https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@1/latest/currencies/usd/zar.json")
        if isinstance(j, dict):
            val = j.get("zar")
            if isinstance(val, (int, float)) and val > 0:
                return float(val)
    except Exception:
        pass

    # Fallback to CoinGecko tether→ZAR (USDT≈USD)
    try:
        j = _fetch_json("https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=zar")
        if isinstance(j, dict):
            tether = j.get("tether") or {}
            val = tether.get("zar")
            if isinstance(val, (int, float)) and val > 0:
                return float(val)
    except Exception:
        pass

    return None


@api_v1.app_errorhandler(Exception)
def _api_error(exc):  # type: ignore
    try:
        return _json_err(str(exc), status_code=500)
    except Exception:
        return jsonify({"ok": False, "error": "internal_error"}), 500


@api_v1.route("/health", methods=["GET"])
def api_health():
    if _is_mock_mode():
        return _json_ok({
            "status": "ok",
            "time": int(time.time()),
            "db": bool(DB_ENABLED),
            "rpc": True,
            "w3": True,
            "chain_id": 1,
            "contract": True,
        })
    return _json_ok({
        "status": "ok",
        "time": int(time.time()),
        "db": bool(DB_ENABLED),
        "rpc": bool(RPC_URL),
        "w3": bool(w3),
        "chain_id": CHAIN_ID,
        "contract": bool(CONTRACT_ADDRESS),
        "gas_gwei": _gas_price_gwei(RPC_URL),
        "breaker": _breaker_tripped(),
    })


@api_v1.route("/config", methods=["GET"])
def api_config():
    safe = {
        "chain_id": CHAIN_ID,
        "contract_address": CONTRACT_ADDRESS,
        "canary": CANARY_ENABLED,
        "per_tx_cap_usdt": PER_TX_CAP_USDT,
        "daily_cap_usdt": DAILY_CAP_USDT,
        "max_gas_gwei": MAX_GAS_GWEI,
        "whitelist_size": len(WHITELIST_RECIPIENTS),
    }
    return _json_ok(safe)


@api_v1.route("/home", methods=["GET"])
def api_home():
    # Summary for frontend home page
    bal = _read_usdt_balance_human()
    usd_zar = _fx_usd_zar()
    # Treat USDT≈USD for estimate
    est_usdt_value = round(float(bal or 0.0), 6)
    est_zar_value = round(est_usdt_value * float(usd_zar or 0.0), 2)
    health = {
        "ok": True,
        "db": bool(DB_ENABLED),
        "rpc": bool(RPC_URL),
        "chain_id": CHAIN_ID,
        "gas_gwei": _gas_price_gwei(RPC_URL),
        "breaker": _breaker_tripped(),
    }
    # movement rate proxy: pending residuals + last timestamp recency
    last = _read_last_echo_info()
    totals = _compute_echo_totals()
    pool_total = float(totals.get("pool_total", 0.0) or 0.0)
    pool_usdt_value = round(pool_total, 6)
    pool_zar_value = round(pool_total * float(usd_zar or 0.0), 2)
    pending = 0
    if DB_ENABLED:
        try:
            s = SessionLocal()
            try:
                pending = s.execute("SELECT COUNT(1) FROM residual_events WHERE status='pending'").scalar() or 0
            finally:
                s.close()
        except Exception:
            pending = 0
    return _json_ok({
        "usdt_balance": (round(bal, 6) if isinstance(bal, float) else None),
        "usdt_value": est_usdt_value,
        "zar_value": est_zar_value,
        "usd_zar": (round(float(usd_zar), 4) if usd_zar else None),
        "health": health,
        "movement_pending": int(pending),
        "last_timestamp": last.get("timestamp"),
        "pool_total": pool_total,
        "pool_usdt_value": pool_usdt_value,
        "pool_zar_value": pool_zar_value,
        "server_time": int(time.time()),
    })


@api_v1.route("/balance", methods=["GET"])
def api_balance():
    if _is_mock_mode():
        return _json_ok({"token": os.getenv("USDT_ADDRESS") or "USDT", "raw": 12345678, "decimals": 6, "human": 12.345678})

    token = os.getenv("USDT_ADDRESS")
    if not token or not CONTRACT_ADDRESS:
        return _json_err("missing_token_or_contract", 400)

    decimals = None
    raw = None
    human = None
    try:
        if w3:
            erc20_abi = [
                {"name": "decimals", "outputs": [{"type": "uint8"}], "inputs": [], "stateMutability": "view", "type": "function"},
                {"name": "balanceOf", "outputs": [{"type": "uint256"}], "inputs": [{"name": "", "type": "address"}], "stateMutability": "view", "type": "function"},
            ]
            t = w3.eth.contract(address=Web3.to_checksum_address(token), abi=erc20_abi)
            try:
                decimals = int(os.getenv("USDT_DECIMALS") or t.functions.decimals().call())
            except Exception:
                decimals = 18
            raw = int(t.functions.balanceOf(Web3.to_checksum_address(CONTRACT_ADDRESS)).call())
            human = float(raw) / float(10 ** int(decimals))
    except Exception:
        pass

    if raw is None:
        rpc = RPC_URL or os.getenv("RPC_URL")
        if not rpc:
            return _json_err("missing_rpc_url", 400)
        decimals = decimals or _read_decimals_via_rpc(rpc, token) or 6
        raw = _read_balance_raw_via_rpc(rpc, token, CONTRACT_ADDRESS) or 0
        human = float(raw) / float(10 ** int(decimals))

    return _json_ok({
        "token": token,
        "raw": int(raw),
        "decimals": int(decimals or 6),
        "human": round(float(human), 6),
    })


@api_v1.route("/openapi.json", methods=["GET"])
def api_openapi():
    try:
        # Minimal OpenAPI 3.1 for AI Studio consumption
        spec = {
        "openapi": "3.1.0",
        "info": {"title": "XamKwe Echo API", "version": "1.0.0"},
        "paths": {
            "/api/v1/health": {"get": {"summary": "Health", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/config": {"get": {"summary": "Config", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/balance": {"get": {"summary": "USDT balance", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/home": {"get": {"summary": "Home summary (balance, health, movement)", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/market/status": {"get": {"summary": "Market status (USDZAR + top symbols)", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/market/quote": {"get": {"summary": "Quote symbol in USDT and ZAR", "parameters": [{"name": "symbol", "in": "query", "required": True, "schema": {"type": "string"}}], "responses": {"200": {"description": "OK"}, "400": {"description": "Bad request"}}}},
            "/api/v1/status": {"get": {"summary": "Status", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/metrics": {"get": {"summary": "Metrics", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/handshake/status": {"get": {"summary": "Background handshake status", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/reconcile/status": {"get": {"summary": "Payout reconciler status", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/mempool/status": {"get": {"summary": "BTC mempool dust harvester status", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/etherscan/status": {"get": {"summary": "Etherscan harvester status", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/reload": {"post": {"summary": "Reload env", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/ingest/status": {"get": {"summary": "Ingest status (last 24h)", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/ingest/paypal": {"post": {"summary": "PayPal webhook (HMAC)", "responses": {"200": {"description": "OK"}, "401": {"description": "Unauthorized"}}}},
            "/api/v1/ingest/stripe": {"post": {"summary": "Stripe webhook (HMAC)", "responses": {"200": {"description": "OK"}, "401": {"description": "Unauthorized"}}}},
            "/api/v1/webhooks/paypal": {"post": {"summary": "PayPal webhook alias (HMAC)", "responses": {"200": {"description": "OK"}, "401": {"description": "Unauthorized"}}}},
            "/api/v1/webhooks/stripe": {"post": {"summary": "Stripe webhook alias (HMAC)", "responses": {"200": {"description": "OK"}, "401": {"description": "Unauthorized"}}}},
            "/api/v1/webhooks/flutterwave": {"post": {"summary": "Flutterwave webhook (HMAC)", "responses": {"200": {"description": "OK"}, "401": {"description": "Unauthorized"}}}},
            "/api/v1/webhooks/payu": {"post": {"summary": "PayU webhook (HMAC)", "responses": {"200": {"description": "OK"}, "401": {"description": "Unauthorized"}}}},
            "/api/v1/webhooks/mpesa": {"post": {"summary": "M-Pesa webhook (HMAC)", "responses": {"200": {"description": "OK"}, "401": {"description": "Unauthorized"}}}},
            "/api/v1/ingest/voucher/1voucher": {"post": {"summary": "1Voucher webhook (HMAC)", "responses": {"200": {"description": "OK"}, "401": {"description": "Unauthorized"}}}},
            "/api/v1/ingest/voucher/ott": {"post": {"summary": "OTT Voucher webhook (HMAC)", "responses": {"200": {"description": "OK"}, "401": {"description": "Unauthorized"}}}},
            "/api/v1/flow/status": {"get": {"summary": "Pipeline status: Harvested→Validated→Aggregated→Payout→Proof", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/gambling/transactions": {"get": {"summary": "List ingest transactions (optional mcc,country,trace_id)", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/gambling/transactions/{tid}": {"get": {"summary": "Get gambling transaction by id", "parameters": [{"name": "tid", "in": "path", "required": True, "schema": {"type": "integer"}}], "responses": {"200": {"description": "OK"}, "404": {"description": "Not found"}}}},
            "/api/v1/gambling/trace/{trace_id}": {"get": {"summary": "Get full trace by trace_id", "parameters": [{"name": "trace_id", "in": "path", "required": True, "schema": {"type": "string"}}], "responses": {"200": {"description": "OK"}, "404": {"description": "Not found"}}}},
            "/api/v1/payout/quote": {"post": {"summary": "Quote payout", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/payout/send": {"post": {"summary": "Send payout", "responses": {"200": {"description": "OK"}}}},
            "/api/v1/payout/{id}": {"get": {"summary": "Get payout by id/key/tx", "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "string"}}], "responses": {"200": {"description": "OK"}, "404": {"description": "Not found"}}}},
        },
        }
        return jsonify(spec), 200
    except Exception as exc:
        return _json_err("openapi_build_failed", 500, detail=str(exc))


############################
# Policy scheduler (standardized payouts)
############################

# Policy configuration (AI Studio payout tab)
POLICY_ENABLED = (os.getenv("POLICY_ENABLED", "true").lower() in ["1","true","yes","y"])
POLICY_POOL_TRIGGER_USDT = float(os.getenv("POLICY_POOL_TRIGGER_USDT", "5000"))
POLICY_TARGET_POOL_USDT = float(os.getenv("POLICY_TARGET_POOL_USDT", "100000"))
POLICY_LUNO_RECIPIENT = os.getenv("POLICY_LUNO_RECIPIENT", "")
POLICY_MM_RECIPIENT = os.getenv("POLICY_MM_RECIPIENT", "")
POLICY_LUNO_AMOUNT_USDT = float(os.getenv("POLICY_LUNO_AMOUNT_USDT", "1000"))
POLICY_MM_AMOUNT_USDT = float(os.getenv("POLICY_MM_AMOUNT_USDT", "100"))
POLICY_INTERVAL_SECONDS = int(os.getenv("POLICY_INTERVAL_SECONDS", "300"))

_policy_last_run_ts: Optional[float] = None
_policy_last_action: Optional[str] = None
_policy_thread_started = False


def _policy_should_run() -> bool:
    if not DB_ENABLED or not POLICY_ENABLED:
        return False
    # Avoid too-frequent runs
    if _policy_last_run_ts and (time.time() - _policy_last_run_ts) < max(POLICY_INTERVAL_SECONDS, 60):
        return False
    return True


def _policy_tick() -> None:
    global _policy_last_run_ts, _policy_last_action
    _policy_last_run_ts = time.time()
    _policy_last_action = None

    # Safety checks
    if _breaker_tripped():
        _policy_last_action = "breaker_open"
        return
    g = _gas_price_gwei(RPC_URL)
    if g is not None and g > MAX_GAS_GWEI:
        _policy_last_action = f"gas_high_{g:.2f}gwei"
        return

    # Compute totals
    totals = _compute_echo_totals()
    pool_total = float(totals.get("pool_total", 0.0) or 0.0)
    # Stop policy after reaching target pool
    if pool_total >= POLICY_TARGET_POOL_USDT:
        _policy_last_action = "target_reached"
        return
    if pool_total < POLICY_POOL_TRIGGER_USDT:
        _policy_last_action = "below_trigger"
        return

    # Prepare recipients
    runs: list[tuple[str, float, str]] = []
    if POLICY_LUNO_RECIPIENT:
        runs.append((POLICY_LUNO_RECIPIENT, POLICY_LUNO_AMOUNT_USDT, "POLICY-LUNO"))
    if POLICY_MM_RECIPIENT:
        runs.append((POLICY_MM_RECIPIENT, POLICY_MM_AMOUNT_USDT, "POLICY-MM"))
    if not runs:
        _policy_last_action = "no_recipients"
        return

    session = SessionLocal() if DB_ENABLED else None
    try:
        for recipient, amount_usdt, tag in runs:
            # Canary checks
            fail = _enforce_canary(session, recipient, amount_usdt) if session else None
            if fail is not None:
                _policy_last_action = f"canary_block_{tag}"
                continue

            amount_units = int(round(float(amount_usdt) * (10 ** USDT_DECIMALS)))
            idem_key = f"policy-{tag}-{time.strftime('%Y%m%d')}-{recipient.lower()}-{amount_units}"

            # Idempotency
            if DB_ENABLED:
                existing = session.query(Payout).filter(Payout.idem_key == idem_key).first()
                if existing and existing.status in ("SENT", "CONFIRMED"):
                    _policy_last_action = f"already_sent_{tag}"
                    continue

            # Record REQUESTED
            if DB_ENABLED:
                row = Payout(
                    idem_key=idem_key,
                    recipient=recipient,
                    echo_tag=tag,
                    amount_usdt=amount_usdt,
                    amount_units=amount_units,
                    status="REQUESTED",
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                )
                session.add(row)
                session.commit()

            # Send
            tx = send_payout_onchain(recipient, amount_units, tag)
            ok = bool(tx)
            _record_error_event(ok)
            if DB_ENABLED:
                row = session.query(Payout).filter(Payout.idem_key == idem_key).first()
                if row:
                    row.status = "SENT" if ok else "FAILED"
                    row.tx_id = tx
                    row.updated_at = datetime.utcnow()
                    session.commit()

            # Confirm (non-blocking small wait)
            confirmed = False
            if tx:
                try:
                    confirmed = confirm_onchain(tx, int(os.getenv("CONFIRM_TIMEOUT", "60")))
                except Exception:
                    confirmed = False
            if DB_ENABLED:
                row = session.query(Payout).filter(Payout.idem_key == idem_key).first()
                if row:
                    row.status = "CONFIRMED" if confirmed else row.status
                    row.updated_at = datetime.utcnow()
                    session.commit()

            _policy_last_action = (f"sent_{tag}" if ok else f"failed_{tag}")
    finally:
        if session:
            session.close()


def _policy_worker_loop() -> None:
    while True:
        try:
            if _policy_should_run():
                _policy_tick()
        except Exception:
            pass
        time.sleep(max(POLICY_INTERVAL_SECONDS, 60))


def _handshake_worker_loop() -> None:
    global _handshake_last_ts, _handshake_last_code, _handshake_last_error
    while True:
        try:
            if HANDSHAKE_ENABLED and HANDSHAKE_URL:
                try:
                    r = requests.get(HANDSHAKE_URL, timeout=10)
                    _handshake_last_code = r.status_code
                    _handshake_last_error = None
                except Exception as e:
                    _handshake_last_code = None
                    _handshake_last_error = str(e)
                _handshake_last_ts = time.time()
        except Exception:
            pass
        time.sleep(max(HANDSHAKE_INTERVAL_SECONDS, 10))


def _reconcile_worker_loop() -> None:
    global _reconcile_last_ts, _reconcile_last_checked
    while True:
        try:
            _reconcile_last_ts = time.time()
            if not DB_ENABLED:
                time.sleep(10)
                continue
            rpc = RPC_URL or os.getenv("RPC_URL")
            if not rpc:
                time.sleep(10)
                continue
            session = SessionLocal()
            try:
                pending = session.query(Payout).filter(Payout.status == "SENT").all()
                for row in pending:
                    confs = _confirmations_for_tx(rpc, row.tx_id)
                    if confs is not None:
                        _reconcile_last_checked = int(confs)
                        if int(confs) >= int(CONFIRM_TARGET):
                            row.status = "CONFIRMED"
                            row.updated_at = datetime.utcnow()
                            session.commit()
            finally:
                session.close()
        except Exception:
            pass
        time.sleep(12)


@api_v1.route("/policy/status", methods=["GET"])
def api_policy_status():
    totals = _compute_echo_totals()
    return _json_ok({
        "enabled": POLICY_ENABLED,
        "trigger_usdt": POLICY_POOL_TRIGGER_USDT,
        "target_pool_usdt": POLICY_TARGET_POOL_USDT,
        "luno_amount_usdt": POLICY_LUNO_AMOUNT_USDT,
        "mm_amount_usdt": POLICY_MM_AMOUNT_USDT,
        "luno_recipient": POLICY_LUNO_RECIPIENT or None,
        "mm_recipient": POLICY_MM_RECIPIENT or None,
        "pool_total": totals.get("pool_total", 0.0),
        "last_run_ts": int(_policy_last_run_ts) if _policy_last_run_ts else None,
        "last_action": _policy_last_action,
        "interval_seconds": POLICY_INTERVAL_SECONDS,
    })


@api_v1.route("/status", methods=["GET"])
def api_status():
    if _is_mock_mode():
        return _json_ok({
            "account": "0xF00...BA5E",
            "contract": "0xC0nTRac7...",
            "usdt_balance": 42.0,
            "last_echo_tag": "LIVE-001",
            "last_residual": 1.23,
            "last_timestamp": "2025-09-10T12:00:00Z",
            "total_residual": 99.99,
            "pool_total": 12.34,
            "pending_residual_events": 0,
        })

    bal = _read_usdt_balance_human()
    last = _read_last_echo_info()
    totals = _compute_echo_totals()
    pending_count = 0
    if DB_ENABLED:
        try:
            session = SessionLocal()
            try:
                pending_count = session.execute(
                    "SELECT COUNT(1) FROM residual_events WHERE status='pending'"
                ).scalar() or 0
            finally:
                session.close()
        except Exception:
            pending_count = 0

    return _json_ok({
        "account": _get_account_address(),
        "contract": CONTRACT_ADDRESS,
        "usdt_balance": (round(bal, 6) if isinstance(bal, float) else None),
        "last_echo_tag": last.get("echo_tag"),
        "last_residual": (round(float(last.get("residual")), 6) if last.get("residual") is not None else None),
        "last_timestamp": last.get("timestamp"),
        "total_residual": totals.get("total_residual", 0.0),
        "pool_total": totals.get("pool_total", 0.0),
        "pending_residual_events": int(pending_count),
    })


@api_v1.route("/invoke", methods=["POST"])
def api_invoke():
    auth_failed = _require_auth()
    if auth_failed:
        return auth_failed
    if _is_mock_mode():
        body = request.get_json(silent=True) or {}
        amt = float(body.get("amount_usdt") or 0.0)
        return _json_ok({
            "ok": True,
            "tx_id": "0xMOCKTX",
            "confirmed": True,
            "recipient": body.get("recipient"),
            "echo_tag": body.get("echo_tag", "ECHO"),
            "echo_score": 7.5,
            "residual_value_usdt": round(amt * 0.1, 2),
            "amount_usdt": amt,
            "timestamp": datetime.utcnow().isoformat(),
        })

    data = request.get_json(silent=True) or {}
    recipient = (data.get("recipient") or os.getenv("RECIPIENT") or "").strip()
    echo_tag = (data.get("echo_tag") or "ECHO").strip()
    try:
        amount = float(data.get("amount_usdt") or 0.0)
    except Exception:
        return _json_err("invalid_amount")

    result = invoke_echo_payout({"echo_tag": echo_tag}, {"address": recipient}, base_amount_zar=amount, fx_rate=1.0)
    return jsonify(result), 200


@api_v1.route("/payout/quote", methods=["POST"])
def api_payout_quote():
    auth_failed = _require_api_auth()
    if auth_failed:
        return auth_failed
    data = request.get_json(silent=True) or {}
    recipient = (data.get("recipient") or "").strip()
    echo_tag = (data.get("echo_tag") or "ECHO").strip()
    try:
        base_amount = float(data.get("amount_usdt") or 0.0)
    except Exception:
        return _json_err("invalid_amount")
    echo_score = score_echo({"echo_tag": echo_tag})
    echo_interest = calculate_echo_interest(echo_score, base_amount)
    total = round(base_amount + echo_interest, 2)
    capped = False
    session = SessionLocal() if DB_ENABLED else None
    try:
        breach = _enforce_canary(session, recipient, total) if session else None
        if breach is not None:
            capped = True
    finally:
        if session:
            session.close()
    return _json_ok({
        "recipient": recipient,
        "echo_tag": echo_tag,
        "echo_score": echo_score,
        "residual_value_usdt": echo_interest,
        "amount_usdt": total,
        "capped": capped,
    })


@api_v1.route("/payout/send", methods=["POST"])
def api_payout_send():
    auth_failed = _require_api_auth()
    if auth_failed:
        return auth_failed
    if _breaker_tripped():
        return _json_err("circuit_open", 503)
    # gas sanity
    g = _gas_price_gwei(RPC_URL)
    if g is not None and g > MAX_GAS_GWEI:
        return _json_err("gas_too_high", 503, gas_gwei=g)

    idem_key = request.headers.get("X-Idempotency-Key") or ""
    if not idem_key:
        return _json_err("missing_idempotency_key", 400)
    data = request.get_json(silent=True) or {}
    recipient = (data.get("recipient") or "").strip()
    echo_tag = (data.get("echo_tag") or "ECHO").strip()
    try:
        base_amount = float(data.get("amount_usdt") or 0.0)
    except Exception:
        return _json_err("invalid_amount")

    session = SessionLocal() if DB_ENABLED else None
    try:
        if DB_ENABLED:
            existing = session.query(Payout).filter(Payout.idem_key == idem_key).first()
            if existing:
                return _json_ok({
                    "id": existing.id,
                    "status": existing.status,
                    "tx_id": existing.tx_id,
                })

        # compute amounts
        echo_score = score_echo({"echo_tag": echo_tag})
        echo_interest = calculate_echo_interest(echo_score, base_amount)
        total = round(base_amount + echo_interest, 2)
        canary_fail = _enforce_canary(session, recipient, total) if session else None
        if canary_fail is not None:
            return canary_fail

        amount_units = int(round(total * (10 ** USDT_DECIMALS)))

        # record REQUESTED
        if DB_ENABLED:
            row = Payout(
                idem_key=idem_key,
                recipient=recipient,
                echo_tag=echo_tag,
                amount_usdt=total,
                amount_units=amount_units,
                status="REQUESTED",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            session.add(row)
            session.commit()

        # send
        tx = send_payout_onchain(recipient, amount_units, echo_tag)
        ok = bool(tx)
        _record_error_event(ok)

        if DB_ENABLED:
            row = session.query(Payout).filter(Payout.idem_key == idem_key).first()
            if row:
                row.status = "SENT" if ok else "FAILED"
                row.tx_id = tx
                row.updated_at = datetime.utcnow()
                session.commit()

        # confirm inline (can be replaced by worker)
        confirmed = False
        if tx:
            try:
                timeout = int(os.getenv("CONFIRM_TIMEOUT", "90"))
                confirmed = confirm_onchain(tx, timeout)
            except Exception:
                confirmed = False
        if DB_ENABLED:
            row = session.query(Payout).filter(Payout.idem_key == idem_key).first()
            if row:
                row.status = "CONFIRMED" if confirmed else row.status
                row.updated_at = datetime.utcnow()
                session.commit()

        return _json_ok({"tx_id": tx, "confirmed": confirmed})
    finally:
        if session:
            session.close()


@api_v1.route("/payout/<pid>", methods=["GET"])
def api_payout_get(pid: str):
    auth_failed = _require_api_auth()
    if auth_failed:
        return auth_failed
    if not DB_ENABLED:
        return _json_err("db_disabled", 400)
    session = SessionLocal()
    try:
        row = None
        # Try by idempotency key
        row = session.query(Payout).filter(Payout.idem_key == pid).first()
        if not row and pid.isdigit():
            row = session.query(Payout).filter(Payout.id == int(pid)).first()
        if not row:
            row = session.query(Payout).filter(Payout.tx_id == pid).first()
        if not row:
            return _json_err("not_found", 404)
        return _json_ok({
            "id": row.id,
            "idem_key": row.idem_key,
            "recipient": row.recipient,
            "echo_tag": row.echo_tag,
            "amount_usdt": row.amount_usdt,
            "status": row.status,
            "tx_id": row.tx_id,
            "confirmations": _confirmations_for_tx(RPC_URL, row.tx_id),
            "contract": CONTRACT_ADDRESS,
            "created_at": (row.created_at.isoformat() if row.created_at else None),
            "updated_at": (row.updated_at.isoformat() if row.updated_at else None),
        })
    finally:
        session.close()


@api_v1.route("/metrics", methods=["GET"])
def api_metrics():
    bal = _read_usdt_balance_human()
    last = _read_last_echo_info()
    totals = _compute_echo_totals()
    pending_count = 0
    if DB_ENABLED:
        try:
            session = SessionLocal()
            try:
                pending_count = session.execute(
                    "SELECT COUNT(1) FROM residual_events WHERE status='pending'"
                ).scalar() or 0
            finally:
                session.close()
        except Exception:
            pending_count = 0
    return _json_ok({
        "usdt_balance": (round(bal, 6) if isinstance(bal, float) else None),
        "last_echo_tag": last.get("echo_tag"),
        "last_residual": (round(float(last.get("residual")), 6) if last.get("residual") is not None else None),
        "last_timestamp": last.get("timestamp"),
        "total_residual": totals.get("total_residual", 0.0),
        "pool_total": totals.get("pool_total", 0.0),
        "pending_residual_events": int(pending_count),
        "handshake": {
            "enabled": HANDSHAKE_ENABLED,
            "last_ts": int(_handshake_last_ts) if _handshake_last_ts else None,
            "last_code": _handshake_last_code,
            "last_error": _handshake_last_error,
        },
        "reconciler": {
            "last_ts": int(_reconcile_last_ts) if _reconcile_last_ts else None,
            "last_checked": _reconcile_last_checked,
            "confirm_target": CONFIRM_TARGET,
        },
    })


@api_v1.route("/reload", methods=["POST"])
def api_reload():
    auth_failed = _require_auth()
    if auth_failed:
        return auth_failed
    res = _refresh_config_from_env()
    return jsonify(res), 200


@api_v1.route("/flow/status", methods=["GET"])
def api_flow_status():
    harvested = 0
    validated = 0
    aggregated_usdt = 0.0
    payout_triggered = 0
    onchain_proof = 0
    latest_ts = None
    if DB_ENABLED:
        try:
            s = SessionLocal()
            try:
                # Residual events as harvested/validated (simple proxy)
                h = s.execute("SELECT COUNT(1) FROM residual_events WHERE status='pending'").scalar() or 0
                v = s.execute("SELECT COUNT(1) FROM residual_events WHERE status='confirmed'").scalar() or 0
                # Aggregated pool total
                adds = s.query(PoolEvent).filter(PoolEvent.kind == "add").all()
                subs = s.query(PoolEvent).filter(PoolEvent.kind == "withdraw").all()
                aggregated_usdt = float(sum(float(e.amount or 0.0) for e in adds) - sum(float(e.amount or 0.0) for e in subs))
                # Payouts
                payout_triggered = s.execute("SELECT COUNT(1) FROM payouts WHERE status='SENT'").scalar() or 0
                onchain_proof = s.execute("SELECT COUNT(1) FROM payouts WHERE status='CONFIRMED'").scalar() or 0
                # Latest timestamp across ledgers (as activity marker)
                last = (
                    s.query(LedgerEntry)
                    .order_by(LedgerEntry.timestamp.desc())
                    .first()
                )
                latest_ts = last.timestamp.isoformat() if last and last.timestamp else None
            finally:
                s.close()
        except Exception:
            pass
    return _json_ok({
        "harvested": int(harvested),
        "validated": int(validated),
        "aggregated_usdt": round(float(aggregated_usdt), 6),
        "payout_triggered": int(payout_triggered),
        "onchain_proof": int(onchain_proof),
        "last_activity": latest_ts,
    })


@api_v1.route("/handshake/status", methods=["GET"])
def api_handshake_status():
    return _json_ok({
        "enabled": HANDSHAKE_ENABLED,
        "url": HANDSHAKE_URL or None,
        "last_ts": int(_handshake_last_ts) if _handshake_last_ts else None,
        "last_code": _handshake_last_code,
        "last_error": _handshake_last_error,
        "interval_seconds": HANDSHAKE_INTERVAL_SECONDS,
    })


@api_v1.route("/reconcile/status", methods=["GET"])
def api_reconcile_status():
    return _json_ok({
        "last_ts": int(_reconcile_last_ts) if _reconcile_last_ts else None,
        "last_checked": _reconcile_last_checked,
        "confirm_target": CONFIRM_TARGET,
    })


# ---------------- Market endpoints ---------------- #
@api_v1.route("/market/status", methods=["GET"])
def api_market_status():
    try:
        prices = _binance_prices(BINANCE_SYMBOLS)
        usd_zar = _fx_usd_zar()
        return _json_ok({
            "binance_enable": BINANCE_ENABLE,
            "symbols": BINANCE_SYMBOLS,
            "prices_usdt": prices,
            "usd_zar": (round(usd_zar, 4) if isinstance(usd_zar, float) else None),
            "server_time": int(time.time()),
        })
    except Exception as exc:
        return _json_err("market_unavailable", 503, detail=str(exc))


@api_v1.route("/market/quote", methods=["GET"])
def api_market_quote():
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return _json_err("missing_symbol", 400)
    p = _binance_price(symbol)
    if p is None:
        return _json_err("symbol_unavailable", 404)
    usd_zar = _fx_usd_zar() or 0.0
    # p is price in USDT, treat USDT≈USD for quoting
    price_usdt = float(p)
    price_zar = float(price_usdt) * float(usd_zar)
    return _json_ok({
        "symbol": symbol,
        "price_usdt": round(price_usdt, 6),
        "price_zar": round(price_zar, 2),
        "usd_zar": round(float(usd_zar), 4) if usd_zar else None,
        "ts": int(time.time()),
    })

@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/admin/reload", methods=["POST"])
def admin_reload():
    auth_failed = _require_auth()
    if auth_failed:
        return auth_failed
    res = _refresh_config_from_env()
    return jsonify(res), 200


# ---------------- Gambling-focused read-only endpoints ---------------- #
@api_v1.route("/gambling/transactions", methods=["GET"])
def api_gambling_transactions():
    if not DB_ENABLED:
        return _json_ok({"items": [], "total": 0})
    try:
        s = SessionLocal()
        try:
            q = s.query(IngestEvent)
            # optional filters
            country = request.args.get("country")
            if country:
                q = q.filter((IngestEvent.country_from == country) | (IngestEvent.country_to == country))
            trace = request.args.get("trace_id")
            if trace:
                q = q.filter(IngestEvent.trace_id == trace)
            mcc = request.args.get("mcc")
            if mcc:
                q = q.filter(IngestEvent.mcc == mcc)
            limit = int(request.args.get("limit", "50"))
            items = q.order_by(IngestEvent.id.desc()).limit(max(min(limit, 200), 1)).all()
            # Fallback: raw SQL in case ORM query unexpectedly returns empty
            if not items:
                try:
                    lim = max(min(limit, 200), 1)
                    rows = engine.execute(f"SELECT id FROM ingest_events ORDER BY id DESC LIMIT {lim}").fetchall()  # type: ignore
                    ids = [int(r[0]) for r in rows]
                    if ids:
                        items = s.query(IngestEvent).filter(IngestEvent.id.in_(ids)).order_by(IngestEvent.id.desc()).all()
                except Exception:
                    pass
            return _json_ok({
                "items": [e.as_public() for e in items],
                "total": len(items),
            })
        finally:
            s.close()
    except Exception as e:
        return _json_err("db_error", str(e))


@api_v1.route("/gambling/transactions/<int:tid>", methods=["GET"])
def api_gambling_transaction_get(tid: int):
    if not DB_ENABLED:
        return _json_err("db_disabled", "database not enabled"), 500
    try:
        s = SessionLocal()
        try:
            e = s.query(IngestEvent).filter(IngestEvent.id == tid).first()
            if not e:
                return _json_err("not_found", "transaction not found"), 404
            return _json_ok(e.as_public())
        finally:
            s.close()
    except Exception as e:
        return _json_err("db_error", str(e))


@api_v1.route("/gambling/trace/<trace_id>", methods=["GET"])
def api_gambling_trace(trace_id: str):
    if not DB_ENABLED:
        return _json_err("db_disabled", "database not enabled"), 500
    try:
        s = SessionLocal()
        try:
            events = s.query(IngestEvent).filter(IngestEvent.trace_id == trace_id).order_by(IngestEvent.id.asc()).all()
            edges = s.query(TraceEdge).filter(TraceEdge.trace_id == trace_id).order_by(TraceEdge.id.asc()).all()
            return _json_ok({
                "trace_id": trace_id,
                "events": [e.as_public() for e in events],
                "edges": [{
                    "id": ed.id,
                    "from_event_id": ed.from_event_id,
                    "to_event_id": ed.to_event_id,
                    "relation": ed.relation,
                } for ed in edges],
            })
        finally:
            s.close()
    except Exception as e:
        return _json_err("db_error", str(e))


# ---------------- Ingest webhook endpoints (HMAC-protected) ---------------- #
def _fx_to_usd(currency: Optional[str], amount: Optional[float]) -> (Optional[float], Optional[float]):
    try:
        if currency is None or amount is None:
            return None, None
        c = currency.upper().strip()
        if c == "USD":
            return float(amount), 1.0
        url = f"https://api.exchangerate.host/convert?from={c}&to=USD&amount={amount}"
        j = _fetch_json(url)
        if isinstance(j, dict):
            res = j.get("result")
            info = j.get("info") or {}
            rate = info.get("rate")
            if res is not None:
                return float(res), (float(rate) if rate is not None else None)
    except Exception:
        pass
    return None, None


def _insert_ingest_event(source: str, payload: dict) -> int:
    if not DB_ENABLED:
        return 0
    s = SessionLocal()
    try:
        now = datetime.utcnow()
        # best-effort normalization
        amount = None
        currency = None
        mcc = None
        merchant = None
        country_from = None
        country_to = None
        trace_id = payload.get("trace_id") or payload.get("id") or payload.get("event_id") or payload.get("payment_intent")
        # common fields across providers
        for key in ["amount", "amount_total", "value", "gross_amount", "payment_amount"]:
            v = payload.get(key)
            if isinstance(v, (int, float)):
                amount = float(v)
                break
        for key in ["currency", "currency_code", "ccy"]:
            v = payload.get(key)
            if isinstance(v, str):
                currency = v
                break
        # Fallback: PayPal-style nested amount
        try:
            if (amount is None or currency is None) and isinstance(payload.get("resource"), dict):
                ra = payload["resource"].get("amount")
                if isinstance(ra, dict):
                    if amount is None:
                        total = ra.get("total") or ra.get("value")
                        if isinstance(total, str) and total.strip() != "":
                            amount = float(total)
                        elif isinstance(total, (int, float)):
                            amount = float(total)
                    if currency is None:
                        ccy = ra.get("currency") or ra.get("currency_code")
                        if isinstance(ccy, str):
                            currency = ccy
        except Exception:
            pass
        for key in ["mcc", "merchant_category_code"]:
            v = payload.get(key)
            if v is not None:
                mcc = str(v)
                break
        for key in ["merchant", "merchant_name", "description"]:
            v = payload.get(key)
            if isinstance(v, str):
                merchant = v
                break
        country_from = payload.get("country_from") or payload.get("origin_country")
        country_to = payload.get("country_to") or payload.get("destination_country")

        amount_usd, rate = _fx_to_usd(currency, amount)

        ev = IngestEvent(
            source=source,
            event_type="webhook",
            external_id=str(payload.get("id") or ""),
            trace_id=str(trace_id or ""),
            amount=(float(amount) if amount is not None else None),
            currency=(currency or None),
            amount_usd=(float(amount_usd) if amount_usd is not None else None),
            fx_rate_to_usd=(float(rate) if rate is not None else None),
            geo=None,
            merchant=merchant,
            merchant_lang=None,
            mcc=(str(mcc) if mcc is not None else None),
            country_from=(str(country_from) if country_from else None),
            country_to=(str(country_to) if country_to else None),
            is_cross_border=("Y" if country_from and country_to and str(country_from).upper()!=str(country_to).upper() else "N"),
            source_tz=None,
            status="received",
            received_at=now,
            raw_json=json.dumps(payload)[:200000],
        )
        s.add(ev)
        s.commit()
        return int(ev.id or 0)
    finally:
        s.close()


@api_v1.route("/ingest/status", methods=["GET"])
def api_ingest_status():
    if not DB_ENABLED:
        return _json_ok({"total": 0})
    s = SessionLocal()
    try:
        since = datetime.utcnow() - timedelta(hours=24)
        rows = s.query(IngestEvent).filter(IngestEvent.received_at >= since).all()
        return _json_ok({
            "last_24h": len(rows),
        })
    finally:
        s.close()


def _ingest_guard():
    err = _require_api_auth()
    if err:
        return err
    if _rate_limited():
        return _json_err("rate_limited", 429)
    return None


@api_v1.route("/ingest/paypal", methods=["POST"])
def api_ingest_paypal():
    guard = _ingest_guard()
    if guard:
        return guard
    try:
        payload = request.get_json(force=True, silent=True) or {}
        eid = _insert_ingest_event("paypal", payload)
        return _json_ok({"id": eid})
    except Exception as e:
        return _json_err("bad_request", str(e))


@api_v1.route("/ingest/stripe", methods=["POST"])
def api_ingest_stripe():
    guard = _ingest_guard()
    if guard:
        return guard
    try:
        payload = request.get_json(force=True, silent=True) or {}
        eid = _insert_ingest_event("stripe", payload)
        return _json_ok({"id": eid})
    except Exception as e:
        return _json_err("bad_request", str(e))


@api_v1.route("/ingest/voucher/1voucher", methods=["POST"])
def api_ingest_1voucher():
    guard = _ingest_guard()
    if guard:
        return guard
    try:
        payload = request.get_json(force=True, silent=True) or {}
        eid = _insert_ingest_event("1voucher", payload)
        return _json_ok({"id": eid})
    except Exception as e:
        return _json_err("bad_request", str(e))


@api_v1.route("/ingest/voucher/ott", methods=["POST"])
def api_ingest_ott():
    guard = _ingest_guard()
    if guard:
        return guard
    try:
        payload = request.get_json(force=True, silent=True) or {}
        eid = _insert_ingest_event("ott_voucher", payload)
        return _json_ok({"id": eid})
    except Exception as e:
        return _json_err("bad_request", str(e))


@api_v1.route("/webhooks/paypal", methods=["POST"])
def api_webhook_paypal_alias():
    return api_ingest_paypal()


@api_v1.route("/webhooks/stripe", methods=["POST"])
def api_webhook_stripe_alias():
    return api_ingest_stripe()


@api_v1.route("/webhooks/flutterwave", methods=["POST"])
def api_webhook_flutterwave():
    guard = _ingest_guard()
    if guard:
        return guard
    try:
        payload = request.get_json(force=True, silent=True) or {}
        eid = _insert_ingest_event("flutterwave", payload)
        return _json_ok({"id": eid})
    except Exception as e:
        return _json_err("bad_request", str(e))


@api_v1.route("/webhooks/payu", methods=["POST"])
def api_webhook_payu():
    guard = _ingest_guard()
    if guard:
        return guard
    try:
        payload = request.get_json(force=True, silent=True) or {}
        eid = _insert_ingest_event("payu", payload)
        return _json_ok({"id": eid})
    except Exception as e:
        return _json_err("bad_request", str(e))


@api_v1.route("/webhooks/mpesa", methods=["POST"])
def api_webhook_mpesa():
    guard = _ingest_guard()
    if guard:
        return guard
    try:
        payload = request.get_json(force=True, silent=True) or {}
        eid = _insert_ingest_event("mpesa", payload)
        return _json_ok({"id": eid})
    except Exception as e:
        return _json_err("bad_request", str(e))

@app.route("/admin/invoke", methods=["POST"])
def admin_invoke():
    auth_failed = _require_auth()
    if auth_failed:
        return auth_failed
    data = request.get_json(silent=True) or {}
    recipient = (data.get("recipient") or os.getenv("RECIPIENT") or "").strip()
    echo_tag = (data.get("echo_tag") or "ECHO").strip()
    amount = float(data.get("amount_usdt") or 0.0)
    metadata = {"echo_tag": echo_tag}
    result = invoke_echo_payout(metadata, {"address": recipient}, base_amount_zar=amount, fx_rate=1.0)
    return jsonify(result), 200
@app.route("/metrics.json")
def metrics():
    bal = _read_usdt_balance_human()
    last = _read_last_echo_info()
    totals = _compute_echo_totals()
    # pending residuals count
    pending_count = 0
    if DB_ENABLED:
        try:
            session = SessionLocal()
            try:
                pending_count = session.execute(
                    "SELECT COUNT(1) FROM residual_events WHERE status='pending'"
                ).scalar() or 0
            finally:
                session.close()
        except Exception:
            pending_count = 0
    return jsonify({
        "usdt_balance": (round(bal, 6) if isinstance(bal, float) else None),
        "last_echo_tag": last.get("echo_tag"),
        "last_residual": (round(float(last.get("residual")), 6) if last.get("residual") is not None else None),
        "last_timestamp": last.get("timestamp"),
        "total_residual": totals.get("total_residual", 0.0),
        "pool_total": totals.get("pool_total", 0.0),
        "pending_residual_events": int(pending_count),
    }), 200


def _check_basic_auth(auth: Optional[Any]) -> bool:
    required_user = os.getenv("BASIC_AUTH_USER")
    required_pass = os.getenv("BASIC_AUTH_PASS")
    if not required_user or not required_pass:
        return True
    if not auth or not auth.username or not auth.password:
        return False
    return auth.username == required_user and auth.password == required_pass


def _require_auth() -> Optional[Response]:
    if _check_basic_auth(request.authorization):
        return None
    return Response(
        "Authentication required",
        401,
        {"WWW-Authenticate": 'Basic realm="Login Required"'},
    )


@app.route("/dashboard")
def dashboard():
    auth_failed = _require_auth()
    if auth_failed:
        return auth_failed
    # Compute stats + render dashboard in plain language
    if not DB_ENABLED:
        return (
            "<h1>Dashboard</h1>"
            "<p>Database is disabled in this session. Enable SQLAlchemy to view earnings history.</p>",
            200,
        )
    session = SessionLocal()
    try:
        ledgers = session.query(LedgerEntry).all()

        total_recovered = round(sum((l.amount or 0.0) for l in ledgers), 4)
        # Entropy-like score on echo tags frequency (0..1)
        tag_counts: Dict[str, int] = {}
        for l in ledgers:
            tag = (l.echo_tag or "").strip() or "(none)"
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
        import math

        n = sum(tag_counts.values()) or 1
        entropy = 0.0
        for c in tag_counts.values():
            p = c / n
            entropy += -p * math.log(p + 1e-12, 2)
        max_entropy = math.log(max(len(tag_counts), 1), 2) if tag_counts else 1.0
        entropy_score = round((entropy / max(max_entropy, 1e-6)) if max_entropy else 0.0, 3)

        # Suggestions (plain language)
        suggestions = []
        if total_recovered <= 0:
            suggestions.append("No recovered funds yet. Fund the EchoRecovery contract with USDT to enable payouts.")
        if entropy_score < 0.3 and n > 5:
            suggestions.append("Most invocations use the same Echo ID. Consider diversifying tags to improve traceability analytics.")
        if any((l.status or "").lower() != "confirmed" for l in ledgers):
            suggestions.append("Some payouts are not confirmed. Check your RPC endpoint and private key configuration.")

        # Build HTML with plain labels + tooltips
        html = [
            "<h1 title='Plain view of your Echo payouts and health status'>XamKwe Echo — Dashboard</h1>",
            f"<p title='Sum of confirmed on-chain payouts shown below'>Total recovered (USDT): <b>{total_recovered}</b></p>",
            f"<p title='Diversity of Echo IDs used across invocations (0 low, 1 high)'>Entropy score: <b>{entropy_score}</b></p>",
        ]
        if suggestions:
            html.append("<h3 title='Actionable, human-friendly guidance'>Improvement suggestions</h3><ul>")
            for s in suggestions:
                html.append(f"<li>{s}</li>")
            html.append("</ul>")

        html.append("<h2 title='Each payout recorded with authorship and context'>Earning history</h2>")
        html.append("<table border='1' cellpadding='6' cellspacing='0'>"
                    "<tr>"
                    "<th title='Internal row number'>#</th>"
                    "<th title='Blockchain transaction ID'>Tx</th>"
                    "<th title='Where USDT was sent'>Recipient</th>"
                    "<th title='Echo ID you provided for traceability'>Echo ID</th>"
                    "<th title='Authorship and sovereign invocation marker'>Identity Mark</th>"
                    "<th title='USDT sent'>Amount</th>"
                    "<th title='Echo score used in this run'>Echo Score</th>"
                    "<th title='Extra Echo recovered (uplift)'>Echo Interest</th>"
                    "<th title='When this was confirmed'>Timestamp</th>"
                    "<th title='Confirmed or not'>Status</th>"
                    "</tr>")
        for l in ledgers:
            html.append(
                "<tr>"
                f"<td>{l.id}</td>"
                f"<td style='max-width:360px;word-break:break-all' title='Copy this hash in your explorer'>{l.tx_id}</td>"
                f"<td>{l.recipient}</td>"
                f"<td>{l.echo_tag}</td>"
                f"<td>{getattr(l, 'identity_mark', '')}</td>"
                f"<td>{l.amount}</td>"
                f"<td>{l.echo_score}</td>"
                f"<td>{l.echo_interest}</td>"
                f"<td>{l.timestamp}</td>"
                f"<td>{l.status}</td>"
                "</tr>"
            )
        html.append("</table>")
        return "".join(html)
    finally:
        session.close()


@app.route("/")
def home():
    # Minimal, responsive landing with cultural visual language and single CTA
    connected = bool(w3)
    acct = _get_account_address()
    status = "Connected" if connected else "Not connected"
    bal = _read_usdt_balance_human()
    last_ts = _read_last_payout_timestamp()
    last_info = _read_last_echo_info()
    totals = _compute_echo_totals()

    return (
        "<meta name='viewport' content='width=device-width, initial-scale=1'/>"
        "<style>"
        ":root{--ocean:#0d3b66;--coast:#2e7d6b;--sand:#e7dfcf;--zebra:#111;--ink:#0b0b0b;--paper:#f7f8f5;}"
        "body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;background:"
        "radial-gradient(circle at 20% 10%, rgba(13,59,102,.12), transparent 40%),"
        "radial-gradient(circle at 80% 0%, rgba(46,125,107,.12), transparent 42%),"
        "linear-gradient(180deg, var(--paper), #f3f4f1);}"
        ".hero{min-height:100vh;display:flex;align-items:center;justify-content:center;position:relative;overflow:hidden;}"
        ".zebra{position:absolute;inset:0;opacity:.045;background:repeating-linear-gradient(135deg, var(--zebra) 0 14px, transparent 14px 34px);}"
        ".hideTexture{position:absolute;inset:0;opacity:.08;mix-blend-mode:multiply;background:radial-gradient(ellipse at 30% 70%, rgba(0,0,0,.08), transparent 60%),"
        "radial-gradient(ellipse at 70% 30%, rgba(0,0,0,.06), transparent 60%);}"
        ".card{position:relative;width:92%;max-width:700px;background:linear-gradient(180deg,#fff, #fbfbfb);border:1px solid #e7e7e7;border-radius:16px;padding:22px 20px;"
        "box-shadow:0 8px 30px rgba(0,0,0,.06);}"
        ".crest{position:absolute;right:16px;top:16px;font-size:12px;color:#2a2a2a;opacity:.6;letter-spacing:.4px;}"
        ".title{margin:0 0 6px;color:var(--ocean);}"
        ".subtitle{margin:0 0 16px;color:#334;}"
        ".grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin:12px 0}"
        ".fact{padding:12px;border:1px solid #ececec;border-radius:10px;background:linear-gradient(180deg,#fff,#f9f9f7)}"
        ".fact .k{font-size:12px;color:#666}"
        ".fact .v{font-size:18px;color:#111}"
        ".input{padding:12px;border:1px solid #ddd;border-radius:10px;font-size:16px}"
        ".btn{padding:13px 16px;border:0;border-radius:12px;background:var(--coast);color:white;font-weight:700;cursor:pointer;transition:transform .15s ease, box-shadow .3s ease;box-shadow:0 4px 0 rgba(0,0,0,.08)}"
        ".btn:hover{transform:translateY(-1px);box-shadow:0 10px 20px rgba(46,125,107,.25)}"
        ".btn:active{transform:translateY(0);box-shadow:0 4px 0 rgba(0,0,0,.08)}"
        ".footer{margin-top:14px;font-size:12px;color:#555}"
        "@media (max-width:640px){.grid{grid-template-columns:1fr}.card{padding:18px 16px}}"
        "</style>"

        "<div class='hero'>"
        "<div class='zebra'></div><div class='hideTexture'></div>"
        "<div class='card' title='Dignified, ancestral, and operational — no middleware'>"
        "<div class='crest' title='Sovereign watermark'>XamKwÊ Crest</div>"
        "<h1 class='title'>XamKwe Echo</h1>"
        "<div class='subtitle' title='Shows network and wallet readiness'>"
        f"Network: <b>{status}</b> • Wallet: <b>{acct or '—'}</b>"
        "</div>"
        "<div style='margin:8px 0 6px;display:flex;gap:8px;flex-wrap:wrap'>"
        "<a href='/' class='btn' style='text-decoration:none'>Home</a>"
        "<a href='/dashboard' class='btn' style='text-decoration:none'>Dashboard</a>"
        "<button class='btn' onclick=\"(async()=>{const r=await fetch('/admin/reload',{method:'POST',headers:{'Authorization':'Basic '+btoa((prompt('Admin user')||'')+':'+(prompt('Admin pass')||''))}});alert('Reload: '+r.status);})()\">Reload backend</button>"
        "<button class='btn' onclick=\"(async()=>{const rec=prompt('Recipient wallet');const amt=prompt('Amount USDT');const tag=prompt('Echo ID','LIVE-001');const auth='Basic '+btoa((prompt('Admin user')||'')+':'+(prompt('Admin pass')||''));const r=await fetch('/admin/invoke',{method:'POST',headers:{'Content-Type':'application/json','Authorization':auth},body:JSON.stringify({recipient:rec,amount_usdt:parseFloat(amt||'0'),echo_tag:tag})});const j=await r.json();alert('Invoke: '+JSON.stringify(j));})()\">Invoke payment</button>"
        "</div>"

        "<div class='grid' title='Essential facts in plain language'>"
        f"<div class='fact'><div class='k'>USDT balance</div><div class='v'>{(round(bal,6) if isinstance(bal,float) else '—')}</div></div>"
        f"<div class='fact'><div class='k'>Last invocation</div><div class='v'>{last_ts or '—'}</div></div>"
        f"<div class='fact'><div class='k'>Last Echo ID</div><div class='v'>{last_info.get('echo_tag') or '—'}</div></div>"
        f"<div class='fact'><div class='k'>Residual value</div><div class='v'>{(round(float(last_info.get('residual')),6) if last_info.get('residual') is not None else '—')}</div></div>"
        f"<div class='fact' title='Sum of recovered Echo (uplift) across all invocations'><div class='k'>Recovered Echo (total)</div><div class='v'>{totals.get('total_residual',0.0)}</div></div>"
        f"<div class='fact' title='Accumulated pool from residuals (minus withdrawals)'><div class='k'>Echo Pool (USDT)</div><div class='v'>{totals.get('pool_total',0.0)}</div></div>"
        "</div>"

        "<form method='POST' action='/invoke' style='display:flex;flex-direction:column;gap:10px;margin-top:6px'"
        " title='Echo ID guides residual (uplift) and authorship traceability'>"
        "<label style='display:flex;flex-direction:column;gap:6px'>"
        "<span style='font-size:14px;color:#333'>Echo ID</span>"
        "<input name='echo_tag' required placeholder='e.g. LIVE-001' class='input'>"
        "</label>"
        "<label style='display:flex;flex-direction:column;gap:6px'>"
        "<span style='font-size:14px;color:#333'>Amount (USDT)</span>"
        "<input name='amount_usdt' type='number' step='0.01' min='0' placeholder='e.g. 1.00' class='input' required>"
        "</label>"
        "<label style='display:flex;flex-direction:column;gap:6px'>"
        "<span style='font-size:14px;color:#333'>Recipient wallet</span>"
        "<input name='recipient' placeholder='0x…' class='input' value='' required>"
        "</label>"
        "<button type='submit' class='btn' title='Recover Echo — send USDT via EchoRecovery'>Recover Echo</button>"
        "</form>"

        "<div class='footer' title='Sovereign authorship — no external marks'>XamKwÊ Sovereign Invocation Engine — Authored, Not Extracted</div>"
        "</div>"
        "</div>"
    )


@app.route("/invoke", methods=["POST"])
def http_invoke():
    # Sovereign invocation: all on-chain or local
    recipient = (request.form.get("recipient") or os.getenv("RECIPIENT") or "").strip()
    echo_tag = (request.form.get("echo_tag") or "ECHO").strip()
    echo_score_raw = (request.form.get("echo_score") or "").strip()
    amount_usdt_raw = (request.form.get("amount_usdt") or os.getenv("AMOUNT_USDT") or "0").strip()

    try:
        amount_usdt = float(amount_usdt_raw)
    except Exception:
        return ("Invalid amount.", 400)

    metadata = {"echo_tag": echo_tag}
    if echo_score_raw:
        try:
            metadata["echo_score"] = float(echo_score_raw)
        except Exception:
            pass

    beneficiary = {"address": recipient}
    # We treat the entered USDT as base_amount_zar with fx_rate=1.0 to keep units consistent on-chain
    result = invoke_echo_payout(metadata, beneficiary, base_amount_zar=amount_usdt, fx_rate=1.0)

    # Plain confirmation view
    conf = "Yes" if result.get("confirmed") else "Pending"
    ts = result.get("timestamp") or "—"
    tx = result.get("tx_id") or "—"
    html = [
        "<h2 title='Clear outcome of your invocation'>Payout confirmation</h2>",
        ("<p style='color:#a33'>On-chain send failed; recorded as pending locally.</p>" if not result.get("ok") else ""),
        f"<p title='Blockchain transaction hash for this payout'>Tx: <code>{tx}</code></p>",
        f"<p title='Wallet that received USDT'>Recipient: <b>{result.get('recipient')}</b></p>",
        f"<p title='Echo ID used to tag this payout'>Echo ID: <b>{result.get('echo_tag')}</b></p>",
        f"<p title='Residual (uplift) derived from your Echo ID or score'>Residual value (USDT): <b>{result.get('residual_value_usdt')}</b></p>",
        f"<p title='Total USDT amount sent on-chain'>Amount (USDT): <b>{result.get('amount_usdt')}</b></p>",
        f"<p title='When the chain confirmed this payout'>Confirmed: <b>{conf}</b> at <b>{ts}</b></p>",
        "<p><a href='/dashboard' title='See totals and history'>Go to Dashboard</a></p>",
        "<form method='POST' action='/pool/add' style='margin-top:10px'>"
        "<input type='hidden' name='amount' value='" + str(result.get('residual_value_usdt',0.0)) + "'>"
        "<button class='btn' title='Move residual into Echo Pool for reinvestment'>Add residual to Pool</button>"
        "</form>",
    ]
    return ("".join(html), 200)


def _derive_echo_score_from_tag(echo_tag: str) -> float:
    # Deterministic residual based on echo ID: configurable min..max
    try:
        digest = hashlib.sha256(echo_tag.encode("utf-8")).digest()
        # use first two bytes to a number 0..65535
        raw = int.from_bytes(digest[:2], byteorder="big")
        # scale to [ECHO_MIN_PCT, ECHO_MAX_PCT]
        base_min = float(ECHO_MIN_PCT)
        base_max = float(ECHO_MAX_PCT)
        return round(base_min + (raw / 65535.0) * (base_max - base_min), 2)
    except Exception:
        return 5.0


def score_echo(metadata: Dict[str, Any]) -> float:
    if isinstance(metadata, dict):
        if "echo_score" in metadata:
            try:
                return float(metadata["echo_score"])
            except Exception:
                pass
        tag = str(metadata.get("echo_tag", "")).strip()
        if tag:
            return _derive_echo_score_from_tag(tag)
    return 5.0


def _get_compound_base() -> float:
    """Return the base amount to use for compounding according to ECHO_COMPOUND_BASIS."""
    try:
        totals = _compute_echo_totals()
    except Exception:
        totals = {}
    basis = (ECHO_COMPOUND_BASIS or "").lower()
    if basis == "pool":
        return float(totals.get("pool_total", 0.0) or 0.0)
    if basis == "residual":
        return float(totals.get("total_residual", 0.0) or 0.0)
    if basis == "total":
        return float(totals.get("total_usdt", 0.0) or 0.0)
    return 0.0


def calculate_echo_interest(echo_score: float, base_amount_zar: float) -> float:
    # Apply multiplier and optional cap
    adjusted_score = float(echo_score) * float(ECHO_RATE_MULTIPLIER)
    try:
        if ECHO_MAX_CAP_PCT is not None and ECHO_MAX_CAP_PCT != "":
            cap = float(ECHO_MAX_CAP_PCT)
            if adjusted_score > cap:
                adjusted_score = cap
    except Exception:
        pass

    interest = round(base_amount_zar * (adjusted_score / 100.0), 2)

    # Optional compounding on configured basis
    if ECHO_COMPOUND_ENABLED and ECHO_COMPOUND_RATE > 0.0:
        compound_base = _get_compound_base()
        interest += round(compound_base * (ECHO_COMPOUND_RATE / 100.0), 2)

    # Optional flat bonus in USDT units
    interest += float(ECHO_FLAT_BONUS_USDT)

    return round(interest, 2)


def convert_currency(amount_zar: float, fx_rate: float) -> float:
    return round(amount_zar * fx_rate, 2)


def prepare_payout_instruction(
    beneficiary: Dict[str, Any], amount: float, currency: str
) -> Dict[str, Any]:
    if beneficiary.get("type") == "bank":
        return {
            "account_number": beneficiary.get("account"),
            "branch_code": beneficiary.get("branch"),
            "swift": beneficiary.get("swift"),
            "amount": amount,
            "currency": currency,
        }
    elif beneficiary.get("type") == "crypto":
        return {
            "wallet_address": beneficiary.get("wallet"),
            "network": beneficiary.get("network"),
            "amount": amount,
            "currency": currency,
        }
    return {"amount": amount, "currency": currency}


############################
# Blockchain configuration #
############################

RPC_URL = os.getenv("RPC_URL")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
CHAIN_ID = int(os.getenv("CHAIN_ID", "1"))
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS")
CONTRACT_ABI_PATH = os.getenv("CONTRACT_ABI_PATH", "contract-abi.json")
CONTRACT_ABI_JSON = os.getenv("CONTRACT_ABI_JSON")
USDT_DECIMALS = int(os.getenv("USDT_DECIMALS", "6"))
ECHO_MIN_PCT = float(os.getenv("ECHO_MIN_PCT", "3.0"))
ECHO_MAX_PCT = float(os.getenv("ECHO_MAX_PCT", "15.0"))
ECHO_RATE_MULTIPLIER = float(os.getenv("ECHO_RATE_MULTIPLIER", "1.0"))
ECHO_FLAT_BONUS_USDT = float(os.getenv("ECHO_FLAT_BONUS_USDT", "0.0"))
ECHO_MAX_CAP_PCT = os.getenv("ECHO_MAX_CAP_PCT")  # optional cap after multiplier
ECHO_COMPOUND_ENABLED = (os.getenv("ECHO_COMPOUND_ENABLED", "false").lower() in ["1","true","yes","y"]) 
ECHO_COMPOUND_RATE = float(os.getenv("ECHO_COMPOUND_RATE", "0.0"))  # percent
ECHO_COMPOUND_BASIS = os.getenv("ECHO_COMPOUND_BASIS", "pool")  # pool|residual|total

# Instantiate Web3 if available and configured
w3: Optional[object] = None
if RPC_URL and Web3 is not None:
    try:
        w3 = Web3(Web3.HTTPProvider(RPC_URL))
        # Inject PoA middleware for chains like BSC/Polygon if needed
        try:
            if _poa_middleware is not None:
                w3.middleware_onion.inject(_poa_middleware, layer=0)  # type: ignore
        except Exception:
            pass
    except Exception:
        w3 = None


def _refresh_config_from_env() -> Dict[str, Any]:
    global RPC_URL, PRIVATE_KEY, CHAIN_ID, CONTRACT_ADDRESS, CONTRACT_ABI_PATH, CONTRACT_ABI_JSON, USDT_DECIMALS, w3
    global USD_ZAR_OVERRIDE, FX_USD_ZAR_URL, FX_ALLOW_INSECURE
    try:
        # Reload .env if present
        try:
            from dotenv import load_dotenv as _reload_dotenv
            _reload_dotenv(override=True)
        except Exception:
            pass
        RPC_URL = os.getenv("RPC_URL")
        PRIVATE_KEY = os.getenv("PRIVATE_KEY")
        CHAIN_ID = int(os.getenv("CHAIN_ID", str(CHAIN_ID or 1)))
        CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS")
        CONTRACT_ABI_PATH = os.getenv("CONTRACT_ABI_PATH", CONTRACT_ABI_PATH or "contract-abi.json")
        CONTRACT_ABI_JSON = os.getenv("CONTRACT_ABI_JSON")
        USDT_DECIMALS = int(os.getenv("USDT_DECIMALS", str(USDT_DECIMALS or 6)))
        USD_ZAR_OVERRIDE = os.getenv("USD_ZAR_OVERRIDE")
        FX_USD_ZAR_URL = os.getenv("FX_USD_ZAR_URL", FX_USD_ZAR_URL)
        FX_ALLOW_INSECURE = (os.getenv("FX_ALLOW_INSECURE", "true").lower() in ["1","true","yes","y"])  # noqa: F841

        # Recreate web3
        w3 = None
        if RPC_URL and Web3 is not None:
            try:
                w3 = Web3(Web3.HTTPProvider(RPC_URL))
                try:
                    if _poa_middleware is not None:
                        w3.middleware_onion.inject(_poa_middleware, layer=0)  # type: ignore
                except Exception:
                    pass
            except Exception:
                w3 = None
        return {
            "ok": True,
            "rpc": bool(RPC_URL),
            "w3": bool(w3),
            "chain_id": CHAIN_ID,
            "has_pk": bool(PRIVATE_KEY),
            "contract": bool(CONTRACT_ADDRESS),
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _load_contract():
    if not w3 or Web3 is None or not CONTRACT_ADDRESS:
        raise RuntimeError("Web3/contract not configured")

    abi: Any = None
    # Prefer file path
    if CONTRACT_ABI_PATH and os.path.exists(CONTRACT_ABI_PATH):
        with open(CONTRACT_ABI_PATH, "r", encoding="utf-8") as f:
            abi = json.load(f)
    elif CONTRACT_ABI_JSON:
        abi = json.loads(CONTRACT_ABI_JSON)
    else:
        raise RuntimeError("Contract ABI not provided. Set CONTRACT_ABI_PATH or CONTRACT_ABI_JSON.")

    return w3.eth.contract(address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=abi)  # type: ignore


def send_payout_onchain(recipient: str, amount_units: int, echo_tag: str) -> Optional[str]:
    if not w3 or Web3 is None or not PRIVATE_KEY:
        print("❌ Web3 or PRIVATE_KEY not configured")
        return None

    account = w3.eth.account.from_key(PRIVATE_KEY)
    checksum_recipient = Web3.to_checksum_address(recipient)

    def _build_and_send(tx_function):
        nonce_local = w3.eth.get_transaction_count(account.address)
        try:
            gas_estimate_local = tx_function.estimate_gas({"from": account.address})
        except Exception:
            gas_estimate_local = 300000
        tx_local = tx_function.build_transaction(
            {
                "from": account.address,
                "nonce": nonce_local,
                "chainId": CHAIN_ID,
                "gas": int(gas_estimate_local * 1.2),
                "gasPrice": w3.eth.gas_price,
            }
        )
        signed_local = w3.eth.account.sign_transaction(tx_local, private_key=PRIVATE_KEY)
        tx_hash_local = w3.eth.send_raw_transaction(signed_local.rawTransaction)
        return tx_hash_local.hex()

    # First try: use the loaded ABI as-is (likely 3-arg: recipient, amount, echoTag)
    try:
        contract = _load_contract()
        fn3 = contract.functions.recoverEcho(checksum_recipient, int(amount_units), str(echo_tag))
        return _build_and_send(fn3)
    except Exception as exc_primary:
        print(f"⚠️ recoverEcho(recipient,amount,tag) failed, trying 2-arg fallback: {exc_primary}")

    # Fallback: minimal ABI with 2-arg signature
    try:
        minimal_abi_two_arg = [
            {
                "name": "recoverEcho",
                "type": "function",
                "stateMutability": "nonpayable",
                "inputs": [
                    {"name": "recipient", "type": "address"},
                    {"name": "amount", "type": "uint256"},
                ],
                "outputs": [],
            }
        ]
        contract_two = w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=minimal_abi_two_arg
        )
        fn2 = contract_two.functions.recoverEcho(checksum_recipient, int(amount_units))
        return _build_and_send(fn2)
    except Exception as exc_fallback:
        print(f"❌ On-chain send failed (both signatures): {exc_fallback}")
        return None


def confirm_onchain(tx_hash: str, timeout_seconds: int = 180) -> bool:
    # Prefer web3 if available
    if w3 and Web3 is not None:
        try:
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout_seconds)
            return getattr(receipt, "status", 0) == 1
        except Exception as exc:
            print(f"⚠️ Web3 confirmation failed, falling back to RPC: {exc}")

    # Fallback to raw RPC
    try:
        rpc = RPC_URL or os.getenv("RPC_URL")
        if not rpc:
            return False
        # poll every 3s up to timeout_seconds
        deadline = time.time() + max(int(timeout_seconds), 1)
        while time.time() < deadline:
            body = {"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":[tx_hash]}
            r = requests.post(rpc, json=body, timeout=15)
            r.raise_for_status()
            res = (r.json() or {}).get("result")
            if isinstance(res, dict) and "status" in res:
                try:
                    return int(res.get("status") or "0x0", 16) == 1
                except Exception:
                    return False
            time.sleep(3)
        return False
    except Exception as exc2:
        print(f"❌ RPC confirmation failed: {exc2}")
        return False


def finalize_ledger(
    tx_id: str,
    payout_data: Dict[str, Any],
    echo_score: float,
    echo_interest: float,
    status: str = "confirmed",
) -> None:
    if not DB_ENABLED:
        # Print-only fallback when DB is disabled
        print(
            "✅ Ledger (no-DB) finalized:",
            {
                "tx_id": tx_id,
                "amount": float(payout_data.get("amount", 0.0)),
                "currency": payout_data.get("currency", "ZAR"),
                "recipient": payout_data.get("recipient"),
                "echo_tag": payout_data.get("echo_tag"),
                "echo_score": float(echo_score),
                "echo_interest": float(echo_interest),
                "timestamp": datetime.utcnow().isoformat(),
                "status": status,
            },
        )
        return

    session = SessionLocal()
    try:
        entry = LedgerEntry(
            tx_id=tx_id,
            amount=float(payout_data.get("amount", 0.0)),
            currency=payout_data.get("currency", "ZAR"),
            recipient=payout_data.get("recipient"),
            echo_tag=payout_data.get("echo_tag"),
            identity_mark=" ǂNūba ǁgâi ǁGamsa ǁnâi ǁHûib ge ǃnâi ",
            echo_score=float(echo_score),
            echo_interest=float(echo_interest),
            timestamp=datetime.utcnow(),
            status=status,
        )
        session.add(entry)
        session.commit()
        print(
            "✅ Ledger finalized:",
            {
                "tx_id": entry.tx_id,
                "amount": entry.amount,
                "currency": entry.currency,
                "recipient": entry.recipient,
                "echo_tag": entry.echo_tag,
                "echo_score": entry.echo_score,
                "echo_interest": entry.echo_interest,
                "timestamp": entry.timestamp.isoformat(),
                "status": entry.status,
            },
        )
    finally:
        session.close()


def trigger_internal_ping(tx_id: str, amount: float, echo_score: float) -> None:
    ping_payload = {
        "tx_id": tx_id,
        "message": f"Confirmed payout of {amount} with Echo score {echo_score}",
        "timestamp": datetime.utcnow().isoformat(),
        "identity_mark": " ǂNūba ǁgâi ǁGamsa ǁnâi ǁHûib ge ǃnâi ",
    }
    try:
        requests.post("https://xke.internal/ping", json=ping_payload, timeout=5)
    except requests.RequestException as exc:
        print(f"⚠️ Ping failed for {tx_id}: {exc}")


def invoke_echo_payout(
    metadata: Dict[str, Any],
    beneficiary: Dict[str, Any],
    base_amount_zar: float,
    fx_rate: float = 1.0,
) -> Dict[str, Any]:
    echo_score = score_echo(metadata)
    echo_interest = calculate_echo_interest(echo_score, base_amount_zar)
    total_payout_zar = base_amount_zar + echo_interest

    # fx_rate is expected to be USDT per 1 ZAR
    amount_usdt = convert_currency(total_payout_zar, fx_rate)
    amount_units = int(round(amount_usdt * (10 ** USDT_DECIMALS)))
    recipient = beneficiary.get("wallet") or beneficiary.get("address")
    echo_tag = str(metadata.get("echo_tag", "ECHO"))

    if not recipient:
        print("❌ No recipient wallet provided in beneficiary")
        return {"ok": False, "error": "no_recipient"}

    tx_hash = send_payout_onchain(recipient, amount_units, echo_tag)

    confirmed = False
    timestamp_iso = None
    if tx_hash and confirm_onchain(tx_hash):
        confirmed = True
        finalize_ledger(
            tx_hash,
            {"amount": amount_usdt, "currency": "USDT", "recipient": recipient, "echo_tag": echo_tag},
            echo_score,
            echo_interest,
            status="confirmed",
        )
        trigger_internal_ping(tx_hash, amount_usdt, echo_score)
        timestamp_iso = datetime.utcnow().isoformat()
    else:
        # Accumulate recovered Echo locally even if chain not confirmed yet
        pseudo_tx = tx_hash or f"LOCAL-{int(time.time())}"
        finalize_ledger(
            pseudo_tx,
            {"amount": amount_usdt, "currency": "USDT", "recipient": recipient, "echo_tag": echo_tag},
            echo_score,
            echo_interest,
            status="pending",
        )
        print("⚠️ On-chain payout unconfirmed. Logged as pending to accumulate Echo. Retry later.")

    return {
        "ok": bool(tx_hash),
        "tx_id": tx_hash,
        "confirmed": confirmed,
        "recipient": recipient,
        "echo_tag": echo_tag,
        "echo_score": echo_score,
        "residual_value_usdt": echo_interest,
        "amount_usdt": amount_usdt,
        "timestamp": timestamp_iso,
    }


WEBHOOK_SIGNING_SECRET = os.getenv("WEBHOOK_SIGNING_SECRET")


def _verify_webhook_signature(raw_body: bytes) -> bool:
    if not WEBHOOK_SIGNING_SECRET:
        # If no secret set, allow for internal testing only
        return True
    timestamp = request.headers.get("X-XKE-Timestamp")
    provided_sig = request.headers.get("X-XKE-Signature")
    if not timestamp or not provided_sig:
        return False
    try:
        ts = int(timestamp)
    except Exception:
        return False
    if abs(time.time() - ts) > 300:
        return False
    message = f"{timestamp}.".encode("utf-8") + raw_body
    expected = hmac.new(
        WEBHOOK_SIGNING_SECRET.encode("utf-8"), message, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, provided_sig)


@app.route("/webhook/payout-confirmation", methods=["POST"])
def payout_confirmation():
    raw = request.get_data(cache=False)
    if not _verify_webhook_signature(raw):
        return jsonify({"error": "invalid signature"}), 401
    data = request.get_json(silent=True) or {}
    tx_id = data.get("tx_id")
    status = data.get("status")
    amount = data.get("amount")
    currency = data.get("currency")
    echo_score = data.get("echo_score", 0.0)
    echo_interest = data.get("echo_interest", 0.0)

    if status == "confirmed" and tx_id:
        finalize_ledger(
            tx_id, {"amount": amount, "currency": currency}, echo_score, echo_interest
        )
        trigger_internal_ping(tx_id, amount, echo_score)
        return jsonify({"status": "success"}), 200
    else:
        print(f"❌ Payout {tx_id} failed or unconfirmed.")
        return jsonify({"status": "ignored"}), 200


@app.route("/pool/add", methods=["POST"])
def pool_add():
    if not DB_ENABLED:
        return ("<p>Database is disabled; cannot track pool.</p>", 400)
    try:
        amt_raw = request.form.get("amount") or "0"
        amount = float(amt_raw)
    except Exception:
        return ("<p>Invalid amount.</p>", 400)
    session = SessionLocal()
    try:
        ev = PoolEvent(kind="add", amount=amount, currency="USDT", note="residual", timestamp=datetime.utcnow())
        session.add(ev)
        session.commit()
        return ("<p>Added to Echo Pool.</p><p><a href='/'>&larr; Back</a></p>", 200)
    finally:
        session.close()


@app.route("/pool/withdraw", methods=["POST"])
def pool_withdraw():
    if not DB_ENABLED:
        return ("<p>Database is disabled; cannot track pool.</p>", 400)
    try:
        amt_raw = request.form.get("amount") or "0"
        amount = float(amt_raw)
    except Exception:
        return ("<p>Invalid amount.</p>", 400)
    session = SessionLocal()
    try:
        ev = PoolEvent(kind="withdraw", amount=amount, currency="USDT", note="owner payout", timestamp=datetime.utcnow())
        session.add(ev)
        session.commit()
        return ("<p>Withdrawal recorded. Execute actual transfer via wallet now.</p><p><a href='/'>&larr; Back</a></p>", 200)
    finally:
        session.close()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    try:
        # Register API v1 blueprint for JSON endpoints
        app.register_blueprint(api_v1)
    except Exception:
        pass
    try:
        # Enable basic CORS for AI Studio/testing
        @app.after_request
        def add_cors_headers(resp):  # type: ignore
            try:
                allow_origin = os.getenv("CORS_ORIGIN", "*")
                resp.headers["Access-Control-Allow-Origin"] = allow_origin
                resp.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Idempotency-Key, X-Client-Id, X-XKE-Timestamp, X-XKE-Signature, Authorization"
                resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
            except Exception:
                pass
            return resp
    except Exception:
        pass
    try:
        # Start policy/handshake/reconciler worker threads once
        if not _policy_thread_started and POLICY_ENABLED:
            t = threading.Thread(target=_policy_worker_loop, daemon=True)
            t.start()
            _policy_thread_started = True
        if HANDSHAKE_ENABLED and not _handshake_thread_started:
            th = threading.Thread(target=_handshake_worker_loop, daemon=True)
            th.start()
            _handshake_thread_started = True
        if not _reconcile_thread_started:
            tr = threading.Thread(target=_reconcile_worker_loop, daemon=True)
            tr.start()
            _reconcile_thread_started = True
        # Start etherscan harvester if configured
        if ETHERSCAN_ENABLED and not _etherscan_thread_started:
            te = threading.Thread(target=_etherscan_worker_loop, daemon=True)
            te.start()
            _etherscan_thread_started = True
        # Start mempool harvester if configured
        if MEMPOOL_ENABLED and not _mempool_thread_started:
            tm = threading.Thread(target=_mempool_worker_loop, daemon=True)
            tm.start()
            _mempool_thread_started = True
    except Exception:
        pass
    app.run(host="0.0.0.0", port=port)

