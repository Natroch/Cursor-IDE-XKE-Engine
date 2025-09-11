from __future__ import annotations

import os
import json
import hashlib
import requests
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


def keccak_selector(signature: str) -> str:
    h = hashlib.sha3_256(signature.encode("utf-8")).hexdigest()
    return "0x" + h[:8]


def eth_call(rpc_url: str, to: str, data: str) -> str:
    headers = {"Content-Type": "application/json"}
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{"to": to, "data": data}, "latest"],
    }
    r = requests.post(rpc_url, json=payload, headers=headers, timeout=30)
    r.raise_for_status()
    res = r.json()
    if "error" in res:
        raise RuntimeError(res["error"])
    return res.get("result", "0x")


def read_address_from_contract(rpc_url: str, contract: str, fn_name: str) -> Optional[str]:
    selector = keccak_selector(f"{fn_name}()")
    data = selector
    out = eth_call(rpc_url, contract, data)
    if out and out.startswith("0x") and len(out) >= 2 + 64:
        # address is the rightmost 20 bytes of the 32-byte word
        addr_hex = out[-40:]
        return "0x" + addr_hex
    return None


def read_decimals(rpc_url: str, token: str) -> Optional[int]:
    selector = keccak_selector("decimals()")
    out = eth_call(rpc_url, token, selector)
    if out and out.startswith("0x"):
        try:
            return int(out, 16)
        except Exception:
            return None
    return None


def read_balance_of(rpc_url: str, token: str, holder: str) -> int:
    selector = keccak_selector("balanceOf(address)")[2:]  # without 0x
    addr = holder.lower().replace("0x", "").rjust(64, "0")
    data = "0x" + selector + addr
    out = eth_call(rpc_url, token, data)
    if not (out and out.startswith("0x")):
        return 0
    return int(out, 16)


def main() -> int:
    rpc = os.getenv("RPC_URL")
    contract = os.getenv("CONTRACT_ADDRESS")
    token = os.getenv("USDT_ADDRESS")
    if not rpc or not contract:
        print("❌ Missing RPC_URL or CONTRACT_ADDRESS")
        return 2

    # Try to discover token from contract if not provided
    if not token:
        for name in ("usdt", "usdtToken", "token"):
            try:
                token = read_address_from_contract(rpc, contract, name)
                if token:
                    break
            except Exception:
                continue
    if not token:
        print("❌ Could not determine USDT token address. Set USDT_ADDRESS in environment.")
        return 2

    # Read decimals (fallback to env or 18)
    decimals_env = os.getenv("USDT_DECIMALS")
    decimals: Optional[int] = None
    if decimals_env:
        try:
            decimals = int(decimals_env)
        except Exception:
            decimals = None
    if decimals is None:
        decimals = read_decimals(rpc, token) or 18

    raw = read_balance_of(rpc, token, contract)
    human = raw / (10 ** int(decimals))
    print(f"Recovered balance: {human} USDT (raw {raw})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

