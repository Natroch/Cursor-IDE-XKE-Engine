from __future__ import annotations

import json
import os
from typing import Optional

from dotenv import load_dotenv
from web3 import Web3


def load_abi(path: Optional[str]) -> Optional[object]:
    if not path:
        return None
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def get_contract_usdt_address(w3: Web3, contract_address: str, abi: Optional[object]) -> Optional[str]:
    if not abi:
        return None
    try:
        c = w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=abi)
    except Exception:
        return None
    # Try common getters
    for name in ("usdt", "usdtToken", "token"):
        try:
            fn = getattr(c.functions, name)
        except AttributeError:
            continue
        try:
            val = fn().call()
            if isinstance(val, str) and val.startswith("0x"):
                return Web3.to_checksum_address(val)
        except Exception:
            continue
    return None


def main() -> int:
    load_dotenv()
    rpc = os.getenv("RPC_URL")
    contract_address = os.getenv("CONTRACT_ADDRESS")
    abi_path = os.getenv("CONTRACT_ABI_PATH", "contract-abi.json")

    if not rpc or not contract_address:
        print("❌ Missing RPC_URL or CONTRACT_ADDRESS")
        return 2

    w3 = Web3(Web3.HTTPProvider(rpc))
    try:
        from web3.middleware import geth_poa_middleware  # type: ignore
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except Exception:
        pass

    abi = load_abi(abi_path)

    # Determine USDT token address
    usdt = os.getenv("USDT_ADDRESS") or get_contract_usdt_address(w3, contract_address, abi)
    if not usdt:
        print("❌ Could not determine USDT token address. Set USDT_ADDRESS in .env.")
        return 2

    # Minimal ERC20 ABI
    erc20_abi = [
        {"name": "decimals", "outputs": [{"type": "uint8"}], "inputs": [], "stateMutability": "view", "type": "function"},
        {"name": "balanceOf", "outputs": [{"type": "uint256"}], "inputs": [{"name": "", "type": "address"}], "stateMutability": "view", "type": "function"},
    ]
    t = w3.eth.contract(address=Web3.to_checksum_address(usdt), abi=erc20_abi)

    # Determine decimals
    try:
        decimals = int(os.getenv("USDT_DECIMALS") or t.functions.decimals().call())
    except Exception:
        decimals = 18

    raw = t.functions.balanceOf(Web3.to_checksum_address(contract_address)).call()
    human = raw / (10 ** decimals)
    print(f"Recovered balance: {human} USDT (raw {raw})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

