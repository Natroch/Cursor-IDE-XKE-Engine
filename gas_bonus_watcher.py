from dotenv import load_dotenv
load_dotenv()

import os
import json
import time
from decimal import Decimal
from typing import Tuple
import requests
from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account import Account


ERC20_ABI = [
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
    {"constant": True, "inputs": [{"name": "", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    {"constant": False, "inputs": [{"name": "_to", "type": "address"}, {"name": "_value", "type": "uint256"}], "name": "transfer", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
]


def _connect() -> Tuple[Web3, str]:
    rpc_url = os.getenv("RPC_URL")
    private_key = os.getenv("PRIVATE_KEY")
    if not (rpc_url and private_key):
        raise SystemExit("Missing RPC_URL or PRIVATE_KEY")
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 60}))
    try:
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except Exception:
        pass
    return w3, private_key


def _token_contract(w3: Web3):
    token_address = os.getenv("TOKEN_ADDRESS") or os.getenv("USDT_ADDRESS")
    if not token_address:
        raise SystemExit("Missing TOKEN_ADDRESS/USDT_ADDRESS")
    return w3.eth.contract(Web3.to_checksum_address(token_address), abi=ERC20_ABI)


def _decimals(token) -> int:
    envd = os.getenv("USDT_DECIMALS")
    if envd:
        return int(envd)
    return int(token.functions.decimals().call())


def _notify(payload: dict) -> None:
    url = os.getenv("WEBHOOK_URL")
    if not url:
        print({"notify": payload})
        return
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as exc:
        print({"notify_error": str(exc)})


def _load_state(path: str) -> dict:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {"last_level": 0}


def _save_state(path: str, state: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception:
        pass


def _send_usdt(w3: Web3, acct, token, to_addr: str, amount_units: int) -> str:
    latest_block = w3.eth.get_block("latest")
    base_fee = latest_block.get("baseFeePerGas") or w3.to_wei("5", "gwei")
    try:
        priority = w3.eth.max_priority_fee
    except Exception:
        priority = w3.to_wei("2", "gwei")
    max_fee = base_fee * 2 + priority

    # estimate gas
    try:
        gas_limit = token.functions.transfer(
            Web3.to_checksum_address(to_addr), amount_units
        ).estimate_gas({"from": acct.address})
        gas_limit = int(gas_limit * 1.2)
    except Exception:
        gas_limit = 100_000

    tx = token.functions.transfer(Web3.to_checksum_address(to_addr), amount_units).build_transaction(
        {
            "from": acct.address,
            "nonce": w3.eth.get_transaction_count(acct.address),
            "maxFeePerGas": int(max_fee),
            "maxPriorityFeePerGas": int(priority),
            "gas": gas_limit,
        }
    )
    signed = acct.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
    return tx_hash.hex()


def main() -> None:
    step = Decimal(os.getenv("GAS_LEVEL_STEP", "1000"))
    bonus = Decimal(os.getenv("GAS_BONUS_USDT", "50"))
    gas_recipient = os.getenv("GAS_RECIPIENT")
    state_file = os.getenv("GAS_STATE_FILE", "gas_state.json")
    poll_seconds = int(os.getenv("GAS_POLL_SECONDS", "60"))

    if not gas_recipient:
        raise SystemExit("Missing GAS_RECIPIENT (your MetaMask USDT address)")

    w3, pk = _connect()
    acct = Account.from_key(pk)
    token = _token_contract(w3)
    decimals = _decimals(token)

    state = _load_state(state_file)
    print({"ok": True, "msg": "gas bonus watcher started", "step": str(step), "bonus": str(bonus), "poll": poll_seconds})

    while True:
        try:
            bal_units = token.functions.balanceOf(acct.address).call()
            bal = Decimal(bal_units) / (Decimal(10) ** decimals)
            level = int(bal // step)
            last_level = int(state.get("last_level", 0))
            print({"balance": str(bal), "level": level, "last_level": last_level})

            if level > last_level:
                # catch up for each new full step crossed
                for lv in range(last_level + 1, level + 1):
                    amount_units = int(bonus * (Decimal(10) ** decimals))
                    try:
                        txh = _send_usdt(w3, acct, token, gas_recipient, amount_units)
                        _notify({
                            "event": "gas_bonus_sent",
                            "level": lv,
                            "bonus": float(bonus),
                            "tx": txh,
                            "ts": int(time.time()),
                        })
                        print({"ok": True, "tx": txh, "bonus": float(bonus), "level": lv})
                    except Exception as exc:
                        print({"ok": False, "error": str(exc), "level": lv})
                        break
                state["last_level"] = level
                _save_state(state_file, state)

        except Exception as exc:
            print({"ok": False, "error": str(exc)})

        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()

