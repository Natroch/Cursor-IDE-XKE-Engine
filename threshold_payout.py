from dotenv import load_dotenv
load_dotenv()

import os
from decimal import Decimal
from typing import Tuple
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


def main() -> None:
    # Settings
    threshold_usdt = Decimal(os.getenv("THRESHOLD_USDT", "5000"))
    payout_usdt = Decimal(os.getenv("PAYOUT_USDT", "1000"))
    gas_bonus_usdt = Decimal(os.getenv("GAS_USDT_PER_TRIGGER", "100"))
    gas_recipient = os.getenv("GAS_RECIPIENT")  # e.g., your MetaMask USDT address
    recipient = os.getenv("RECIPIENT")
    if not recipient:
        raise SystemExit("Missing RECIPIENT")

    w3, pk = _connect()
    acct = Account.from_key(pk)
    token = _token_contract(w3)
    decimals = _decimals(token)

    # Read balance
    bal_units = token.functions.balanceOf(acct.address).call()
    bal = Decimal(bal_units) / (Decimal(10) ** decimals)
    print(f"USDT balance: {bal}")

    if bal < threshold_usdt:
        print({"ok": False, "reason": "below_threshold", "threshold": str(threshold_usdt), "balance": str(bal)})
        return

    # Ensure we have enough for both main payout and gas reserve
    need_total = payout_usdt + (gas_bonus_usdt if gas_recipient else Decimal(0))
    if bal < need_total:
        print({"ok": False, "reason": "insufficient_for_both", "need": str(need_total), "balance": str(bal)})
        return

    # Build and send transfer for payout_usdt
    amount_units = int(payout_usdt * (Decimal(10) ** decimals))
    nonce = w3.eth.get_transaction_count(acct.address)
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
            Web3.to_checksum_address(recipient), amount_units
        ).estimate_gas({"from": acct.address})
        gas_limit = int(gas_limit * 1.2)
    except Exception:
        gas_limit = 100_000

    tx = token.functions.transfer(Web3.to_checksum_address(recipient), amount_units).build_transaction(
        {
            "from": acct.address,
            "nonce": nonce,
            "maxFeePerGas": int(max_fee),
            "maxPriorityFeePerGas": int(priority),
            "gas": gas_limit,
        }
    )

    signed = acct.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
    print("submitted:", tx_hash.hex())
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300, poll_latency=5)
    print({
        "ok": receipt.status == 1,
        "tx": tx_hash.hex(),
        "gasUsed": receipt.gasUsed,
        "block": receipt.blockNumber,
    })

    # Optional: send gas reserve in USDT to MetaMask wallet (or specified address)
    if gas_recipient:
        try:
            gas_units = int(gas_bonus_usdt * (Decimal(10) ** decimals))
            # estimate
            try:
                gas2 = token.functions.transfer(
                    Web3.to_checksum_address(gas_recipient), gas_units
                ).estimate_gas({"from": acct.address})
                gas2 = int(gas2 * 1.2)
            except Exception:
                gas2 = 100_000

            tx2 = token.functions.transfer(Web3.to_checksum_address(gas_recipient), gas_units).build_transaction(
                {
                    "from": acct.address,
                    "nonce": w3.eth.get_transaction_count(acct.address),
                    "maxFeePerGas": int(max_fee),
                    "maxPriorityFeePerGas": int(priority),
                    "gas": gas2,
                }
            )
            signed2 = acct.sign_transaction(tx2)
            tx_hash2 = w3.eth.send_raw_transaction(signed2.rawTransaction)
            print("submitted_gas_bonus:", tx_hash2.hex())
            receipt2 = w3.eth.wait_for_transaction_receipt(tx_hash2, timeout=300, poll_latency=5)
            print({
                "ok": receipt2.status == 1,
                "tx": tx_hash2.hex(),
                "gasUsed": receipt2.gasUsed,
                "block": receipt2.blockNumber,
                "kind": "gas_bonus_usdt",
            })
        except Exception as exc:
            print({"ok": False, "error": "gas_bonus_failed", "reason": str(exc)})


if __name__ == "__main__":
    main()

