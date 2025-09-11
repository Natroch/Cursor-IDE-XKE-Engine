from dotenv import load_dotenv
load_dotenv()

import os
from decimal import Decimal
from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account import Account


def main() -> None:
    rpc_url = os.getenv("RPC_URL")
    private_key = os.getenv("PRIVATE_KEY")
    recipient = os.getenv("RECIPIENT")
    amount_eth = os.getenv("AMOUNT_ETH")
    if not (rpc_url and private_key and recipient and amount_eth):
        raise SystemExit("Missing one of: RPC_URL, PRIVATE_KEY, RECIPIENT, AMOUNT_ETH")

    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 60}))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    acct = Account.from_key(private_key)
    value = int(Decimal(amount_eth) * 10 ** 18)
    nonce = w3.eth.get_transaction_count(acct.address)

    latest = w3.eth.get_block("latest")
    base_fee = latest.get("baseFeePerGas") or w3.to_wei("5", "gwei")
    try:
        priority = w3.eth.max_priority_fee
    except Exception:
        priority = w3.to_wei("2", "gwei")
    max_fee = base_fee * 2 + priority

    tx = {
        "to": Web3.to_checksum_address(recipient),
        "from": acct.address,
        "value": value,
        "nonce": nonce,
        "maxFeePerGas": int(max_fee),
        "maxPriorityFeePerGas": int(priority),
        "gas": 21_000,
    }
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


if __name__ == "__main__":
    main()

