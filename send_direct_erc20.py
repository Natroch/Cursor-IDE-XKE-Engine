from dotenv import load_dotenv
load_dotenv()

import os
from decimal import Decimal
from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account import Account


ERC20_ABI = [
    {
        "constant": False,
        "inputs": [
            {"name": "_to", "type": "address"},
            {"name": "_value", "type": "uint256"},
        ],
        "name": "transfer",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [{"name": "", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
]


def main() -> None:
    rpc_url = os.getenv("RPC_URL")
    private_key = os.getenv("PRIVATE_KEY")
    token_address = os.getenv("TOKEN_ADDRESS") or os.getenv("USDT_ADDRESS")
    recipient = os.getenv("RECIPIENT")
    amount_str = os.getenv("AMOUNT_TOKEN") or os.getenv("AMOUNT_USDT")
    decimals_env = os.getenv("USDT_DECIMALS")

    if not (rpc_url and private_key and token_address and recipient and amount_str):
        raise SystemExit("Missing one of: RPC_URL, PRIVATE_KEY, TOKEN_ADDRESS/USDT_ADDRESS, RECIPIENT, AMOUNT_TOKEN/AMOUNT_USDT")

    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 60}))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    account = Account.from_key(private_key)
    token = w3.eth.contract(Web3.to_checksum_address(token_address), abi=ERC20_ABI)

    # Determine decimals
    if decimals_env:
        decimals = int(decimals_env)
    else:
        decimals = token.functions.decimals().call()

    amount_units = int(Decimal(amount_str) * (10 ** decimals))

    nonce = w3.eth.get_transaction_count(account.address)
    latest_block = w3.eth.get_block("latest")
    base_fee = latest_block.get("baseFeePerGas") or w3.to_wei("5", "gwei")
    try:
        priority = w3.eth.max_priority_fee
    except Exception:
        priority = w3.to_wei("2", "gwei")
    max_fee = base_fee * 2 + priority

    # Gas limit estimation first, so build_transaction doesn't try to auto-estimate
    try:
        gas_limit = token.functions.transfer(
            Web3.to_checksum_address(recipient), amount_units
        ).estimate_gas({"from": account.address})
        gas_limit = int(gas_limit * 1.2)
    except Exception:
        gas_limit = 100_000

    tx = token.functions.transfer(Web3.to_checksum_address(recipient), amount_units).build_transaction(
        {
            "from": account.address,
            "nonce": nonce,
            "maxFeePerGas": int(max_fee),
            "maxPriorityFeePerGas": int(priority),
            "gas": gas_limit,
        }
    )

    signed = account.sign_transaction(tx)
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

