import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account import Account
from solcx import install_solc, set_solc_version, compile_standard


def fail(message: str) -> None:
    print(f"ERROR: {message}")
    sys.exit(1)


def read_private_key() -> str:
    env_key = os.getenv("PRIVATE_KEY")
    if env_key and env_key.startswith("0x") and len(env_key) == 66:
        return env_key
    pk_path = Path("pk.txt")
    if pk_path.exists():
        content = pk_path.read_text(encoding="utf-8").strip()
        if content and not content.startswith("0x"):
            content = "0x" + content
        if len(content) == 66:
            return content
    fail("PRIVATE_KEY not set and pk.txt missing or invalid (need 0x + 64 hex)")


def compile_contract(source_path: Path, solc_version: str = "0.8.20") -> dict:
    try:
        install_solc(solc_version)
    except Exception:
        pass
    set_solc_version(solc_version)
    source = source_path.read_text(encoding="utf-8")
    input_json = {
        "language": "Solidity",
        "sources": {source_path.name: {"content": source}},
        "settings": {
            "optimizer": {"enabled": True, "runs": 200},
            "outputSelection": {"*": {"*": ["abi", "evm.bytecode"]}},
        },
    }
    result = compile_standard(input_json)
    contracts = result["contracts"][source_path.name]
    artifact = contracts["EchoRecovery"]
    abi = artifact["abi"]
    bytecode = artifact["evm"]["bytecode"]["object"]
    return {"abi": abi, "bytecode": bytecode}


def main() -> None:
    load_dotenv()
    rpc_url = os.getenv("RPC_URL") or fail("RPC_URL not set")
    token_address = os.getenv("USDT_ADDRESS") or fail("USDT_ADDRESS not set (token for constructor)")
    if not Web3.is_address(token_address):
        fail("USDT_ADDRESS is not a valid address")

    pk = read_private_key()
    acct = Account.from_key(pk)
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 60}))
    # Add POA middleware for BSC/opBNB if applicable; harmless on Ethereum
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    chain_id_env = os.getenv("CHAIN_ID")
    try:
        chain_id = int(chain_id_env) if chain_id_env else w3.eth.chain_id
    except Exception:
        chain_id = w3.eth.chain_id

    print(f"Using RPC: {rpc_url}")
    print(f"From: {acct.address}")
    print(f"Chain ID: {chain_id}")
    print(f"Constructor token: {token_address}")

    # Show balance and nonce to help diagnose issues
    try:
        balance_wei = w3.eth.get_balance(acct.address)
        print(f"Balance: {w3.from_wei(balance_wei, 'ether')} ETH")
    except Exception:
        print("Balance: <unavailable>")

    source_path = Path("contracts") / "EchoRecovery.sol"
    if not source_path.exists():
        fail(f"Missing source: {source_path}")

    artifact = compile_contract(source_path)
    abi = artifact["abi"]
    bytecode = artifact["bytecode"]

    contract = w3.eth.contract(abi=abi, bytecode=bytecode)
    nonce = w3.eth.get_transaction_count(acct.address)
    print(f"Nonce: {nonce}")

    # Derive EIP-1559 gas from latest base fee
    latest_block = w3.eth.get_block("latest")
    base_fee = latest_block.get("baseFeePerGas") or w3.to_wei("20", "gwei")
    try:
        priority_fee = w3.eth.max_priority_fee
    except Exception:
        priority_fee = w3.to_wei("3", "gwei")
    max_fee = base_fee * 2 + priority_fee

    # Estimate gas for constructor to avoid overspending
    try:
        est_gas = contract.constructor(token_address).estimate_gas({"from": acct.address})
    except Exception:
        est_gas = 800_000
    gas_limit = int(est_gas * 1.25)

    tx = contract.constructor(token_address).build_transaction(
        {
            "from": acct.address,
            "nonce": nonce,
            "chainId": chain_id,
            "gas": gas_limit,
            "maxFeePerGas": int(max_fee),
            "maxPriorityFeePerGas": int(priority_fee),
        }
    )
    signed = acct.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
    print(f"Submitted tx: {tx_hash.hex()}")
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300, poll_latency=5)
    if receipt.status != 1:
        fail("Deployment failed; status != 1")
    print(f"Contract deployed at: {receipt.contractAddress}")
    # Write a small file for convenience
    Path("deployed_address.txt").write_text(receipt.contractAddress, encoding="utf-8")
    # Also emit minimal ABI for runtime interactions
    Path("deployed_abi.json").write_text(json.dumps(abi, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()

