from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from web3 import Web3


class Config(BaseModel):
    rpc_url: str = Field(..., alias="RPC_URL")
    private_key: str = Field(..., alias="PRIVATE_KEY")
    chain_id: int = Field(..., alias="CHAIN_ID")
    contract_address: str = Field(..., alias="CONTRACT_ADDRESS")
    contract_abi_path: Optional[str] = Field(None, alias="CONTRACT_ABI_PATH")
    contract_abi_json: Optional[str] = Field(None, alias="CONTRACT_ABI_JSON")
    usdt_decimals: int = Field(6, alias="USDT_DECIMALS")

    @field_validator("private_key")
    @classmethod
    def _pk_hex(cls, v: str) -> str:
        if not isinstance(v, str) or not v.startswith("0x") or len(v) != 66:
            raise ValueError("PRIVATE_KEY must be 0x + 64 hex")
        int(v[2:], 16)  # will raise if invalid
        return v

    @field_validator("contract_address")
    @classmethod
    def _addr_hex(cls, v: str) -> str:
        if not isinstance(v, str) or not v.startswith("0x"):
            raise ValueError("CONTRACT_ADDRESS must be hex address")
        return v


def load_config() -> Config:
    # allow dotenv to be pre-loaded by main entry
    env = {k: os.getenv(k) for k in (
        "RPC_URL",
        "PRIVATE_KEY",
        "CHAIN_ID",
        "CONTRACT_ADDRESS",
        "CONTRACT_ABI_PATH",
        "CONTRACT_ABI_JSON",
        "USDT_DECIMALS",
    )}
    return Config.model_validate(env)


@dataclass
class Web3Client:
    w3: Web3

    @staticmethod
    def connect(rpc_url: str) -> "Web3Client":
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        try:
            from web3.middleware import geth_poa_middleware  # type: ignore
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        except Exception:
            pass
        return Web3Client(w3=w3)

    def contract(self, address: str, abi: Any):
        return self.w3.eth.contract(address=Web3.to_checksum_address(address), abi=abi)


def load_abi(cfg: Config) -> Any:
    if cfg.contract_abi_path and os.path.exists(cfg.contract_abi_path):
        with open(cfg.contract_abi_path, "r", encoding="utf-8") as f:
            return json.load(f)
    if cfg.contract_abi_json:
        return json.loads(cfg.contract_abi_json)
    raise RuntimeError("Missing ABI: set CONTRACT_ABI_PATH or CONTRACT_ABI_JSON")


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8),
       retry=retry_if_exception_type(Exception))
def build_and_send(w3: Web3, private_key: str, chain_id: int, fn):
    account = w3.eth.account.from_key(private_key)
    nonce = w3.eth.get_transaction_count(account.address)
    try:
        gas_est = fn.estimate_gas({"from": account.address})
    except Exception:
        gas_est = 300000
    tx = fn.build_transaction({
        "from": account.address,
        "nonce": nonce,
        "chainId": chain_id,
        "gas": int(gas_est * 1.2),
        "gasPrice": w3.eth.gas_price,
    })
    signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
    tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
    return tx_hash.hex()


def payout(recipient: str, amount_units: int, echo_tag: str) -> Optional[str]:
    cfg = load_config()
    client = Web3Client.connect(cfg.rpc_url)
    abi = load_abi(cfg)
    contract = client.contract(cfg.contract_address, abi)

    # Prefer 3-arg; fallback to 2-arg
    try:
        fn = contract.functions.recoverEcho(Web3.to_checksum_address(recipient), int(amount_units), str(echo_tag))
        return build_and_send(client.w3, cfg.private_key, cfg.chain_id, fn)
    except Exception:
        minimal = [{
            "name": "recoverEcho", "type": "function", "stateMutability": "nonpayable",
            "inputs": [{"name": "recipient", "type": "address"}, {"name": "amount", "type": "uint256"}],
            "outputs": []
        }]
        contract2 = client.contract(cfg.contract_address, minimal)
        fn2 = contract2.functions.recoverEcho(Web3.to_checksum_address(recipient), int(amount_units))
        return build_and_send(client.w3, cfg.private_key, cfg.chain_id, fn2)


def confirm(tx_hash: str, timeout: int = 180) -> bool:
    cfg = load_config()
    client = Web3Client.connect(cfg.rpc_url)
    try:
        receipt = client.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
        return getattr(receipt, "status", 0) == 1
    except Exception:
        return False

