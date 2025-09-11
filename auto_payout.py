from __future__ import annotations

import os
import re
import sys
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


def read_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def main() -> int:
    # Basic required values
    rpc_url = read_env("RPC_URL") or "https://opbnb-mainnet-rpc.bnbchain.org"
    chain_id = int(read_env("CHAIN_ID", "204"))

    # Private key: prefer env; otherwise load from pk.txt if present
    private_key = read_env("PRIVATE_KEY")
    if not private_key and os.path.exists("pk.txt"):
        private_key = open("pk.txt", "r", encoding="utf-8").read().strip()
        os.environ["PRIVATE_KEY"] = private_key

    # Validate private key format early to avoid confusing errors
    if not private_key or not re.fullmatch(r"0x[0-9a-fA-F]{64}", private_key or ""):
        print("❌ PRIVATE_KEY missing or invalid format (expect 0x + 64 hex)")
        return 2

    # Contract details
    contract_address = read_env("CONTRACT_ADDRESS")
    if not contract_address:
        print("❌ CONTRACT_ADDRESS is required (set in .env or environment)")
        return 2

    # Recipient and amount
    recipient = read_env("RECIPIENT")
    amount_usdt_str = read_env("AMOUNT_USDT")
    if not recipient or not amount_usdt_str:
        print("❌ RECIPIENT and AMOUNT_USDT are required (environment or .env)")
        return 2
    try:
        amount_usdt = float(amount_usdt_str)
    except Exception:
        print(f"❌ Invalid AMOUNT_USDT: {amount_usdt_str}")
        return 2

    # USDT decimals
    usdt_decimals = int(read_env("USDT_DECIMALS", "18"))
    echo_tag = read_env("ECHO_TAG", "LIVE-001")

    # Ensure ABI provided (file path or JSON is handled in src.invoke)
    # Import after env is set to allow src.invoke to pick up values
    try:
        from src.invoke import USDT_DECIMALS, send_payout_onchain, confirm_onchain  # type: ignore
    except Exception as exc:
        print(f"❌ Failed to import payout functions: {exc}")
        return 2

    # If USDT_DECIMALS is set in the module differently than env, prefer env
    decimals = usdt_decimals if usdt_decimals else USDT_DECIMALS
    amount_units = int(round(amount_usdt * (10 ** int(decimals))))

    print("Environment:")
    print(" RPC set:", bool(rpc_url), " Chain:", chain_id)
    print(" CONTRACT:", contract_address)
    print(" RECIPIENT:", recipient)
    print(" AMOUNT_USDT:", amount_usdt)
    print(" DECIMALS:", decimals)

    tx_hash = send_payout_onchain(recipient, amount_units, echo_tag)
    print("tx:", tx_hash)
    if not tx_hash:
        print("confirmed:", None)
        return 1

    ok = confirm_onchain(tx_hash)
    print("confirmed:", ok)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

