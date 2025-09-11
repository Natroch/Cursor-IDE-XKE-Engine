from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()

from src.xke_v2 import payout, confirm


def main() -> int:
    recipient = os.getenv("RECIPIENT")
    amount = float(os.getenv("AMOUNT_USDT", "0"))
    decimals = int(os.getenv("USDT_DECIMALS", "18"))
    echo_tag = os.getenv("ECHO_TAG", "LIVE-001")

    if not recipient or amount <= 0:
        print("❌ Set RECIPIENT and AMOUNT_USDT")
        return 2

    units = int(round(amount * (10 ** decimals)))
    tx = payout(recipient, units, echo_tag)
    print("tx:", tx)
    if not tx:
        print("confirmed:", None)
        return 1
    ok = confirm(tx)
    print("confirmed:", ok)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

