from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()

from src.bank_payout import BankConfig, BankBeneficiary, send_bank_payout


def main() -> int:
    cfg = BankConfig()
    bene = BankBeneficiary(
        name=os.getenv("BANK_BENEFICIARY_NAME", "Beneficiary"),
        account_number=os.getenv("BANK_ACCOUNT_NUMBER", ""),
        branch_code=os.getenv("BANK_BRANCH_CODE", ""),
        currency=os.getenv("BANK_CURRENCY", "ZAR"),
    )
    amount_str = os.getenv("BANK_AMOUNT", "0")
    reference = os.getenv("BANK_REFERENCE", "ECHO")
    try:
        amount = float(amount_str)
    except Exception:
        print("❌ BANK_AMOUNT invalid")
        return 2
    if not bene.account_number or not bene.branch_code or amount <= 0:
        print("❌ Missing BANK_* details or amount")
        return 2
    try:
        res = send_bank_payout(cfg, bene, amount, reference)
        print("bank_tx:", res)
        return 0
    except Exception as exc:
        print("❌ bank payout failed:", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

