from dotenv import load_dotenv
load_dotenv()

import os
from src.invoke import invoke_echo_payout

print("RPC", bool(os.getenv("RPC_URL")),
      "CHAIN", os.getenv("CHAIN_ID"),
      "CONTRACT", os.getenv("CONTRACT_ADDRESS"),
      "PK_LEN", len(os.getenv("PRIVATE_KEY") or ""),
      "RECIPIENT", os.getenv("RECIPIENT"),
      "AMOUNT_USDT", os.getenv("AMOUNT_USDT"))

recipient = os.getenv("RECIPIENT")
amount_usdt = float(os.getenv("AMOUNT_USDT") or "0")

metadata = {"echo_tag": "LIVE-001", "echo_score": 0}
beneficiary = {"address": recipient}

print("Invoking…")
invoke_echo_payout(metadata, beneficiary, base_amount_zar=amount_usdt, fx_rate=1.0)
print("Finished invoke")