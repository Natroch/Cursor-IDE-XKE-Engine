from dotenv import load_dotenv
load_dotenv()

import os
from src.invoke import invoke_echo_payout

# Do NOT set echo_score here; it will be derived deterministically from echo_tag
metadata = {"echo_tag": "LIVE-001"}
beneficiary = {"address": os.getenv("RECIPIENT")}
amount = float(os.getenv("AMOUNT_USDT") or "0")

print("Invoking:", beneficiary, amount)
invoke_echo_payout(metadata, beneficiary, base_amount_zar=amount, fx_rate=1.0)
print("Done")