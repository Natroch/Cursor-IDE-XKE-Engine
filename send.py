from dotenv import load_dotenv
load_dotenv()
import os
from src.invoke import USDT_DECIMALS, send_payout_onchain, confirm_onchain

recipient = os.getenv("RECIPIENT")
amount_usdt = float(os.getenv("AMOUNT_USDT"))
units = int(amount_usdt * (10 ** USDT_DECIMALS))

tx = send_payout_onchain(recipient, units, "LIVE-001")
print("tx:", tx)
print("confirmed:", confirm_onchain(tx) if tx else None)