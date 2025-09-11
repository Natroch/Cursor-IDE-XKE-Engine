from dotenv import load_dotenv
load_dotenv()
import os
from web3 import Web3

# Read token address from env to support non-BSC chains (e.g., opBNB)
USDT = os.getenv("USDT_ADDRESS") or "0x55d398326f99059fF775485246999027B3197955"
RPC = os.getenv("RPC_URL"); PK = os.getenv("PRIVATE_KEY")
CHAIN = int(os.getenv("CHAIN_ID","56")); SPENDER = os.getenv("CONTRACT_ADDRESS")
assert RPC and PK and SPENDER, "Missing RPC_URL/PRIVATE_KEY/CONTRACT_ADDRESS"

w3 = Web3(Web3.HTTPProvider(RPC)); acct = w3.eth.account.from_key(PK)
abi=[{"name":"approve","type":"function","stateMutability":"nonpayable",
      "inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],
      "outputs":[{"type":"bool"}]}]
usdt=w3.eth.contract(address=Web3.to_checksum_address(USDT), abi=abi)

allow=1000*(10**int(os.getenv("USDT_DECIMALS","18")))
tx=usdt.functions.approve(Web3.to_checksum_address(SPENDER), allow).build_transaction({
    "from":acct.address,"nonce":w3.eth.get_transaction_count(acct.address),
    "chainId":CHAIN,"gasPrice":w3.eth.gas_price})
try: tx["gas"]=int(w3.eth.estimate_gas(tx)*1.2)
except Exception: tx["gas"]=300000

signed=w3.eth.account.sign_transaction(tx, PK)
h=w3.eth.send_raw_transaction(signed.rawTransaction).hex()
print("approve tx:",h)
print("status:", w3.eth.wait_for_transaction_receipt(h).status)