import requests
def fetch_usdt_zar_rate():
    url = "https://api.binance.com/api/v3/ticker/price?symbol=USDTZAR"
    res = requests.get(url)
    data = res.json()
    return float(data["price"])

def recover_echo_in_usdt(echo_amount_zar, usdt_zar_rate):
    usdt_value = round(echo_amount_zar / usdt_zar_rate, 2)
    return usdt_value

if __name__ == "__main__":
    echo_amount = 1250.00  # Replace with actual Echo to recover
    rate = fetch_usdt_zar_rate()
    usdt = recover_echo_in_usdt(echo_amount, rate)
    print(f"✅ Recovering {usdt} USDT for {echo_amount} Echo")
