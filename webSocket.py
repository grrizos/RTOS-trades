import asyncio
import json
import websockets
import time
import os
# Λίστα συμβόλων που θέλουμε
symbols = ["BTC-USDT", "ADA-USDT", "ETH-USDT", "DOGE-USDT", "XRP-USDT", "SOL-USDT", "LTC-USDT", "BNB-USDT"]

# Αρχικοποίηση αρχείων με header (αν δεν υπάρχουν)
for symbol in symbols:
    filepath = f"trades/{symbol}.csv"
    if not os.path.exists(filepath):
        with open(filepath, "w") as f:
            f.write("tradeID,ts_exchange_ms,ts_received_ms,delay,price,volume\n")

async def subscribe(websocket):
    # Μήνυμα εγγραφής (subscribe) για το κανάλι συναλλαγών (trades)
    sub_msg = {
        "id": "5323",
        "op": "subscribe",
        "args": [
            {
                "channel": "trades",
                "instId": symbol
            }for symbol in symbols
        ]
    }
    await websocket.send(json.dumps(sub_msg))
    print ("Subscribed to trades for symbols:" ,symbols)

def save_trade(symbol,trade):
    """
    Αποθηκεύει μία συναλλαγή σε CSV.
    Format: tradeID,ts_exchange_ms,delay,price,volume
    """
    filepath = f"trades/{symbol}.csv"
    tradeID = trade["tradeId"]  # trade ID από το OKX
    ts_exchange = int(trade["ts"])  # string από το OKX
    ts_received = int(time.time() * 1000)  # τρέχων χρόνος σε ms
    delay = (ts_received - ts_exchange) # καθυστέρηση σε ms
    price = trade["px"]
    volume = trade["sz"]

    line = f"{tradeID},{ts_exchange},{ts_received},{delay},{price},{volume}\n"
    with open(filepath, "a") as f:
        f.write(line)

async def listen():
    url = "wss://ws.okx.com:8443/ws/v5/public"
    while True:
        try:
            async with websockets.connect(url) as ws:
                await subscribe(ws)
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    # Ελέγχουμε αν είναι trade data
                    if "data" in data and "arg" in data and data["arg"].get("channel") == "trades":
                        symbol = data["arg"]["instId"]
                        for trade in data["data"]:
                            save_trade(symbol, trade)
        except Exception as e:
            print("Connection error:", e)
            print("Reconnecting in 5s...")
            await asyncio.sleep(5)
            
if __name__ == "__main__":
    asyncio.run(listen())