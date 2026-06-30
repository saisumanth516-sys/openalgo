#!/usr/bin/env python
"""
PRO STRATEGY: 09:20 NIFTY ATM CE SHORT

FEATURES:
✔ LIMIT ENTRY
✔ TICK-BY-TICK STOPLOSS (WebSocket)
✔ MARKET EXIT (FAST)
✔ FIXED STOPLOSS (NO TRAILING)
✔ BACKUP SL-M (FAILSAFE)
✔ TIME EXIT
✔ DUPLICATE ORDER PROTECTION
"""

from datetime import datetime
import os
import threading
import time
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from openalgo import api

# ================= CONFIG =================
load_dotenv()

IST = ZoneInfo("Asia/Kolkata")

API_KEY = ""
HOST = os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000")
WS_URL = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:8765")

STRATEGY = "PRO FIXED SL"

UNDERLYING = "NIFTY"
UNDERLYING_EXCHANGE = "NSE_INDEX"
OPTION_EXCHANGE = "NFO"
OPTION_TYPE = "CE"
OFFSET = "ATM"

PRODUCT = "MIS"
LOTS = 1

ENTRY_TIME = "09:20"
EXIT_TIME = "15:15"

STOPLOSS_PERCENT = 50

ENTRY_OFFSET = 1
TICK_SIZE = 0.05
# ==========================================

client = api(api_key=API_KEY, host=HOST, ws_url=WS_URL)

# ================= STATE =================
state_lock = threading.Lock()
exit_event = threading.Event()

option_symbol = None
quantity = None

latest_ltp = None
entry_price = None
stoploss_price = None

position_open = False
exit_in_progress = False

sl_order_id = None
exit_reason = None
# =========================================


# ================= UTILS =================
def now():
    return datetime.now(IST)


def round_to_tick(price):
    return round(round(price / TICK_SIZE) * TICK_SIZE, 2)
# ========================================


# ================= WEBSOCKET =================
def on_ltp(message):
    global latest_ltp

    try:
        ltp = float(message["data"]["ltp"])
    except:
        return

    trigger = False

    with state_lock:
        latest_ltp = ltp

        if position_open and not exit_in_progress:

            # 🔥 SL HIT (tick-by-tick)
            if stoploss_price and ltp >= stoploss_price:
                trigger = True

    if trigger:
        threading.Thread(
            target=exit_market,
            args=(f"SL HIT {ltp} >= {stoploss_price}",),
            daemon=True,
        ).start()
# ==============================================


# ================= ORDERS =================
def place_limit(action, price):
    return client.placeorder(
        strategy=STRATEGY,
        symbol=option_symbol,
        action=action,
        exchange=OPTION_EXCHANGE,
        price_type="LIMIT",
        price=round_to_tick(price),
        product=PRODUCT,
        quantity=quantity,
    )


def place_sl(entry):
    global sl_order_id, stoploss_price

    stoploss_price = round_to_tick(entry * (1 + STOPLOSS_PERCENT / 100))

    res = client.placeorder(
        strategy=STRATEGY,
        symbol=option_symbol,
        action="BUY",
        exchange=OPTION_EXCHANGE,
        price_type="SL-M",
        trigger_price=stoploss_price,
        product=PRODUCT,
        quantity=quantity,
    )

    print("🛡️ SL-M:", res)

    if res.get("status") == "success":
        sl_order_id = res.get("orderid")
# ============================================


# ================= ENTRY =================
def enter():
    global entry_price, position_open

    time.sleep(2)

    ltp = latest_ltp
    if not ltp:
        raise RuntimeError("No LTP")

    price = ltp - ENTRY_OFFSET

    res = place_limit("SELL", price)

    if res.get("status") != "success":
        raise RuntimeError("Entry failed")

    order_id = res["orderid"]

    time.sleep(2)

    status = client.orderstatus(order_id=order_id)
    entry_price = float(status["data"]["average_price"])

    print(f"✅ ENTRY @ {entry_price}")

    position_open = True

    # 🔥 Place FIXED SL
    place_sl(entry_price)
# ==========================================


# ================= EXIT =================
def exit_market(reason):
    global position_open, exit_in_progress, exit_reason

    with state_lock:
        if not position_open or exit_in_progress:
            return
        exit_in_progress = True
        exit_reason = reason

    print(f"⚡ EXIT: {reason}")

    # First, cancel SL-M order to prevent double exit
    try:
        if sl_order_id:
            cancel_res = client.cancelorder(order_id=sl_order_id, strategy=STRATEGY)
            print(f"Cancelled SL-M order: {cancel_res}")
    except Exception as e:
        print(f"Error cancelling SL-M order: {e}")

    # Place Market Exit Order
    res = client.placeorder(
        strategy=STRATEGY,
        symbol=option_symbol,
        action="BUY",
        exchange=OPTION_EXCHANGE,
        price_type="MARKET",
        product=PRODUCT,
        quantity=quantity,
    )
    print(f"Exit Order Response: {res}")

    if res.get("status") == "success":
        with state_lock:
            position_open = False
        exit_event.set()
    else:
        print(f"❌ EXIT ORDER FAILED: {res}")
        with state_lock:
            exit_in_progress = False
# =========================================


# ================= TIME EXIT =================
def time_exit():
    while True:
        if now().strftime("%H:%M") >= EXIT_TIME:
            exit_market("TIME EXIT")
            return
        time.sleep(5)
# ============================================


# ================= MAIN =================
def run():
    global option_symbol, quantity

    print("🚀 STARTED")

    res = client.optionsymbol(
        underlying=UNDERLYING,
        exchange=UNDERLYING_EXCHANGE,
        offset=OFFSET,
        option_type=OPTION_TYPE,
    )

    option_symbol = res["symbol"]
    quantity = int(res["lotsize"]) * LOTS

    print("Trading:", option_symbol, "Qty:", quantity)

    client.connect()
    client.subscribe_ltp(
        [{"exchange": OPTION_EXCHANGE, "symbol": option_symbol}],
        on_data_received=on_ltp,
    )

    while now().strftime("%H:%M") < ENTRY_TIME:
        time.sleep(1)

    enter()

    threading.Thread(target=time_exit, daemon=True).start()

    while not exit_event.is_set():
        print("LTP:", latest_ltp, "SL:", stoploss_price)
        time.sleep(3)

    client.disconnect()
    print("🏁 DONE:", exit_reason)
# ==========================================


if __name__ == "__main__":
    run()
