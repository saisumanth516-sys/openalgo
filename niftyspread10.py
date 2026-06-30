#!/usr/bin/env python
"""
09:20 NIFTY ATM CE short strategy using websocket LTP ticks.

Rules:
- At 09:20 IST, resolve and sell the ATM CE with a LIMIT order.
- Stop-loss is synthetic: if websocket LTP reaches 50% above entry, exit with
  LIMIT BUY orders only.
- If stop-loss is not hit, exit at 15:15 IST with LIMIT BUY orders only.
- Buffer (Limit Offset) set to 1.0 points for NIFTY.
"""
from datetime import datetime, time as dtime
import os
import threading
import time
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from openalgo import api


load_dotenv()

IST = ZoneInfo("Asia/Kolkata")

# Using the same API Key provided in the session
API_KEY = "5dfcd2bd6b8c6f726ba5123fae7e6289e05679c6633ec6cf52ab00042b1d627d"
HOST = os.getenv("HOST_SERVER") or os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000")
WS_URL = os.getenv("WEBSOCKET_URL") or (
    f"ws://{os.getenv('WEBSOCKET_HOST', '127.0.0.1')}:{os.getenv('WEBSOCKET_PORT', '8765')}"
)

# --- NIFTY SPECIFIC CONFIG ---
STRATEGY = os.getenv("STRATEGY_NAME", "920 Sell CE Limit WS SL NIFTY")
UNDERLYING = "NIFTY"
UNDERLYING_EXCHANGE = "NSE_INDEX"
OPTION_EXCHANGE = "NFO"
OPTION_TYPE = os.getenv("OPTION_TYPE", "CE")
OFFSET = os.getenv("OFFSET", "ATM")
EXPIRY_DATE = os.getenv("EXPIRY_DATE") or None
PRODUCT = os.getenv("PRODUCT", "MIS")
LOTS = int(os.getenv("LOTS", "1"))

ENTRY_TIME = os.getenv("ENTRY_TIME", "10:00")
EXIT_TIME = os.getenv("EXIT_TIME", "15:15")
STOPLOSS_PERCENT = float(os.getenv("STOPLOSS_PERCENT", "50"))

# Buffer set to 1.0 points for NIFTY
ENTRY_LIMIT_OFFSET_POINTS = 1.0
EXIT_LIMIT_OFFSET_POINTS = 1.0
TICK_SIZE = 0.05

ENTRY_FILL_TIMEOUT_SECONDS = 20.0
EXIT_FILL_TIMEOUT_SECONDS = 10.0
EXIT_REPRICE_ATTEMPTS = 10
STOPLOSS_MAX_SPREAD_POINTS = 1.0
STOPLOSS_SPREAD_RECHECK_SECONDS = 0.5


if not API_KEY:
    print("Error: OPENALGO_API_KEY environment variable not set")
    raise SystemExit(1)


client = api(
    api_key=API_KEY,
    host=HOST,
    ws_url=WS_URL,
    verbose=int(os.getenv("WS_VERBOSE", "0")),
)

state_lock = threading.Lock()
exit_event = threading.Event()

option_symbol = None
quantity = None
latest_ltp = None
entry_price = None
stoploss_price = None
position_open = False
exit_in_progress = False
exit_reason = None


def now_ist():
    return datetime.now(IST)


def parse_hhmm(value):
    hour, minute = value.split(":")
    return dtime(hour=int(hour), minute=int(minute), tzinfo=IST)


def wait_until(clock_time):
    while True:
        current = now_ist()
        target = datetime.combine(current.date(), clock_time, tzinfo=IST)
        if current >= target:
            return
        time.sleep(min((target - current).total_seconds(), 30))


def today_at(clock_time):
    return datetime.combine(now_ist().date(), clock_time, tzinfo=IST)


def round_to_tick(price):
    return round(round(price / TICK_SIZE) * TICK_SIZE, 2)


def normalize_expiry_date(expiry_date):
    if not expiry_date:
        return None
    value = expiry_date.strip().upper()
    for fmt in ("%d-%b-%y", "%d-%b-%Y", "%d%b%y", "%d%b%Y"):
        try:
            return datetime.strptime(value, fmt).strftime("%d%b%y").upper()
        except ValueError:
            continue
    return value.replace("-", "")


def resolve_expiry_date():
    configured_expiry = normalize_expiry_date(EXPIRY_DATE)
    if configured_expiry:
        return configured_expiry

    response = client.expiry(
        symbol=UNDERLYING,
        exchange=OPTION_EXCHANGE,
        instrumenttype="options",
    )
    if response.get("status") != "success":
        raise RuntimeError(f"expiry lookup failed: {response}")

    expiries = response.get("data") or []
    if not expiries:
        raise RuntimeError(
            f"No live option expiries found for {UNDERLYING} on {OPTION_EXCHANGE}: {response}"
        )

    expiry_date = normalize_expiry_date(expiries[0])
    print(f"Using nearest expiry: {expiry_date}")
    return expiry_date


def extract_ltp(message):
    try:
        return float(message["data"]["ltp"])
    except (KeyError, TypeError, ValueError):
        return None


def parse_positive_float(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def extract_bid_ask(response):
    if not isinstance(response, dict):
        return None, None

    data = response.get("data")
    if not isinstance(data, dict):
        data = response

    bid = parse_positive_float(
        data.get("bid")
        or data.get("best_bid")
        or data.get("bid_price")
        or data.get("bp1")
    )
    ask = parse_positive_float(
        data.get("ask")
        or data.get("best_ask")
        or data.get("ask_price")
        or data.get("ap1")
    )
    return bid, ask


def wait_for_stoploss_spread():
    while True:
        response = client.quotes(symbol=option_symbol, exchange=OPTION_EXCHANGE)
        bid, ask = extract_bid_ask(response)

        if bid is not None and ask is not None:
            spread = ask - bid
            if 0 <= spread <= STOPLOSS_MAX_SPREAD_POINTS:
                print(
                    f"Stop-loss spread OK: bid={bid:.2f}, ask={ask:.2f}, "
                    f"spread={spread:.2f}"
                )
                return ask

            print(
                f"Stop-loss spread too wide: bid={bid:.2f}, ask={ask:.2f}, "
                f"spread={spread:.2f}; rechecking in "
                f"{STOPLOSS_SPREAD_RECHECK_SECONDS:.1f}s"
            )
        else:
            print(
                f"Stop-loss bid/ask unavailable from quotes response; rechecking in "
                f"{STOPLOSS_SPREAD_RECHECK_SECONDS:.1f}s. Response: {response}"
            )

        time.sleep(STOPLOSS_SPREAD_RECHECK_SECONDS)


def on_ltp(message):
    global latest_ltp

    ltp = extract_ltp(message)
    if ltp is None:
        return

    should_exit = False
    with state_lock:
        latest_ltp = ltp
        if position_open and not exit_in_progress and stoploss_price is not None:
            should_exit = ltp >= stoploss_price

    if should_exit:
        threading.Thread(
            target=exit_with_limit_orders,
            args=(f"stop-loss hit: LTP {ltp:.2f} >= {stoploss_price:.2f}", True),
            daemon=True,
        ).start()


def wait_for_ltp(timeout_seconds=15):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        with state_lock:
            if latest_ltp is not None:
                return latest_ltp
        time.sleep(0.05)
    return None


def order_status(order_id):
    response = client.orderstatus(order_id=order_id, strategy=STRATEGY)
    data = response.get("data", {}) if isinstance(response, dict) else {}
    status = str(data.get("order_status", "")).lower()
    try:
        average_price = float(data.get("average_price") or 0)
    except (TypeError, ValueError):
        average_price = 0
    return status, average_price, response


def wait_for_fill(order_id, timeout_seconds):
    deadline = time.time() + timeout_seconds
    last_response = None
    while time.time() < deadline:
        status, average_price, last_response = order_status(order_id)
        if status in {"complete", "completed", "filled", "traded"}:
            return True, average_price, last_response
        if status in {"rejected", "cancelled", "canceled"}:
            return False, average_price, last_response
        time.sleep(0.5)
    return False, 0, last_response


def cancel_order(order_id):
    response = client.cancelorder(order_id=order_id, strategy=STRATEGY)
    print("Cancel Order Response:", response)
    return response


def place_limit_order(action, price):
    response = client.placeorder(
        strategy=STRATEGY,
        symbol=option_symbol,
        action=action,
        exchange=OPTION_EXCHANGE,
        price_type="LIMIT",
        price=round_to_tick(price),
        product=PRODUCT,
        quantity=quantity,
    )
    print(f"{action} LIMIT @{round_to_tick(price):.2f} Response:", response)
    return response


def resolve_option_contract():
    global option_symbol, quantity

    expiry_date = resolve_expiry_date()
    response = client.optionsymbol(
        underlying=UNDERLYING,
        exchange=UNDERLYING_EXCHANGE,
        expiry_date=expiry_date,
        offset=OFFSET,
        option_type=OPTION_TYPE,
    )
    if response.get("status") != "success":
        raise RuntimeError(f"optionsymbol failed: {response}")

    option_symbol = response["symbol"]
    lot_size = int(response.get("lotsize") or 0)
    if lot_size <= 0:
        raise RuntimeError(f"Invalid lot size from optionsymbol: {response}")
    quantity = lot_size * LOTS

    print(
        f"Resolved {UNDERLYING} {OFFSET} {OPTION_TYPE}: "
        f"{option_symbol} {OPTION_EXCHANGE}, lot_size={lot_size}, quantity={quantity}"
    )


def enter_short_ce():
    global entry_price, stoploss_price, position_open

    first_ltp = wait_for_ltp(timeout_seconds=15)
    if first_ltp is None:
        raise RuntimeError("No option LTP received from websocket before entry")

    # Selling: Buffer subtracted to get a fill
    entry_limit = max(TICK_SIZE, first_ltp - ENTRY_LIMIT_OFFSET_POINTS)
    response = place_limit_order("SELL", entry_limit)
    if response.get("status") != "success":
        raise RuntimeError(f"Entry order failed: {response}")

    order_id = response.get("orderid")
    filled, average_price, status_response = wait_for_fill(order_id, ENTRY_FILL_TIMEOUT_SECONDS)
    if not filled:
        cancel_order(order_id)
        raise RuntimeError(f"Entry LIMIT order was not filled: {status_response}")

    entry_price = average_price if average_price > 0 else first_ltp
    stoploss_price = round_to_tick(entry_price * (1 + STOPLOSS_PERCENT / 100))

    with state_lock:
        position_open = True

    print(
        f"Short CE active: entry={entry_price:.2f}, "
        f"stoploss={stoploss_price:.2f} ({STOPLOSS_PERCENT:.0f}%)"
    )


def exit_with_limit_orders(reason, check_stoploss_spread=False):
    global position_open, exit_in_progress, exit_reason

    with state_lock:
        if not position_open or exit_in_progress:
            return
        exit_in_progress = True
        exit_reason = reason

    print(f"Exit triggered: {reason}")

    for attempt in range(1, EXIT_REPRICE_ATTEMPTS + 1):
        spread_ask = wait_for_stoploss_spread() if check_stoploss_spread else None

        with state_lock:
            ltp = latest_ltp
        if ltp is None:
            ltp = stoploss_price or entry_price

        # Buying: Buffer added to get a fill
        exit_reference = spread_ask if spread_ask is not None else ltp
        exit_limit = max(TICK_SIZE, exit_reference + EXIT_LIMIT_OFFSET_POINTS)
        response = place_limit_order("BUY", exit_limit)
        if response.get("status") != "success":
            print(f"Exit order attempt {attempt} failed to place: {response}")
            time.sleep(0.5)
            continue

        order_id = response.get("orderid")
        filled, _, status_response = wait_for_fill(order_id, EXIT_FILL_TIMEOUT_SECONDS)
        if filled:
            with state_lock:
                position_open = False
            exit_event.set()
            print(f"Exit filled on attempt {attempt}")
            return

        print(f"Exit LIMIT order not filled on attempt {attempt}: {status_response}")
        cancel_order(order_id)
        time.sleep(0.2)

    with state_lock:
        exit_in_progress = False
    print("Exit not completed after configured LIMIT reprice attempts; strategy remains open.")


def time_exit_worker():
    wait_until(parse_hhmm(EXIT_TIME))
    exit_with_limit_orders(f"time exit {EXIT_TIME} IST")


def run_strategy():
    print(f"Starting {STRATEGY}")
    print(f"Entry: {ENTRY_TIME} IST | Exit: {EXIT_TIME} IST | Orders: LIMIT only")

    entry_clock = parse_hhmm(ENTRY_TIME)
    exit_clock = parse_hhmm(EXIT_TIME)

    if today_at(entry_clock) >= today_at(exit_clock):
        raise RuntimeError(f"ENTRY_TIME {ENTRY_TIME} must be before EXIT_TIME {EXIT_TIME}")

    if now_ist() >= today_at(exit_clock):
        print(f"Exit time {EXIT_TIME} IST already passed. No entry will be placed.")
        return

    wait_until(entry_clock)
    if now_ist() >= today_at(exit_clock):
        print(f"Entry skipped because exit time {EXIT_TIME} IST has already passed.")
        return

    resolve_option_contract()

    instruments = [{"exchange": OPTION_EXCHANGE, "symbol": option_symbol}]
    if not client.connect():
        raise RuntimeError("WebSocket connection/authentication failed")
    if not client.subscribe_ltp(instruments, on_data_received=on_ltp):
        raise RuntimeError(f"WebSocket subscription failed for {instruments}")

    try:
        enter_short_ce()
        threading.Thread(target=time_exit_worker, daemon=True).start()

        while not exit_event.is_set():
            with state_lock:
                ltp = latest_ltp
                sl = stoploss_price
                open_position = position_open
            if open_position and ltp is not None:
                print(f"{now_ist().strftime('%H:%M:%S')} {option_symbol} LTP={ltp:.2f} SL={sl:.2f}")
            time.sleep(5)
    finally:
        try:
            client.unsubscribe_ltp(instruments)
        finally:
            client.disconnect()
        print(f"Strategy finished. Reason: {exit_reason or 'entry not active'}")


if __name__ == "__main__":
    run_strategy()
