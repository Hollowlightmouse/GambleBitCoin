import asyncio
import json
import os
import signal
import sys
import time

from binance import AsyncClient, BinanceSocketManager


SYMBOLS = [
    s.strip().lower()
    for s in os.getenv("BINANCE_SYMBOLS", "ethusdt,solusdt,bnbusdt").split(",")
    if s.strip()
]

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
TLD = (os.getenv("BINANCE_TLD") or "com").strip()
WS_TIMEOUT = int(os.getenv("BINANCE_WS_TIMEOUT") or "30")

SHOULD_STOP = False


def print_event(payload):
    print(json.dumps(payload), flush=True)


def to_tick(message):
    if not isinstance(message, dict):
        return None

    if message.get("e") != "trade":
        return None

    symbol = message.get("s")
    price = message.get("p")
    ts = message.get("T")

    if not symbol or price is None:
        return None

    try:
        return {
            "symbol": str(symbol).upper(),
            "price": float(price),
            "ts": int(ts) if ts else int(time.time() * 1000),
        }
    except Exception:
        return None


async def stream_once(symbols):
    client = await AsyncClient.create(
        api_key=API_KEY,
        api_secret=API_SECRET,
        tld=TLD,
        requests_params={"timeout": WS_TIMEOUT},
    )
    bsm = BinanceSocketManager(client)

    streams = [f"{symbol}@trade" for symbol in symbols]
    socket = bsm.multiplex_socket(streams)

    async with socket as stream:
        while not SHOULD_STOP:
            message = await stream.recv()
            data = message.get("data") if isinstance(message, dict) else None
            tick = to_tick(data)
            if tick:
                print_event(tick)

    await client.close_connection()


async def run():
    backoff = 2
    max_backoff = 20

    while not SHOULD_STOP:
        try:
            await stream_once(SYMBOLS)
            backoff = 2
        except asyncio.CancelledError:
            break
        except Exception as exc:
            print_event({"type": "stream_error", "message": str(exc)})
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff + 2)


def handle_signal(_sig, _frame):
    global SHOULD_STOP
    SHOULD_STOP = True


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(run())


if __name__ == "__main__":
    main()
