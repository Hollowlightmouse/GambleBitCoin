import asyncio
import json
import os
import signal
import time

from binance import AsyncClient, BinanceSocketManager
from kafka import KafkaProducer


SYMBOLS = [
    s.strip().lower()
    for s in os.getenv("BINANCE_SYMBOLS", "ethusdt,solusdt,bnbusdt").split(",")
    if s.strip()
]

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
TLD = (os.getenv("BINANCE_TLD") or "com").strip()
WS_TIMEOUT = int(os.getenv("BINANCE_WS_TIMEOUT") or "30")

BOOTSTRAP = os.getenv("KAFKA_BROKERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_PRICE_TOPIC", "market.prices.raw")

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
            "source": "binance_trade",
        }
    except Exception:
        return None


async def stream_once(symbols, producer):
    client = await AsyncClient.create(
        api_key=API_KEY,
        api_secret=API_SECRET,
        tld=TLD,
        requests_params={"timeout": WS_TIMEOUT},
    )
    bsm = BinanceSocketManager(client)

    streams = [f"{symbol}@trade" for symbol in symbols]
    socket = bsm.multiplex_socket(streams)

    last_message_at = time.time()
    async with socket as stream:
        while not SHOULD_STOP:
            try:
                message = await asyncio.wait_for(stream.recv(), timeout=WS_TIMEOUT)
            except asyncio.TimeoutError:
                now = time.time()
                if now - last_message_at >= WS_TIMEOUT:
                    print_event({"type": "ws_timeout", "message": "no data", "ts": int(now * 1000)})
                    break
                continue

            data = message.get("data") if isinstance(message, dict) else None
            tick = to_tick(data)
            if tick:
                last_message_at = time.time()
                producer.send(TOPIC, key=tick["symbol"].encode("utf-8"), value=tick)
                print_event({"type": "kafka_sent", "symbol": tick["symbol"], "ts": tick["ts"]})

    await client.close_connection()


async def run():
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=int(os.getenv("KAFKA_LINGER_MS", "5")),
        batch_size=int(os.getenv("KAFKA_BATCH_SIZE", "16384")),
    )

    backoff = 2
    max_backoff = 20

    while not SHOULD_STOP:
        try:
            await stream_once(SYMBOLS, producer)
            backoff = 2
        except asyncio.CancelledError:
            break
        except Exception as exc:
            print_event({"type": "stream_error", "message": str(exc)})
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff + 2)

    producer.flush()
    producer.close()


def handle_signal(_sig, _frame):
    global SHOULD_STOP
    SHOULD_STOP = True


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(run())


if __name__ == "__main__":
    main()
