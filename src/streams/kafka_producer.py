import asyncio
import json
import os
import signal
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError
from binance import AsyncClient, BinanceSocketManager

SYMBOLS = [
    s.strip().lower()
    for s in os.getenv("BINANCE_SYMBOLS", "ethusdt,solusdt,bnbusdt").split(",")
    if s.strip()
]

API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")
TLD = (os.getenv("BINANCE_TLD") or "com").strip()
WS_TIMEOUT = int(os.getenv("BINANCE_WS_TIMEOUT") or "30")

KAFKA_BROKERS = [
    b.strip()
    for b in os.getenv("KAFKA_BROKERS", "kafka:29092").split(",")
    if b.strip()
]

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "binance.trades.raw")

SHOULD_STOP = False

producer = None


def print_event(payload):
    print(json.dumps(payload), flush=True)


def to_tick(message):
    if not isinstance(message, dict):
        return None

    if message.get("e") != "trade":
        return None

    symbol = message.get("s")
    price = message.get("p")
    qty = message.get("q")
    ts = message.get("T")

    if not symbol or price is None:
        return None

    try:
        return {
            "symbol": str(symbol).upper(),
            "price": float(price),
            "qty": float(qty) if qty else 0.0,
            "ts": int(ts) if ts else int(time.time() * 1000),
        }
    except Exception:
        return None


def init_kafka():
    global producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1,
            request_timeout_ms=30000,
            api_version=(2, 5),
        )
        print_event({"type": "kafka_info", "message": f"Kafka producer connected to {KAFKA_BROKERS}"})
        return True
    except KafkaError as e:
        print_event({"type": "kafka_error", "message": f"Kafka connection failed: {str(e)}"})
        return False


def send_to_kafka(tick):
    global producer
    if producer is None:
        return
    try:
        producer.send(KAFKA_TOPIC, value=tick)
        producer.flush()
    except KafkaError as e:
        print_event({"type": "kafka_send_error", "message": str(e)})


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
                send_to_kafka(tick)

    await client.close_connection()


async def run():
    backoff = 2
    max_backoff = 20

    if not init_kafka():
        print_event({"type": "stream_error", "message": "Aborting: Kafka not available"})
        return

    try:
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
    finally:
        if producer:
            producer.close()


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
