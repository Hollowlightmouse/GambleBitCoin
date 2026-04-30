import json
import os
import signal
import time
from kafka import KafkaProducer


BOOTSTRAP = os.getenv("KAFKA_BROKERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_PRICE_TOPIC", "market.prices.raw")


def main():
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    def handle_signal(_sig, _frame):
        producer.flush()
        producer.close()
        raise SystemExit

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # read stream data from python-binance streamer
    stream_cmd = "python src/streams/binance_py_stream.py"
    stream = os.popen(stream_cmd)

    for line in stream:
        try:
            payload = json.loads(line)
        except Exception:
            continue

        if payload.get("type") == "stream_error":
            continue

        if not payload.get("symbol") or payload.get("price") is None:
            continue

        event = {
            "symbol": payload["symbol"],
            "price": float(payload["price"]),
            "ts": int(payload.get("ts") or time.time() * 1000),
            "source": "binance_trade",
        }
        producer.send(TOPIC, value=event)
        producer.flush()


if __name__ == "__main__":
    main()
