"""
Flink-Style Streaming Job - Pure Python Implementation
=======================================================
Pipeline: Kafka -> Window Processor -> Redis

Demonstrates streaming concepts equivalent to Apache Flink:
  - Time Windows: Tumbling event-time window of 30 seconds
  - Watermarks: 10-second bounded out-of-orderness for late data
  - Aggregations: avg, max, min price; trade count; total volume
  - Sink: Redis with key 'flink:window:{symbol}:{window_end_ms}'

This implementation uses kafka-python-ng for Kafka consumption and
implements Flink-style windowing/watermarking logic in Python.
It runs as a long-lived Docker service managed by docker-compose.

Variables from .env:
  KAFKA_BROKERS - Kafka bootstrap servers
  REDIS_URL     - Redis connection string
"""

import os
import sys
import json
import time
import threading
from collections import defaultdict


KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:29092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "binance.trades.raw")
WINDOW_SIZE_SEC = int(os.getenv("FLINK_WINDOW_SIZE_SEC", "30"))
WATERMARK_DELAY_SEC = int(os.getenv("FLINK_WATERMARK_DELAY_SEC", "10"))

REDIS_HOST = REDIS_URL.split("://")[-1].split(":")[0] if "://" in REDIS_URL else "redis"
REDIS_PORT = int(REDIS_URL.split("://")[-1].split(":")[1].split("/")[0]) if "://" in REDIS_URL else 6379


class TradeWindow:
    """Holds aggregation state for a single tumbling window."""

    def __init__(self, window_start_ms, window_end_ms):
        self.window_start_ms = window_start_ms
        self.window_end_ms = window_end_ms
        self.prices = []
        self.quantities = []

    def add(self, price, qty):
        self.prices.append(price)
        self.quantities.append(qty)

    @property
    def avg_price(self):
        return sum(self.prices) / len(self.prices) if self.prices else 0

    @property
    def max_price(self):
        return max(self.prices) if self.prices else 0

    @property
    def min_price(self):
        return min(self.prices) if self.prices else 0

    @property
    def trade_count(self):
        return len(self.prices)

    @property
    def total_volume(self):
        return sum(self.quantities)


class WatermarkWindowProcessor:
    """
    Implements Flink-style tumbling event-time windows with watermarks.

    - TUMBLE window: fixed-size non-overlapping windows (30s default)
    - Watermark: BoundedOutOfOrderness (10s default) - late events within
      the watermark delay are still accepted into their window.
    """

    def __init__(self, window_size_ms, watermark_delay_ms):
        self.window_size_ms = window_size_ms
        self.watermark_delay_ms = watermark_delay_ms
        self.windows = defaultdict(dict)
        self.max_event_time = 0
        self._lock = threading.Lock()

    def _window_start(self, event_time_ms):
        return (event_time_ms // self.window_size_ms) * self.window_size_ms

    def add_trade(self, symbol, price, qty, event_time_ms):
        with self._lock:
            if event_time_ms > self.max_event_time:
                self.max_event_time = event_time_ms

            current_watermark = self.max_event_time - self.watermark_delay_ms

            win_start = self._window_start(event_time_ms)
            win_end = win_start + self.window_size_ms

            if win_end > current_watermark:
                key = (symbol, win_start)
                if key not in self.windows:
                    self.windows[key] = TradeWindow(win_start, win_end)
                self.windows[key].add(price, qty)

    def flush_expired_windows(self):
        with self._lock:
            current_watermark = self.max_event_time - self.watermark_delay_ms
            expired_keys = []

            for key, window in self.windows.items():
                if window.window_end_ms <= current_watermark:
                    expired_keys.append(key)

            results = []
            for key in expired_keys:
                window = self.windows.pop(key)
                if window.trade_count > 0:
                    results.append({
                        "symbol": key[0],
                        "avg_price": window.avg_price,
                        "max_price": window.max_price,
                        "min_price": window.min_price,
                        "trade_count": window.trade_count,
                        "total_volume": window.total_volume,
                        "window_end_ms": window.window_end_ms,
                    })

            return results


def write_to_redis(result):
    try:
        import redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, socket_connect_timeout=5)

        key = f"flink:window:{result['symbol']}:{result['window_end_ms']}"
        r.hset(key, mapping={
            "symbol": result["symbol"],
            "avg_price": f"{result['avg_price']:.4f}",
            "max_price": f"{result['max_price']:.4f}",
            "min_price": f"{result['min_price']:.4f}",
            "trade_count": str(result["trade_count"]),
            "total_volume": f"{result['total_volume']:.4f}",
            "window_ts": str(result["window_end_ms"]),
        })
        r.expire(key, 600)

        r.sadd("flink:window:keys", key)
        r.expire("flink:window:keys", 600)

        print(f"[Flink->Redis] {key}: avg={result['avg_price']:.4f}, count={result['trade_count']}")

    except Exception as e:
        print(f"[Flink->Redis ERROR] {e}")


def main():
    window_size_ms = WINDOW_SIZE_SEC * 1000
    watermark_delay_ms = WATERMARK_DELAY_SEC * 1000

    print(f"[Flink] Initializing WindowProcessor (window={WINDOW_SIZE_SEC}s, watermark={WATERMARK_DELAY_SEC}s)...")
    processor = WatermarkWindowProcessor(window_size_ms, watermark_delay_ms)

    print(f"[Flink] Connecting to Kafka: {KAFKA_BROKERS}, topic={KAFKA_TOPIC}")

    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS.split(","),
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="flink-consumer-group",
        consumer_timeout_ms=5000,
    )

    print("[Flink] Executing pipeline...")

    flush_interval = max(1, WINDOW_SIZE_SEC // 3)
    last_flush = time.time()

    try:
        for msg in consumer:
            trade = msg.value
            symbol = trade.get("symbol", "UNKNOWN")
            price = float(trade.get("price", 0))
            qty = float(trade.get("qty", 0))
            event_time_ms = trade.get("ts", int(time.time() * 1000))

            processor.add_trade(symbol, price, qty, event_time_ms)

            now = time.time()
            if now - last_flush >= flush_interval:
                results = processor.flush_expired_windows()
                for result in results:
                    write_to_redis(result)
                last_flush = now

    except KeyboardInterrupt:
        print("[Flink] Shutting down...")
        results = processor.flush_expired_windows()
        for result in results:
            write_to_redis(result)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
