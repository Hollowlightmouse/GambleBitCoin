#!/bin/bash
set -e

echo "Starting Flink-style streaming job (Python implementation)..."
echo "  Kafka: ${KAFKA_BROKERS:-kafka:29092}"
echo "  Redis: ${REDIS_URL:-redis://redis:6379}"
echo "  Window: ${FLINK_WINDOW_SIZE_SEC:-30}s, Watermark: ${FLINK_WATERMARK_DELAY_SEC:-10}s"

python3 /opt/flink/usrlib/flink_streaming_job.py
