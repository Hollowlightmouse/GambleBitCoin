import json
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    min as spark_min,
    max as spark_max,
    avg as spark_avg,
    first as spark_first,
    last as spark_last,
    sum as spark_sum,
    count as spark_count,
    expr,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BROKERS", "localhost:9092")
PRICE_TOPIC = os.getenv("KAFKA_PRICE_TOPIC", "market.prices.raw")
BET_TOPIC = os.getenv("KAFKA_BET_TOPIC", "market.bets.events")
ALERTS_TOPIC = os.getenv("KAFKA_ALERTS_TOPIC", "market.alerts")

SPIKE_UP_PCT = float(os.getenv("PRICE_SPIKE_UP_PCT", "2.0"))
SPIKE_DOWN_PCT = float(os.getenv("PRICE_SPIKE_DOWN_PCT", "2.0"))
SPIKE_WINDOW_SEC = int(os.getenv("PRICE_SPIKE_WINDOW_SEC", "30"))

OUTPUT_DIR = os.getenv("SPARK_OUTPUT_DIR", "data")
PRICES_CSV = os.path.join(OUTPUT_DIR, "metrics_prices")
BETS_CSV = os.path.join(OUTPUT_DIR, "metrics_bets")
ALERTS_CSV = os.path.join(OUTPUT_DIR, "alerts")
ANOMALY_REPORT_CSV = os.path.join(OUTPUT_DIR, "anomaly_report")


def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)


price_schema = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("ts", LongType(), True),
        StructField("source", StringType(), True),
    ]
)

bet_schema = StructType(
    [
        StructField("type", StringType(), True),
        StructField("ts", LongType(), True),
        StructField("symbol", StringType(), True),
        StructField("roundId", StringType(), True),
        StructField("userId", StringType(), True),
        StructField("userName", StringType(), True),
        StructField("side", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("balanceAfter", DoubleType(), True),
    ]
)


def main():
    ensure_output_dir()

    spark = (
        SparkSession.builder.appName("GambleKafkaPipeline")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    price_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", PRICE_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    prices = (
        price_stream.selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), price_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", (col("ts") / 1000).cast("timestamp"))
    )

    price_windows = (
        prices.withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), "1 minute"), col("symbol"))
        .agg(
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
            spark_avg("price").alias("avg_price"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("min_price"),
            col("max_price"),
            col("avg_price"),
        )
    )

    bet_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", BET_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    bets = (
        bet_stream.selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), bet_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", (col("ts") / 1000).cast("timestamp"))
    )

    bet_windows = (
        bets.withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), "30 seconds", "10 seconds"), col("symbol"))
        .agg(
            spark_count("amount").alias("bet_count"),
            spark_sum("amount").alias("total_amount"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("bet_count"),
            col("total_amount"),
        )
    )

    spike_window = f"{SPIKE_WINDOW_SEC} seconds"
    spike_windows = (
        prices.withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), spike_window), col("symbol"))
        .agg(
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
            spark_avg("price").alias("avg_price"),
        )
        .withColumn(
            "spike_up",
            expr(f"(max_price - min_price) / min_price * 100 >= {SPIKE_UP_PCT}"),
        )
        .withColumn(
            "spike_down",
            expr(f"(min_price - max_price) / max_price * 100 <= -{SPIKE_DOWN_PCT}"),
        )
        .filter("spike_up OR spike_down")
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("min_price"),
            col("max_price"),
            col("avg_price"),
            col("spike_up"),
            col("spike_down"),
        )
        .withColumn("alert_type", expr("CASE WHEN spike_up THEN 'SPIKE_UP' ELSE 'SPIKE_DOWN' END"))
        .withColumn("trend", expr("CASE WHEN spike_up THEN 'UP' ELSE 'DOWN' END"))
        .withColumn("threshold", expr(f"CASE WHEN spike_up THEN {SPIKE_UP_PCT} ELSE {SPIKE_DOWN_PCT} END"))
        .withColumn("created_at", expr("current_timestamp()"))
    )

    anomaly_report = (
        prices.withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), "1 minute"), col("symbol"))
        .agg(
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
            spark_avg("price").alias("avg_price"),
            spark_first("price", ignorenulls=True).alias("first_price"),
            spark_last("price", ignorenulls=True).alias("last_price"),
        )
        .withColumn(
            "trend",
            expr(
                "CASE WHEN last_price > first_price THEN 'UP' "
                "WHEN last_price < first_price THEN 'DOWN' "
                "ELSE 'HOLD' END"
            ),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("min_price"),
            col("max_price"),
            col("avg_price"),
            col("first_price"),
            col("last_price"),
            col("trend"),
        )
        .withColumn("alert_type", expr("'ANOMALY_REPORT'"))
        .withColumn("created_at", expr("current_timestamp()"))
    )

    def console_writer(df, batch_id, label):
        rows = df.collect()
        if not rows:
            return
        print(f"\n[{datetime.utcnow().isoformat()}] {label} batch={batch_id}")
        for row in rows:
            print(row)

    price_query = (
        price_windows.writeStream.outputMode("append")
        .foreachBatch(lambda df, bid: console_writer(df, bid, "PRICE_WINDOWS"))
        .option("checkpointLocation", os.path.join(OUTPUT_DIR, "chk_prices"))
        .start()
    )

    bet_query = (
        bet_windows.writeStream.outputMode("append")
        .foreachBatch(lambda df, bid: console_writer(df, bid, "BET_WINDOWS"))
        .option("checkpointLocation", os.path.join(OUTPUT_DIR, "chk_bets"))
        .start()
    )

    alerts_query = (
        spike_windows.writeStream.outputMode("append")
        .foreachBatch(lambda df, bid: console_writer(df, bid, "ALERTS"))
        .option("checkpointLocation", os.path.join(OUTPUT_DIR, "chk_alerts"))
        .start()
    )

    def write_prices_csv(df, batch_id):
        if df.rdd.isEmpty():
            return
        (df.withColumn("created_at", expr("current_timestamp()"))
           .select(
               "created_at",
               "window_start",
               "window_end",
               "symbol",
               "min_price",
               "max_price",
               "avg_price",
           )
           .coalesce(1)
           .write.mode("append")
           .option("header", True)
           .csv(PRICES_CSV))

    def write_bets_csv(df, batch_id):
        if df.rdd.isEmpty():
            return
        (df.withColumn("created_at", expr("current_timestamp()"))
           .select(
               "created_at",
               "window_start",
               "window_end",
               "symbol",
               "bet_count",
               "total_amount",
           )
           .coalesce(1)
           .write.mode("append")
           .option("header", True)
           .csv(BETS_CSV))

    def write_alerts_csv(df, batch_id):
        if df.rdd.isEmpty():
            return
        (df.select(
            "created_at",
            "window_start",
            "window_end",
            "symbol",
            "alert_type",
            "trend",
            "min_price",
            "max_price",
            "avg_price",
            "threshold",
        )
        .coalesce(1)
        .write.mode("append")
        .option("header", True)
        .csv(ALERTS_CSV))

    def write_anomaly_report_csv(df, batch_id):
        if df.rdd.isEmpty():
            return
        (df.select(
            "created_at",
            "window_start",
            "window_end",
            "symbol",
            "alert_type",
            "trend",
            "avg_price",
            "min_price",
            "max_price",
            "first_price",
            "last_price",
        )
        .coalesce(1)
        .write.mode("append")
        .option("header", True)
        .csv(ANOMALY_REPORT_CSV))

    price_windows.writeStream.outputMode("append").foreachBatch(write_prices_csv).option(
        "checkpointLocation", os.path.join(OUTPUT_DIR, "chk_prices_csv")
    ).start()

    bet_windows.writeStream.outputMode("append").foreachBatch(write_bets_csv).option(
        "checkpointLocation", os.path.join(OUTPUT_DIR, "chk_bets_csv")
    ).start()

    alerts_writer = (
        spike_windows.writeStream.outputMode("append")
        .foreachBatch(write_alerts_csv)
        .option("checkpointLocation", os.path.join(OUTPUT_DIR, "chk_alerts_csv"))
        .start()
    )

    anomaly_report_writer = (
        anomaly_report.writeStream.outputMode("append")
        .foreachBatch(write_anomaly_report_csv)
        .option("checkpointLocation", os.path.join(OUTPUT_DIR, "chk_anomaly_report_csv"))
        .start()
    )

    alerts_to_kafka = (
        spike_windows.selectExpr(
            "to_json(named_struct("
            "'symbol', symbol, "
            "'alert_type', alert_type, "
            "'trend', trend, "
            "'window_start', CAST(window_start AS STRING), "
            "'window_end', CAST(window_end AS STRING), "
            "'min_price', min_price, "
            "'max_price', max_price, "
            "'avg_price', avg_price, "
            "'threshold', threshold, "
            "'created_at', CAST(created_at AS STRING)"
            ")) AS value"
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", ALERTS_TOPIC)
        .option("checkpointLocation", os.path.join(OUTPUT_DIR, "chk_alerts_kafka"))
        .outputMode("append")
        .start()
    )

    anomaly_report_to_kafka = (
        anomaly_report.selectExpr(
            "to_json(named_struct("
            "'symbol', symbol, "
            "'alert_type', alert_type, "
            "'trend', trend, "
            "'window_start', CAST(window_start AS STRING), "
            "'window_end', CAST(window_end AS STRING), "
            "'min_price', min_price, "
            "'max_price', max_price, "
            "'avg_price', avg_price, "
            "'first_price', first_price, "
            "'last_price', last_price, "
            "'created_at', CAST(created_at AS STRING)"
            ")) AS value"
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", ALERTS_TOPIC)
        .option("checkpointLocation", os.path.join(OUTPUT_DIR, "chk_anomaly_report_kafka"))
        .outputMode("append")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
