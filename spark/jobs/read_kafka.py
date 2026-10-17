import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, LongType, BooleanType

import pyspark.sql.functions as F
def main():
    # Docker Compose đọc .env và truyền các giá trị này vào container.
    output_path = os.getenv("OHLCV_OUTPUT_PATH", "/opt/spark/data/ohlcv_1m")
    checkpoint_path = os.getenv("OHLCV_CHECKPOINT_PATH")
    region = os.getenv("AWS_REGION")

    # Spark dùng s3a:// để đọc/ghi S3, kể cả khi cấu hình nhập s3://.
    output_path = output_path.replace("s3://", "s3a://", 1)
    if output_path.startswith("s3a://") and not checkpoint_path:
        raise ValueError("Hãy điền OHLCV_CHECKPOINT_PATH riêng cho S3 trong .env")
    checkpoint_path = checkpoint_path or "/opt/spark/checkpoints/ohlcv-parquet"
    checkpoint_path = checkpoint_path.replace("s3://", "s3a://", 1)

    # Tạo SparkSession với cấu hình S3 và múi giờ UTC
    spark = (
        SparkSession.builder
        .appName("MarketTradesConsumer")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{region}.amazonaws.com" if region else "s3.amazonaws.com",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Đọc dữ liệu từ Kafka topic "market_trades_raw"
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:19092")
        .option("subscribe", "market_trades_raw")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )

    # Chuyển key và value từ binary sang chuỗi
    messages = raw.selectExpr(
        "CAST(key AS STRING) AS symbol_key",
        "CAST(value AS STRING) AS json_value",
        "partition",
        "offset",
    )
    trade_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("trade_id", LongType(), True),
        StructField("price", DecimalType(18, 8), True),
        StructField("quantity", DecimalType(18, 8), True),
        StructField("trade_time", LongType(), True),
        StructField("event_time", LongType(), True),
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("ingested_at", LongType(), True),
    ])
    trades = messages.withColumn(
        "trade",
        F.from_json(F.col("json_value"), trade_schema)
    ).select(
        "symbol_key",
        "partition",
        "offset",
        "trade.*"
    )
    trades = trades.withColumn(
        "trade_ts",
        F.timestamp_millis("trade_time"),
    )

    trades.printSchema()

    # Chọn thứ tự giao dịch theo thời gian, rồi theo trade_id
    trade_order = F.struct("trade_ts", "trade_id")

    # Tổng hợp nến 1 phút cho từng symbol
    candles = (
        trades
        .withWatermark("trade_ts", "10 seconds")
        .groupBy(
            "symbol",
            F.window("trade_ts", "1 minute").alias("window"),
        )
        .agg(
            F.min_by("price", trade_order).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", trade_order).alias("close"),
            F.sum("quantity").alias("volume"),
            F.sum(
                F.col("price") * F.col("quantity")
            ).alias("quote_volume"),
            F.count("*").alias("trade_count"),
        )
        .withColumn(
            "vwap",
            F.when(
                F.col("volume") > 0,
                F.col("quote_volume") / F.col("volume"),
            ),
        )
        .select(
            "symbol",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
            "vwap",
        )
    )

    candles_to_save = candles.withColumn(
        "trade_date",
        F.to_date("window_start")
    )

    query = (
        candles_to_save.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("trade_date")
        .trigger(processingTime="5 seconds")
        .start()
    )

    try:
        query.awaitTermination()
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
