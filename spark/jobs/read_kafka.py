from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, LongType, BooleanType

import pyspark.sql.functions as F
def main():
    spark = (
        SparkSession.builder
        .appName("MarketTradesConsumer")
        .config("spark.sql.session.timeZone", "UTC")
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
        .option("maxOffsetsPerTrigger", 1000)
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

    # In dữ liệu để kiểm tra
    query = (
        trades.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 10)
        .option("checkpointLocation", "/tmp/checkpoints/read-kafka")
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