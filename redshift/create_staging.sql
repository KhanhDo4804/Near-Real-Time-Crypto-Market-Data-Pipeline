-- Chạy trong Redshift Query Editor v2.
-- Thứ tự và kiểu cột khớp với file Parquet Spark hiện tại.
-- trade_date là partition trên S3, không nằm trong nội dung file Parquet.
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.ohlcv_1m (
    symbol       VARCHAR(32),
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    "open"       DECIMAL(18, 8),
    high         DECIMAL(18, 8),
    low          DECIMAL(18, 8),
    "close"      DECIMAL(18, 8),
    volume       DECIMAL(28, 8),
    trade_count  BIGINT,
    vwap         DECIMAL(38, 8)
)
DISTSTYLE AUTO
SORTKEY (window_start, symbol);
