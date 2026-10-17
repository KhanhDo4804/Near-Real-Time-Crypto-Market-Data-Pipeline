-- Bảng chính lưu lịch sử nến, không xóa mỗi lần nạp staging.
CREATE SCHEMA IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.ohlcv_1m (
    symbol       VARCHAR(32) NOT NULL,
    window_start TIMESTAMP NOT NULL,
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
