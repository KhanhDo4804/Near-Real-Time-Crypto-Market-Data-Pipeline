{{ config(materialized='view') }}

SELECT DISTINCT
    symbol,
    window_start,
    window_end,
    "open",
    high,
    low,
    "close",
    volume,
    trade_count,
    vwap
FROM {{ source('raw_market', 'ohlcv_1m') }}