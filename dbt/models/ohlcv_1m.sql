{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['symbol', 'window_start']
) }}

SELECT
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
FROM {{ ref('stg_ohlcv_1m') }}