SELECT symbol, window_start, COUNT(*) AS row_count
FROM {{ ref('stg_ohlcv_1m') }}
GROUP BY symbol, window_start
HAVING COUNT(*) > 1