-- SAMPLE STRATEGY SQL for 1-week backtest
-- Expected output columns: ts, symbol, signal, price, size

SELECT
    timestamp as ts,
    'NIFTY50' as symbol,
    CASE WHEN close > open THEN 'buy' ELSE 'sell' END AS signal,
    close AS price,
    1.0 as size
FROM "Nifty50"
WHERE timestamp BETWEEN :start AND :end
ORDER BY timestamp;