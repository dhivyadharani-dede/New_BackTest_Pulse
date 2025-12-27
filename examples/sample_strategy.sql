-- SAMPLE STRATEGY SQL (placeholder)
-- Expected output columns: ts, symbol, signal, price, size

SELECT
    ts,
    symbol,
    CASE WHEN close > open THEN 'buy' ELSE 'sell' END AS signal,
    close AS price,
    1.0 as size
FROM market_data
WHERE ts BETWEEN :start AND :end
ORDER BY ts;
