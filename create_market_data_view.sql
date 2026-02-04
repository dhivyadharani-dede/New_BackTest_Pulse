CREATE OR REPLACE VIEW market_data AS
SELECT
    (date + time)::timestamp as ts,
    'NIFTY' as symbol,
    open,
    high,
    low,
    close,
    volume,
    oi
FROM "Nifty50_y2021"
UNION ALL
SELECT
    (date + time)::timestamp as ts,
    'NIFTY' as symbol,
    open,
    high,
    low,
    close,
    volume,
    oi
FROM "Nifty50_y2022"
UNION ALL
SELECT
    (date + time)::timestamp as ts,
    'NIFTY' as symbol,
    open,
    high,
    low,
    close,
    volume,
    oi
FROM "Nifty50_y2023"
UNION ALL
SELECT
    (date + time)::timestamp as ts,
    'NIFTY' as symbol,
    open,
    high,
    low,
    close,
    volume,
    oi
FROM "Nifty50_y2024"
UNION ALL
SELECT
    (date + time)::timestamp as ts,
    'NIFTY' as symbol,
    open,
    high,
    low,
    close,
    volume,
    oi
FROM "Nifty50_y2025";