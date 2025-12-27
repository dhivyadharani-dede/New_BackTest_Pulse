-- Heikin-Ashi generator function
CREATE OR REPLACE FUNCTION public.get_heikin_ashi(interval_minutes INT)
RETURNS TABLE (
  trade_date DATE,
  candle_time TIME,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  ha_open NUMERIC,
  ha_high NUMERIC,
  ha_low NUMERIC,
  ha_close NUMERIC
)
LANGUAGE SQL
AS
$$
WITH RECURSIVE base1 AS (
  SELECT 
    date + time::time AS trade_time,
    open, high, low, close
  FROM public."Nifty50"
  WHERE time >= '09:15:00'
),
with_bucket AS (
  SELECT
    date_trunc('day', trade_time) AS trade_date,
    (DATE_TRUNC('day', trade_time) + INTERVAL '9 hours 15 minutes') +
      FLOOR(
        EXTRACT(EPOCH FROM (trade_time - DATE_TRUNC('day', trade_time) - INTERVAL '9 hours 15 minutes'))
        / (interval_minutes * 60)
      ) * (interval_minutes || ' minutes')::interval AS candle_time,
    trade_time,
    open, high, low, close
  FROM base1
),
with_windowed AS (
  SELECT * ,
    FIRST_VALUE(open) OVER w AS first_open,
    LAST_VALUE(close) OVER w AS last_close
  FROM with_bucket
  WINDOW w AS (
    PARTITION BY trade_date, candle_time ORDER BY trade_time 
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  )
),
mv_nifty_candles AS (
  SELECT
    trade_date,
    candle_time,
    MIN(first_open) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    MAX(last_close) AS close
  FROM with_windowed
  GROUP BY trade_date, candle_time
),
base AS (
  SELECT * FROM mv_nifty_candles
),
first_candle AS (
  SELECT * FROM base ORDER BY trade_date, candle_time LIMIT 1
),
recursive_ha AS (
  SELECT 
    b.trade_date,
    b.candle_time,
    b.open, b.high, b.low, b.close,
    (b.open + b.high + b.low + b.close) / 4.0 AS ha_close,
    b.open AS ha_open,
    GREATEST(b.high, b.open, (b.open + b.high + b.low + b.close)/4.0) AS ha_high,
    LEAST(b.low, b.open, (b.open + b.high + b.low + b.close)/4.0) AS ha_low
  FROM first_candle b
  UNION ALL
  SELECT 
    b.trade_date,
    b.candle_time,
    b.open, b.high, b.low, b.close,
    (b.open + b.high + b.low + b.close) / 4.0 AS ha_close,
    (r.ha_open + r.ha_close)/2.0 AS ha_open,
    GREATEST(b.high, (r.ha_open + r.ha_close)/2.0, (b.open + b.high + b.low + b.close)/4.0) AS ha_high,
    LEAST(b.low, (r.ha_open + r.ha_close)/2.0, (b.open + b.high + b.low + b.close)/4.0) AS ha_low
  FROM base b
  JOIN recursive_ha r
    ON b.candle_time = (
      SELECT MIN(c2.candle_time)
      FROM base c2
      WHERE c2.candle_time > r.candle_time
    )
)
SELECT 
  trade_date,
  candle_time::time,
  open, high, low, close,
  ROUND(ha_open, 2) AS ha_open,
  ROUND(ha_high, 2) AS ha_high,
  ROUND(ha_low, 2) AS ha_low,
  ROUND(ha_close, 2) AS ha_close
FROM recursive_ha
ORDER BY trade_date, candle_time;
$$;
