DROP MATERIALIZED VIEW IF EXISTS public.mv_all_5min_breakouts CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_all_5min_breakouts AS
WITH ha_bounds AS (
    SELECT
        h.trade_date,
        h.candle_time,
        h.ha_high,
        h.ha_low
    FROM (
        SELECT 
            trade_date,
            candle_time,
            ha_high,
            ha_low,
            ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY candle_time) AS rn
        FROM v_ha_big_filtered
    ) h
    JOIN v_strategy_config s
      ON h.rn = s.entry_candle
),

combined AS (
    SELECT
        f.trade_date,
        f.candle_time,
        f.ha_open,
        f.ha_close,
        f.ha_high,
        f.ha_low,

        h.ha_high AS ha_15m_high,
        h.ha_low  AS ha_15m_low,

        s.breakout_threshold_pct,
        f.ha_close - f.ha_open AS candle_body,

        CASE
            WHEN f.ha_open  > h.ha_high
             AND f.ha_close > h.ha_high
             AND f.ha_high  > h.ha_high
             AND f.ha_low   > h.ha_high
            THEN 'full_body_bullish'

            WHEN f.ha_close >
                 (h.ha_high + ABS(f.ha_close - f.ha_open) * s.breakout_threshold_pct)
             AND f.ha_high > h.ha_high
            THEN 'pct_breakout_bullish'

            WHEN f.ha_open  < h.ha_low
             AND f.ha_close < h.ha_low
             AND f.ha_high  < h.ha_low
             AND f.ha_low   < h.ha_low
            THEN 'full_body_bearish'

            WHEN f.ha_close <
                 (h.ha_low - ABS(f.ha_close - f.ha_open) * s.breakout_threshold_pct)
             AND f.ha_low < h.ha_low
            THEN 'pct_breakout_bearish'

            ELSE NULL
        END AS breakout_type
    FROM v_ha_small_filtered f
    JOIN ha_bounds h
      ON f.trade_date = h.trade_date
    JOIN v_strategy_config s
      ON TRUE
    WHERE f.candle_time >=
          TIME '09:15:00'
          + (s.entry_candle * s.big_candle_tf || ' minutes')::interval
)

SELECT
    trade_date,
    candle_time AS breakout_time,
    candle_time + (s.small_candle_tf || ' minutes')::interval AS entry_time,
    ha_open,
    ha_close,
    ha_high,
    ha_low,
    ha_15m_high,
    ha_15m_low,
    breakout_type
FROM combined
JOIN v_strategy_config s ON TRUE
WHERE breakout_type IS NOT NULL;

-- create an index to speed lookups
CREATE INDEX IF NOT EXISTS idx_mv_all_5min_breakouts_date_time ON public.mv_all_5min_breakouts (trade_date, breakout_time);