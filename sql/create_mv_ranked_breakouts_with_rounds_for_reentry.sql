-- Materialized view for reentry-ranked breakouts
DROP MATERIALIZED VIEW IF EXISTS public.mv_ranked_breakouts_with_rounds_for_reentry CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_ranked_breakouts_with_rounds_for_reentry AS
WITH strategy AS (
    SELECT reentry_breakout_type FROM public.strategy_settings LIMIT 1
),
filtered_breakouts AS (
    SELECT
        b.trade_date,
        b.breakout_time,
        b.entry_time,
        b.ha_open,
        b.ha_close,
        b.ha_high,
        b.ha_low,
        b.ha_15m_high,
        b.ha_15m_low,
        b.breakout_type
    FROM public.mv_all_5min_breakouts b
    CROSS JOIN strategy s
    JOIN public.mv_ranked_breakouts_with_rounds r
      ON b.trade_date = r.trade_date
    WHERE b.breakout_type IS NOT NULL
      AND (
            (s.reentry_breakout_type = 'full_candle_breakout' AND b.breakout_type IN ('full_body_bullish', 'full_body_bearish'))
         OR (s.reentry_breakout_type = 'pct_based_breakout' AND b.breakout_type IN ('pct_breakout_bullish', 'pct_breakout_bearish','full_body_bullish', 'full_body_bearish'))
      )
      AND r.entry_round = 1
      AND RIGHT(b.breakout_type, 7) = RIGHT(r.breakout_type, 7)
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY breakout_time) AS entry_round
    FROM filtered_breakouts
)
SELECT
    trade_date,
    breakout_time AS breakout_time,
    (breakout_time + INTERVAL '5 minute') AS entry_time,
    breakout_type,
    CASE
        WHEN breakout_type LIKE '%bullish%' THEN 'P'
        WHEN breakout_type LIKE '%bearish%' THEN 'C'
        ELSE NULL
    END AS entry_option_type,
    entry_round
FROM ranked;

CREATE INDEX IF NOT EXISTS idx_mv_ranked_breakouts_reentry_date_time ON public.mv_ranked_breakouts_with_rounds_for_reentry (trade_date, breakout_time);
