-- Materialized view: breakout context (high/low) for entry round 1
DROP MATERIALIZED VIEW IF EXISTS public.mv_breakout_context_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_breakout_context_round1 AS
WITH strategy AS (
    SELECT entry_candle FROM public.v_strategy_config LIMIT 1
)
SELECT
    x.trade_date,
    x.ha_high AS breakout_high,
    x.ha_low  AS breakout_low
FROM (
    SELECT 
        trade_date,
        candle_time,
        ha_high,
        ha_low,
        ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY candle_time) AS rn
    FROM public.v_ha_big_filtered
) x
JOIN strategy s ON TRUE
WHERE x.rn = s.entry_candle;

CREATE INDEX IF NOT EXISTS idx_mv_breakout_context_round1_date ON public.mv_breakout_context_round1 (trade_date);
