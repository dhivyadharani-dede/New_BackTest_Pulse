CREATE MATERIALIZED VIEW mv_reentry_breakout_context AS
WITH strategy AS (
    SELECT entry_candle FROM v_strategy_config
)
SELECT
    trade_date,
    ha_high AS breakout_high,
    ha_low  AS breakout_low
FROM (
    SELECT 
        trade_date,
        candle_time,
        ha_high,
        ha_low,
        ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY candle_time) AS rn
    FROM v_ha_big_filtered
) x
JOIN strategy s ON TRUE
WHERE rn = s.entry_candle;