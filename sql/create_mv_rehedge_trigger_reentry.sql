DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_trigger_reentry CASCADE;
CREATE MATERIALIZED VIEW mv_rehedge_trigger_reentry AS
SELECT
    s.trade_date,
    s.expiry_date,
    s.entry_round,
    MAX(sl.exit_time) AS rehedge_trigger_time
FROM mv_reentry_legs_stats s
JOIN mv_reentry_sl_hits sl
  ON s.trade_date  = sl.trade_date
 AND s.expiry_date = sl.expiry_date
 AND s.entry_round = sl.entry_round
WHERE s.sl_hit_legs = s.total_entry_legs   -- 🔑 ALL ENTRY SL
GROUP BY
    s.trade_date,
    s.expiry_date,
    s.entry_round;