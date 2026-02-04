-- Materialized view: rehedge trigger (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_trigger_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_rehedge_trigger_round1 AS
SELECT
    s.trade_date,
    s.expiry_date,
    s.entry_round,
    MAX(sl.exit_time) AS rehedge_trigger_time
FROM mv_entry_round1_stats s
JOIN mv_entry_sl_hits_round1 sl
  ON s.trade_date  = sl.trade_date
 AND s.expiry_date = sl.expiry_date
 AND s.entry_round = sl.entry_round
WHERE s.sl_hit_legs = s.total_entry_legs   -- 🔑 ALL ENTRY SL
GROUP BY
    s.trade_date,
    s.expiry_date,
    s.entry_round;

CREATE INDEX IF NOT EXISTS idx_mv_rehedge_trigger_round1_date ON public.mv_rehedge_trigger_round1 (trade_date, expiry_date, entry_round);
