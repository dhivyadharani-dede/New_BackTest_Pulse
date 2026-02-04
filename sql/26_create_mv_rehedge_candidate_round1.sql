-- Materialized view: rehedge candidates (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_candidate_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_rehedge_candidate_round1 AS
SELECT
    h.trade_date,
    h.expiry_date,
    h.entry_round,
    t.rehedge_trigger_time + INTERVAL '1 minute' AS entry_time,
    h.spot_price,
    CASE
        WHEN h.option_type = 'C' THEN 'P'
        ELSE 'C'
    END AS option_type,
    o.strike,
    o.open AS entry_price,
    ABS(o.open - h.exit_price) AS premium_diff,
    h.exit_price AS prev_hedge_exit_price,
    o.time AS option_time
FROM mv_hedge_exit_on_all_entry_sl h
JOIN mv_rehedge_trigger_round1 t
  ON h.trade_date  = t.trade_date
 AND h.expiry_date = t.expiry_date
 AND h.entry_round = t.entry_round
 -- AND h.exit_time = t.rehedge_trigger_time
JOIN v_nifty_options_filtered o
  ON o.date   = h.trade_date
 AND o.expiry = h.expiry_date
 AND o.time = (h.exit_time + INTERVAL '1 minute')
 AND o.option_type =
   CASE WHEN h.option_type = 'C' THEN 'P' ELSE 'C' END
 AND o.time > t.rehedge_trigger_time
;

CREATE INDEX IF NOT EXISTS idx_mv_rehedge_candidate_round1_date ON public.mv_rehedge_candidate_round1 (trade_date, expiry_date, entry_round);
