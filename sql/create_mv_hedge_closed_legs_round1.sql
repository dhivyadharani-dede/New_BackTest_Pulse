-- Materialized view: hedge closed legs (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_hedge_closed_legs_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_hedge_closed_legs_round1 AS
SELECT *
FROM mv_hedge_exit_on_all_entry_sl

UNION ALL

SELECT *
FROM mv_hedge_exit_partial_conditions

ORDER BY
    trade_date,
    expiry_date,
    entry_round,
    exit_time;

CREATE INDEX IF NOT EXISTS idx_mv_hedge_closed_legs_round1_date ON public.mv_hedge_closed_legs_round1 (trade_date, expiry_date);
