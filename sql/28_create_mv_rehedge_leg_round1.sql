-- Materialized view: rehedge leg (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_leg_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_rehedge_leg_round1 AS
SELECT
    trade_date,
    expiry_date,
    NULL::TIME AS breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    0 AS sl_level,
    entry_round,
    'REHEDGE' AS leg_type,
    'SELL' AS transaction_type,
    NULL::TIME AS exit_time,
    NULL::NUMERIC AS exit_price,
    'REHEDGE ON ALL ENTRY SL' AS exit_reason,
    0 AS pnl_amount
FROM mv_rehedge_selected_round1;

CREATE INDEX IF NOT EXISTS idx_mv_rehedge_leg_round1_date ON public.mv_rehedge_leg_round1 (trade_date, expiry_date, entry_round);
