DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_leg_reentry CASCADE;
CREATE MATERIALIZED VIEW mv_rehedge_leg_reentry AS
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
    'REHEDGE_RENTRY' AS leg_type,
    'SELL' AS transaction_type,
    NULL::TIME AS exit_time,
    NULL::NUMERIC AS exit_price,
    'REHEDGE ON ALL ENTRY SL' AS exit_reason,
    0 AS pnl_amount
FROM mv_rehedge_selected_reentry;