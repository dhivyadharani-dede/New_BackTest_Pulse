-- Materialized view: all legs (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_all_legs_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_all_legs_round1 AS

/* =====================================================
   ENTRY – FINAL EXIT (risk + soft exits)
   ===================================================== */
SELECT DISTINCT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM mv_entry_final_exit_round1
UNION ALL
SELECT DISTINCT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    '0' AS sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM mv_double_buy_legs_round1
UNION ALL
SELECT DISTINCT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
   '0' AS sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM mv_hedge_closed_legs_round1

UNION ALL
SELECT DISTINCT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
   '0' AS sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM mv_hedge_eod_exit_round1

/* =====================================================
   RE-HEDGE – EOD EXIT
   ===================================================== */
UNION ALL
SELECT DISTINCT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    '0' AS sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM mv_rehedge_eod_exit_round1

ORDER BY
    trade_date,
    expiry_date,
    entry_round,
    entry_time,
    exit_time,
    strike,
    leg_type;

CREATE INDEX IF NOT EXISTS idx_mv_all_legs_round1_date ON public.mv_all_legs_round1 (trade_date, expiry_date, entry_round);
