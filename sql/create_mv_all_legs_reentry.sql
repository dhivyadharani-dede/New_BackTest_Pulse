DROP MATERIALIZED VIEW IF EXISTS public.mv_all_legs_reentry CASCADE;
CREATE MATERIALIZED VIEW mv_all_legs_reentry AS

/* =====================================================
   ENTRY – FINAL EXIT (risk + soft exits)
   ===================================================== */
SELECT
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
FROM mv_reentry_final_exit
UNION ALL
SELECT
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
FROM mv_double_buy_legs_reentry
UNION ALL
SELECT
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
FROM mv_hedge_reentry_closed_legs

UNION ALL
SELECT
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
FROM mv_hedge_reentry_eod_exit

/* =====================================================
   RE-HEDGE – EOD EXIT
   ===================================================== */
UNION ALL
SELECT
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
FROM mv_rehedge_eod_exit_reentry

ORDER BY
    trade_date,
    expiry_date,
    entry_round,
    entry_time,
    exit_time,
    strike,
    leg_type;