DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_exit_on_partial_hedge CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_exit_on_partial_hedge AS
WITH strategy AS (
    SELECT
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   1. PARTIAL HEDGE EXIT TIMES
   ===================================================== */
partial_hedge_exit AS (
    SELECT
        trade_date,
        expiry_date,
        entry_round,
        exit_time
    FROM mv_hedge_reentry_exit_on_partial_conditions
),

/* =====================================================
   2. ENTRY LEGS
   ===================================================== */
entry_legs AS (
    SELECT *
    FROM mv_reentry_legs_and_hedge_legs
    WHERE leg_type = 'RE-ENTRY'
),

/* =====================================================
   3. ENTRY LIVE PRICES
   ===================================================== */
entry_prices AS (
    SELECT *
    FROM mv_reentry_live_prices
    WHERE leg_type = 'RE-ENTRY'
)

/* =====================================================
   4. FORCE ENTRY EXIT
   ===================================================== */
SELECT
    e.trade_date,
    e.expiry_date,
    e.breakout_time,
    e.entry_time,
    e.spot_price,
    e.option_type,
    e.strike,
    e.entry_price,
    0 AS sl_level,
    e.entry_round,
    'RE-ENTRY'::TEXT AS leg_type,
    e.transaction_type,

    p.exit_time,

    p_price.option_open AS exit_price,

    'EXIT_ON_PARTIAL_HEDGE' AS exit_reason,

    ROUND(
        (e.entry_price - p_price.option_open)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount

FROM partial_hedge_exit p
JOIN entry_legs e
  ON e.trade_date  = p.trade_date
 AND e.expiry_date = p.expiry_date
 AND e.entry_round = p.entry_round

JOIN entry_prices p_price
  ON p_price.trade_date  = e.trade_date
 AND p_price.expiry_date = e.expiry_date
 AND p_price.option_type = e.option_type
 AND p_price.strike      = e.strike
 AND p_price.entry_round = e.entry_round
 AND p_price.ltp_time    = p.exit_time

JOIN strategy s ON TRUE

ORDER BY
    e.trade_date,
    e.expiry_date,
    p.exit_time;