DROP MATERIALIZED VIEW IF EXISTS public.mv_double_buy_legs_reentry CASCADE;
CREATE MATERIALIZED VIEW mv_double_buy_legs_reentry AS
WITH strategy AS (
    SELECT
        eod_time,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   1. ENTRY SL-EXITED LEGS
   ===================================================== */
sl_exited_entries AS (
    SELECT *
    FROM mv_reentry_final_exit where exit_reason like 'SL_HIT%'
),

/* =====================================================
   2. ENTRY LEG DETAILS
   ===================================================== */
entry_legs AS (
    SELECT *
    FROM mv_reentry_legs_and_hedge_legs
    WHERE leg_type = 'RE-ENTRY'
),

/* =====================================================
   3. EOD PRICES
   ===================================================== */
eod_prices AS (
    SELECT *
    FROM wrk_reentry_live_prices
    WHERE leg_type = 'RE-ENTRY'
)

/* =====================================================
   4. DOUBLE BUY LEG
   ===================================================== */
SELECT
    e.trade_date,
    e.expiry_date,
    e.breakout_time,
    s.exit_time AS entry_time,   -- double buy entry = SL exit time
    e.spot_price,
    e.option_type,
    e.strike,

    s.exit_price AS entry_price, -- buy at SL price

    0 AS sl_level,
    e.entry_round,
    'DOUBLE_BUY_REENTRY' AS leg_type,
    'BUY' AS transaction_type,

    c.eod_time AS exit_time,
    p.option_close AS exit_price,

    'DOUBLE_BUY_REENTRY_EOD_EXIT' AS exit_reason,

    ROUND(
        (p.option_close-s.exit_price)
        * c.lot_size
        * c.no_of_lots,
        2
    ) AS pnl_amount

FROM sl_exited_entries s
JOIN entry_legs e
  ON e.trade_date  = s.trade_date
 AND e.expiry_date = s.expiry_date
 AND e.option_type = s.option_type
 AND e.strike      = s.strike
 AND e.entry_round = s.entry_round

JOIN strategy c ON TRUE
JOIN eod_prices p
  ON p.trade_date  = e.trade_date
 AND p.expiry_date = e.expiry_date
 AND p.option_type = e.option_type
 AND p.strike      = e.strike
 AND p.entry_round = e.entry_round
 AND p.ltp_time::TIME = c.eod_time::TIME;