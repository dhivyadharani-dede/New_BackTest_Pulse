
-- Materialized view: profit bookings for entry legs (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_profit_booking_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_profit_booking_round1 AS
WITH strategy AS (
    SELECT
        leg_profit_pct,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   OPEN ENTRY LEGS
   ===================================================== */
open_entry_legs AS (
    SELECT *
    FROM mv_entry_open_legs_round1
),

/* =====================================================
   LIVE PRICES AFTER ENTRY
   ===================================================== */
live_prices AS (
    SELECT
        l.trade_date,
        l.expiry_date,
        l.breakout_time,
        l.entry_time,
        l.spot_price,
        l.option_type,
        l.strike,
        l.entry_price,
        l.entry_round,
        l.leg_type,
        l.transaction_type,
        o.time  AS ltp_time,
        o.open  AS option_open
    FROM open_entry_legs l
    JOIN v_nifty_options_filtered o
      ON o.date = l.trade_date
     AND o.expiry = l.expiry_date
     AND o.option_type = l.option_type
     AND o.strike = l.strike
     AND o.time > l.entry_time
),

/* =====================================================
   PROFIT HIT DETECTION
   ===================================================== */
profit_hit AS (
    SELECT
        lp.trade_date,
        lp.expiry_date,
        lp.option_type,
        lp.strike,
        lp.entry_round,
        MIN(lp.ltp_time) AS exit_time
    FROM live_prices lp
    JOIN strategy s ON TRUE
    WHERE lp.option_open
          <= ROUND(lp.entry_price * (1 - s.leg_profit_pct), 2)
    GROUP BY
        lp.trade_date,
        lp.expiry_date,
        lp.option_type,
        lp.strike,
        lp.entry_round
)

/* =====================================================
   FINAL PROFIT BOOKED LEGS
   ===================================================== */
SELECT
    lp.trade_date,
    lp.expiry_date,
    lp.breakout_time,
    lp.entry_time,
    lp.spot_price,
    lp.option_type,
    lp.strike,
    lp.entry_price,
    lp.entry_round,
    lp.leg_type,
    lp.transaction_type,
    p.exit_time,
    lp.option_open AS exit_price,
    'PROFIT_BOOKED' AS exit_reason,
    ROUND(
        (lp.entry_price - lp.option_open)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount
FROM profit_hit p
JOIN live_prices lp
  ON lp.trade_date  = p.trade_date
 AND lp.expiry_date = p.expiry_date
 AND lp.option_type = p.option_type
 AND lp.strike      = p.strike
 AND lp.entry_round = p.entry_round
 AND lp.ltp_time    = p.exit_time
JOIN strategy s ON TRUE
ORDER BY trade_date, expiry_date, exit_time, strike;

CREATE INDEX IF NOT EXISTS idx_mv_entry_profit_booking_round1_date ON public.mv_entry_profit_booking_round1 (trade_date, expiry_date);
