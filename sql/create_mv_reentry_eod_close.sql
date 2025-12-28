DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_eod_close CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_eod_close AS
WITH strategy AS (
    SELECT
        sl_type,
        sl_percentage,
        box_sl_hard_pct,
        eod_time,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   OPEN ENTRY LEGS (NO SL, NO PROFIT)
   ===================================================== */
open_entry_legs AS (
    SELECT *
    FROM mv_reentry_open_legs
    WHERE NOT EXISTS (
        SELECT 1
        FROM mv_reentry_profit_booking p
        WHERE p.trade_date  = mv_reentry_open_legs.trade_date
          AND p.expiry_date = mv_reentry_open_legs.expiry_date
          AND p.option_type = mv_reentry_open_legs.option_type
          AND p.strike      = mv_reentry_open_legs.strike
          AND p.entry_round = mv_reentry_open_legs.entry_round
    )
),

/* =====================================================
   EOD PRICE
   ===================================================== */
eod_prices AS (
    SELECT
        o.date   AS trade_date,
        o.expiry AS expiry_date,
        o.option_type,
        o.strike,
        o.time   AS exit_time,
        o.open   AS exit_price
    FROM v_nifty_options_filtered o
    JOIN strategy s ON TRUE
    WHERE o.time::TIME = s.eod_time::TIME
)

/* =====================================================
   FINAL EOD EXIT
   ===================================================== */
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
    e.exit_time,
    e.exit_price,
    'EOD_CLOSE' AS exit_reason,
    ROUND(
        (l.entry_price - e.exit_price)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount
FROM open_entry_legs l
JOIN eod_prices e
  ON e.trade_date  = l.trade_date
 AND e.expiry_date = l.expiry_date
 AND e.option_type = l.option_type
 And e.strike      = l.strike
JOIN strategy s ON TRUE
ORDER BY trade_date, expiry_date, strike;