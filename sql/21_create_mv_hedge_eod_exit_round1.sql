-- Materialized view: hedge EOD exits (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_hedge_eod_exit_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_hedge_eod_exit_round1 AS
WITH strategy AS (
    SELECT
        eod_time,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   1. ALL HEDGE LEGS
   ===================================================== */
hedge_legs AS (
    SELECT *
    FROM mv_entry_and_hedge_legs
    WHERE leg_type = 'HEDGE'
),

/* =====================================================
   2. ALREADY CLOSED HEDGE LEGS
   ===================================================== */
closed_hedges AS (
    SELECT DISTINCT
        trade_date,
        expiry_date,
        entry_round
    FROM mv_hedge_closed_legs_round1
),

/* =====================================================
   3. OPEN HEDGE LEGS (NO EXIT YET)
   ===================================================== */
open_hedges AS (
    SELECT h.*
    FROM hedge_legs h
    LEFT JOIN closed_hedges c
      ON c.trade_date  = h.trade_date
     AND c.expiry_date = h.expiry_date
     AND c.entry_round = h.entry_round
    WHERE c.trade_date IS NULL
),

/* =====================================================
   4. HEDGE PRICE AT EOD
   ===================================================== */
hedge_eod_price AS (
    SELECT *
    FROM mv_live_prices_entry_round1
    WHERE leg_type = 'HEDGE'
)

/* =====================================================
   5. FINAL HEDGE EOD EXIT
   ===================================================== */
SELECT
    h.trade_date,
    h.expiry_date,
    h.breakout_time,
    h.entry_time,
    h.spot_price,
    h.option_type,
    h.strike,

    /* true hedge entry price */
    h.entry_price,

    0 AS sl_level,
    h.entry_round,
    'HEDGE'::TEXT AS leg_type,
    h.transaction_type,

    s.eod_time AS exit_time,

    p.option_open AS exit_price,

    'EOD_CLOSE' AS exit_reason,

    ROUND(
        (h.entry_price - p.option_open)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount

FROM open_hedges h
JOIN strategy s ON TRUE
JOIN hedge_eod_price p
  ON p.trade_date  = h.trade_date
 AND p.expiry_date = h.expiry_date
 AND p.option_type = h.option_type
 AND p.strike      = h.strike
 AND p.entry_round = h.entry_round
 AND p.ltp_time::TIME = s.eod_time::TIME

ORDER BY
    h.trade_date,
    h.expiry_date,
    s.eod_time;

CREATE INDEX IF NOT EXISTS idx_mv_hedge_eod_exit_round1_date ON public.mv_hedge_eod_exit_round1 (trade_date, expiry_date);
