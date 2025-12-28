-- Materialized view: hedge exit when all entry legs hit SL (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_hedge_exit_on_all_entry_sl CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_hedge_exit_on_all_entry_sl AS
WITH strategy AS (
    SELECT
        no_of_lots,
        lot_size
    FROM v_strategy_config
),
/* =====================================================
   1. LAST ENTRY SL EXIT TIME (ACTUAL EXECUTION)
   ===================================================== */
entry_last_sl_time AS (
    SELECT
        trade_date,
        expiry_date,
        entry_round,
        MAX(exit_time) AS exit_time
    FROM mv_entry_closed_legs_round1
    WHERE leg_type = 'ENTRY'
      AND exit_reason LIKE 'SL_%'
    GROUP BY
        trade_date,
        expiry_date,
        entry_round
),

/* =====================================================
   2. CONFIRM ALL ENTRY LEGS HIT SL (USING STATS MV)
   ===================================================== */
all_entry_sl_completed AS (
    SELECT
        s.trade_date,
        s.expiry_date,
        s.entry_round,
        t.exit_time
    FROM mv_entry_round1_stats s
    JOIN entry_last_sl_time t
      ON s.trade_date  = t.trade_date
     AND s.expiry_date = t.expiry_date
     AND s.entry_round = t.entry_round
     AND s.ltp_time = t.exit_time
    WHERE s.sl_hit_legs = s.total_entry_legs
),

/* =====================================================
   3. ACTUAL HEDGE LEGS (TRUE ENTRY PRICE)
   ===================================================== */
hedge_legs AS (
    SELECT *
    FROM mv_entry_and_hedge_legs
    WHERE leg_type = 'HEDGE'
),

/* =====================================================
   4. HEDGE LIVE PRICES (EXIT PRICE)
   ===================================================== */
hedge_prices AS (
    SELECT *
    FROM mv_live_prices_entry_round1
    WHERE leg_type = 'HEDGE'
)

/* =====================================================
   5. FINAL HEDGE EXIT
   ===================================================== */
SELECT
    h.trade_date,
    h.expiry_date,
    h.breakout_time,
    h.entry_time,
    h.spot_price,
    h.option_type,
    h.strike,

    /* ✅ true hedge entry price */
    h.entry_price,

    0 AS sl_level,
    h.entry_round,
    'HEDGE'::TEXT AS leg_type,
    h.transaction_type,

    a.exit_time,

    /* ✅ hedge exit price at correct minute */
    p.option_open AS exit_price,

    'ALL_ENTRY_SL' AS exit_reason,

    ROUND(
        (h.entry_price - p.option_open)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount

FROM all_entry_sl_completed a
JOIN hedge_legs h
  ON h.trade_date  = a.trade_date
 AND h.expiry_date = a.expiry_date
 AND h.entry_round = a.entry_round

JOIN hedge_prices p
  ON p.trade_date  = h.trade_date
 AND p.expiry_date = h.expiry_date
 AND p.option_type = h.option_type
 AND p.strike      = h.strike
 AND p.entry_round = h.entry_round
 AND p.ltp_time    = a.exit_time

JOIN strategy s ON TRUE

ORDER BY
    h.trade_date,
    h.expiry_date,
    a.exit_time;

CREATE INDEX IF NOT EXISTS idx_mv_hedge_exit_on_all_entry_sl_date ON public.mv_hedge_exit_on_all_entry_sl (trade_date, expiry_date);
