DROP MATERIALIZED VIEW IF EXISTS public.mv_hedge_reentry_exit_on_partial_conditions CASCADE;
CREATE MATERIALIZED VIEW mv_hedge_reentry_exit_on_partial_conditions AS
WITH strategy AS (
    SELECT
        hedge_exit_entry_ratio,
        hedge_exit_multiplier,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   1. EXCLUDE ALL-ENTRY-SL ROUNDS
   ===================================================== */
excluded_rounds AS (
    SELECT DISTINCT
        trade_date,
        expiry_date,
        entry_round
    FROM mv_hedge_reentry_exit_on_all_entry_sl
),

/* =====================================================
   2. CANDIDATE EXIT MINUTES (STATS-DRIVEN)
   ===================================================== */
exit_candidates AS (
    SELECT
        s.trade_date,
        s.expiry_date,
        s.entry_round,
        s.ltp_time AS exit_time,
        CASE
            WHEN s.sl_hit_legs = 0
             AND s.hedge_ltp * c.hedge_exit_entry_ratio > s.total_entry_ltp
                THEN 'EXIT_50PCT_ENTRY_LT_HEDGE'

            WHEN s.sl_hit_legs > 0
             AND s.sl_hit_legs < s.total_entry_legs
             AND s.hedge_ltp > c.hedge_exit_multiplier * s.total_entry_ltp
                THEN 'EXIT_3X_HEDGE'
        END AS exit_reason
    FROM mv_reentry_legs_stats s
    JOIN strategy c ON TRUE
    LEFT JOIN excluded_rounds e
      ON e.trade_date  = s.trade_date
     AND e.expiry_date = s.expiry_date
     AND e.entry_round = s.entry_round
    WHERE e.trade_date IS NULL
      AND (
            (s.sl_hit_legs = 0
             AND s.hedge_ltp * c.hedge_exit_entry_ratio > s.total_entry_ltp)
         OR (s.sl_hit_legs > 0
             AND s.sl_hit_legs < s.total_entry_legs
             AND s.hedge_ltp > c.hedge_exit_multiplier * s.total_entry_ltp)
      )
),

/* =====================================================
   3. EARLIEST VALID EXIT PER ROUND
   ===================================================== */
earliest_exit AS (
    SELECT DISTINCT ON (trade_date, expiry_date, entry_round)
        trade_date,
        expiry_date,
        entry_round,
        exit_time,
        exit_reason
    FROM exit_candidates
    ORDER BY trade_date, expiry_date, entry_round, exit_time
),

/* =====================================================
   4. ACTUAL HEDGE LEGS (TRUE ENTRY PRICE)
   ===================================================== */
hedge_legs AS (
    SELECT *
    FROM mv_reentry_legs_and_hedge_legs
    WHERE leg_type = 'HEDGE-RE-ENTRY'
),

/* =====================================================
   5. HEDGE LIVE PRICES (EXIT PRICE)
   ===================================================== */
hedge_prices AS (
    SELECT *
    FROM mv_reentry_live_prices
    WHERE leg_type = 'HEDGE-RE-ENTRY'
)

/* =====================================================
   6. FINAL HEDGE EXIT (PARTIAL CONDITIONS)
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
    'HEDGE-RE-ENTRY'::TEXT AS leg_type,
    h.transaction_type,

    e.exit_time,

    /* hedge price at exit minute */
    p.option_open AS exit_price,

    e.exit_reason,

    ROUND(
        (h.entry_price - p.option_open)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount

FROM earliest_exit e
JOIN hedge_legs h
  ON h.trade_date  = e.trade_date
 AND h.expiry_date = e.expiry_date
 AND h.entry_round = e.entry_round

JOIN hedge_prices p
  ON p.trade_date  = h.trade_date
 AND p.expiry_date = h.expiry_date
 AND p.option_type = h.option_type
 AND p.strike      = h.strike
 AND p.entry_round = h.entry_round
 AND p.ltp_time    = e.exit_time

JOIN strategy s ON TRUE

ORDER BY
    h.trade_date,
    h.expiry_date,
    e.exit_time;