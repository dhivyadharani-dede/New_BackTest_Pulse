DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_open_legs CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_open_legs AS
WITH

/* =====================================================
   ALL ENTRY LEGS (ROUND 2)
   ===================================================== */
entry_legs AS (
    SELECT *
    FROM mv_reentry_legs_and_hedge_legs
    WHERE leg_type = 'RE-ENTRY'
      AND entry_round = 2
),

/* =====================================================
   ENTRY SL HITS
   ===================================================== */
sl_hit_keys AS (
    SELECT
        trade_date,
        expiry_date,
        option_type,
        strike,
        entry_round
    FROM mv_reentry_sl_hits
)

/* =====================================================
   OPEN ENTRY LEGS (NO SL HIT)
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
    e.entry_round,
    e.leg_type,
    e.transaction_type
FROM entry_legs e
WHERE NOT EXISTS (
    SELECT 1
    FROM sl_hit_keys s
    WHERE s.trade_date  = e.trade_date
      AND s.expiry_date = e.expiry_date
      AND s.option_type = e.option_type
      AND s.strike      = e.strike
      AND s.entry_round = e.entry_round
);