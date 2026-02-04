-- Materialized view: entry round1 stats
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_round1_stats CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_round1_stats AS
WITH

/* =====================================================
   ALL LEGS (ROUND 1)
   ===================================================== */
legs AS (
    SELECT *
    FROM mv_entry_and_hedge_legs
    WHERE entry_round = 1
),

/* =====================================================
   ENTRY SL-HIT LEGS
   ===================================================== */
entry_sl_hits AS (
    SELECT
        trade_date,
        expiry_date,
        option_type,
        strike,
        entry_round
    FROM mv_entry_closed_legs_round1
    WHERE leg_type = 'ENTRY'
      AND exit_reason LIKE 'SL_HIT%'
),

/* =====================================================
   LIVE PRICES (TIME-ALIGNED)
   ===================================================== */
live_prices AS (
    SELECT *
    FROM wrk_live_prices_entry_round1
)

/* =====================================================
   FINAL AGGREGATION (TIME-SAFE)
   ===================================================== */
SELECT
    lp.trade_date,
    lp.expiry_date,
    lp.entry_round,
    lp.ltp_time,

    /* ---------- ENTRY COUNTS ---------- */
    COUNT(*) FILTER (WHERE l.leg_type = 'ENTRY') AS total_entry_legs,

    COUNT(*) FILTER (
        WHERE l.leg_type = 'ENTRY'
          AND EXISTS (
              SELECT 1
              FROM entry_sl_hits s
              WHERE s.trade_date  = l.trade_date
                AND s.expiry_date = l.expiry_date
                AND s.option_type = l.option_type
                AND s.strike      = l.strike
                AND s.entry_round = l.entry_round
          )
    ) AS sl_hit_legs,

    /* ---------- TIME-ALIGNED PREMIUMS ---------- */
    SUM(lp.option_open) FILTER (WHERE l.leg_type = 'ENTRY')
        AS total_entry_ltp,

    MAX(lp.option_open) FILTER (WHERE l.leg_type = 'HEDGE')
        AS hedge_ltp

FROM live_prices lp
JOIN legs l
  ON l.trade_date  = lp.trade_date
 AND l.expiry_date = lp.expiry_date
 AND l.option_type = lp.option_type
 AND l.strike      = lp.strike
 AND l.entry_round = lp.entry_round

GROUP BY
    lp.trade_date,
    lp.expiry_date,
    lp.entry_round,
    lp.ltp_time;

CREATE INDEX IF NOT EXISTS idx_mv_entry_round1_stats_date ON public.mv_entry_round1_stats (trade_date, expiry_date, entry_round);
