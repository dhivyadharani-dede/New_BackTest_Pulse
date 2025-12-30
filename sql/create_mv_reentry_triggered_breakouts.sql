-- Materialized view: re-entry triggered breakouts
DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_triggered_breakouts CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_reentry_triggered_breakouts AS
WITH config AS (
    SELECT 
        max_reentry_rounds,
        reentry_breakout_type
    FROM public.v_strategy_config
    LIMIT 1
),

/* =====================================================
   STEP 1: FIRST SL HIT TIME PER ENTRY ROUND
   ===================================================== */
first_sl_hit AS (
    SELECT 
        trade_date,
        expiry_date,
        entry_round,
        MIN(exit_time) AS first_sl_exit_time
    FROM public.mv_all_legs_reentry
    WHERE exit_reason LIKE 'SL_HIT_%'
    GROUP BY trade_date, expiry_date, entry_round
),

/* =====================================================
   STEP 2: CALCULATE SCAN START TIME (NEXT 5-MIN CANDLE)
   ===================================================== */
scan_start_time AS (
    SELECT 
        f.trade_date,
        f.expiry_date,
        f.entry_round + 1 AS next_entry_round,
        (
            date_trunc('hour', f.first_sl_exit_time)
            + INTERVAL '1 minute'
              * CEIL(EXTRACT(MINUTE FROM f.first_sl_exit_time)::INT / 5.0) * 5
        ) AS scan_start_time,
        c.max_reentry_rounds
    FROM first_sl_hit f
    JOIN config c ON TRUE
    WHERE f.entry_round < c.max_reentry_rounds
),

/* =====================================================
   STEP 3: FIND NEXT VALID BREAKOUT AFTER SL
   ===================================================== */
ranked_next_breakouts AS (
    SELECT 
        b.trade_date,
        b.breakout_time,
        b.breakout_type,
        b.entry_option_type,
        s.next_entry_round,
        ROW_NUMBER() OVER (
            PARTITION BY b.trade_date, s.next_entry_round
            ORDER BY b.breakout_time
        ) AS rn
    FROM public.mv_ranked_breakouts_with_rounds_for_reentry b
    JOIN scan_start_time s
      ON b.trade_date = s.trade_date
     AND b.breakout_time >= s.scan_start_time
)

/* =====================================================
   STEP 4: PICK FIRST BREAKOUT FOR EACH RE-ENTRY ROUND
   ===================================================== */
SELECT 
    trade_date,
    breakout_time,
    breakout_time + INTERVAL '5 minutes' AS entry_time,
    breakout_type,
    entry_option_type,
    next_entry_round AS entry_round
FROM ranked_next_breakouts
WHERE rn = 1;

CREATE INDEX IF NOT EXISTS idx_mv_reentry_triggered_breakouts_date_round ON public.mv_reentry_triggered_breakouts (trade_date, entry_round);
