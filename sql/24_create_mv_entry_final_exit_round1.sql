-- Materialized view: final entry exits (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_final_exit_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_final_exit_round1 AS
WITH

/* =====================================================
   1. ALL ENTRY EXIT CANDIDATES
   ===================================================== */
all_entry_exits AS (

    /* 1️⃣ ENTRY SL exits */
    SELECT
        trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        spot_price,
        option_type,
        strike,
        entry_price,
      --  sl_level,
        entry_round,
        leg_type,
        transaction_type,
        exit_time,
        exit_price,
        exit_reason,
        pnl_amount
    FROM mv_entry_closed_legs_round1
    WHERE leg_type = 'ENTRY'
     -- AND exit_reason LIKE 'SL_HIT%'

    UNION ALL

    /* 2️⃣ ENTRY exits due to partial hedge exit */
    SELECT
        trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        spot_price,
        option_type,
        strike,
        entry_price,
   --     sl_level,
        entry_round,
        leg_type,
        transaction_type,
        exit_time,
        exit_price,
        exit_reason,
        pnl_amount
    FROM mv_entry_exit_on_partial_hedge_round1

    
),

/* =====================================================
   2. EARLIEST EXIT PER ENTRY LEG WINS
   ===================================================== */
ranked_entry_exits AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY
                   trade_date,
                   expiry_date,
                   option_type,
                   strike,
                   entry_round
               ORDER BY exit_time
           ) AS rn
    FROM all_entry_exits
)

/* =====================================================
   3. FINAL ENTRY EXIT
   ===================================================== */
SELECT 
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
   '0' sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM ranked_entry_exits
WHERE rn = 1
ORDER BY
    trade_date,
    expiry_date,
    entry_round,
    exit_time,
    strike;

CREATE INDEX IF NOT EXISTS idx_mv_entry_final_exit_round1_date ON public.mv_entry_final_exit_round1 (trade_date, expiry_date);
