-- Materialized view: closed entry legs (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_closed_legs_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_closed_legs_round1 AS
WITH

/* =====================================================
   SL EXECUTED ENTRY LEGS
   ===================================================== */
sl_exits AS (
    SELECT
        trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        spot_price,
        option_type,
        strike,
        entry_price,
        entry_round,
        leg_type,
        transaction_type,
        exit_time,
        exit_price,
        exit_reason,
        pnl_amount
    FROM mv_entry_sl_executions_round1
),

/* =====================================================
   PROFIT BOOKED ENTRY LEGS
   ===================================================== */
profit_exits AS (
    SELECT
        trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        spot_price,
        option_type,
        strike,
        entry_price,
        entry_round,
        leg_type,
        transaction_type,
        exit_time,
        exit_price,
        exit_reason,
        pnl_amount
    FROM mv_entry_profit_booking_round1
),

/* =====================================================
   EOD CLOSED ENTRY LEGS
   ===================================================== */
eod_exits AS (
    SELECT
        trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        spot_price,
        option_type,
        strike,
        entry_price,
        entry_round,
        leg_type,
        transaction_type,
        exit_time,
        exit_price,
        exit_reason,
        pnl_amount
    FROM mv_entry_eod_close_round1
),

/* =====================================================
   UNION ALL ENTRY EXITS
   ===================================================== */
all_entry_exits AS (
    SELECT * FROM sl_exits
    UNION ALL
    SELECT * FROM profit_exits
    UNION ALL
    SELECT * FROM eod_exits
),

/* =====================================================
   SAFETY: EARLIEST EXIT WINS
   ===================================================== */
ranked AS (
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

SELECT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM ranked
WHERE rn = 1
ORDER BY trade_date, expiry_date, entry_time, strike;

CREATE INDEX IF NOT EXISTS idx_mv_entry_closed_legs_round1_date ON public.mv_entry_closed_legs_round1 (trade_date, expiry_date);
