CREATE MATERIALIZED VIEW mv_reentry_final_exit AS
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
    FROM mv_reentry_sl_executions
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
    FROM mv_reentry_profit_booking
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
    FROM mv_reentry_eod_close
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