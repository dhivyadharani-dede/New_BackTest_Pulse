CREATE MATERIALIZED VIEW mv_all_entries_sl_tracking_adjusted AS
WITH config AS (
    SELECT 
        sl_type,
        sl_percentage,
        box_sl_trigger_pct,
        box_sl_hard_pct,
        eod_time,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

next_round_reentry_times AS (
    SELECT
        trade_date,
        entry_round - 1 AS prior_round,
        entry_time AS next_round_start_time
    FROM mv_reentry_triggered_breakouts
),

all_legs AS (
       SELECT * FROM mv_all_legs_REENTRY
    UNION ALL
    SELECT * FROM mv_all_legs_round1
),

adjusted_exit_time_data AS (
    SELECT
        l.*,
        CASE
            WHEN r.next_round_start_time IS NOT NULL
             AND l.entry_round = r.prior_round
             AND l.exit_time > r.next_round_start_time
            THEN r.next_round_start_time
            ELSE l.exit_time
        END AS adjusted_exit_time,
        CASE
            WHEN r.next_round_start_time IS NOT NULL
             AND l.entry_round = r.prior_round
             AND l.exit_time > r.next_round_start_time
            THEN 'Closed due to re-entry'
            ELSE l.exit_reason
        END AS adjusted_exit_reason
    FROM all_legs l
    LEFT JOIN next_round_reentry_times r
      ON r.trade_date = l.trade_date
     AND r.prior_round = l.entry_round
),

adjusted_exit_price_data AS (
    SELECT
        l.*,
        CASE
    WHEN adjusted_exit_reason = 'Closed due to re-entry'
        THEN p.option_open
    ELSE
        l.exit_price
END AS adjusted_exit_price
        -- COALESCE(p.option_open,l.exit_price) AS adjusted_exit_price
    FROM adjusted_exit_time_data l
    LEFT JOIN wrk_entry_leg_live_prices p
      ON p.trade_date  = l.trade_date
     AND p.expiry_date = l.expiry_date
     AND p.option_type = l.option_type
     AND p.strike      = l.strike
     AND p.ltp_time    = l.adjusted_exit_time
     WHERE l.adjusted_exit_time>l.entry_time
)

SELECT DISTINCT ON (
    trade_date, expiry_date,entry_time, option_type, strike, leg_type, entry_round
)
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    sl_level,
    entry_round,
    leg_type,
    transaction_type,
    adjusted_exit_time AS exit_time,
    adjusted_exit_price AS exit_price,
    adjusted_exit_reason AS exit_reason,
    ROUND(
        CASE 
            WHEN transaction_type = 'BUY'
                THEN (adjusted_exit_price - entry_price)
            ELSE (entry_price - adjusted_exit_price)
        END * lot_size * no_of_lots,
        2
    ) AS pnl_amount
FROM adjusted_exit_price_data
JOIN config ON TRUE
--where trade_date='2025-04-03'
ORDER BY
    trade_date, expiry_date,entry_time, option_type, strike, leg_type, entry_round, adjusted_exit_time;