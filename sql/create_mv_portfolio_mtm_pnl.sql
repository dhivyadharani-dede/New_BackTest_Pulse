CREATE MATERIALIZED VIEW mv_portfolio_mtm_pnl AS
WITH config AS (
    SELECT 
        portfolio_capital,
        portfolio_profit_target_pct,
        portfolio_stop_loss_pct,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

all_legs AS (
    SELECT * FROM mv_all_entries_sl_tracking_adjusted
),

all_times AS (
    SELECT DISTINCT
        date,
        expiry,
        time
    FROM v_nifty_options_filtered
    WHERE time >= '09:36:00'
),

closed_pnl_at_time AS (
    SELECT
        l.trade_date,
        l.expiry_date,
        t.time,
        SUM(
            CASE
                WHEN l.transaction_type = 'SELL'
                    THEN (l.entry_price - l.exit_price)
                ELSE (l.exit_price - l.entry_price)
            END * c.lot_size * c.no_of_lots
        ) AS realized_pnl
    FROM all_times t
    JOIN all_legs l
      ON l.trade_date = t.date
     AND l.expiry_date = t.expiry
    JOIN config c ON TRUE
    WHERE l.exit_time IS NOT NULL
      AND l.exit_time < t.time
    GROUP BY l.trade_date, l.expiry_date, t.time
),

open_mtm_at_time AS (
    SELECT
        l.trade_date,
        l.expiry_date,
        t.time,
        SUM(
            CASE
                WHEN l.transaction_type = 'SELL'
                    THEN (l.entry_price - o.open)
                ELSE (o.open - l.entry_price)
            END * c.lot_size * c.no_of_lots
        ) AS unrealized_pnl
    FROM all_times t
    JOIN all_legs l
      ON l.trade_date = t.date
     AND l.expiry_date = t.date
    JOIN v_nifty_options_filtered o
      ON o.date  = l.trade_date
     AND o.expiry = l.expiry_date
     AND o.option_type = l.option_type
     AND o.strike      = l.strike
     AND o.time        = t.time
    JOIN config c ON TRUE
    WHERE l.entry_time <= t.time
      AND (l.exit_time IS NULL OR t.time < l.exit_time)
    GROUP BY l.trade_date, l.expiry_date, t.time
)

SELECT
    t.date,
    t.expiry,
    t.time,
    ROUND(COALESCE(c.realized_pnl, 0) + COALESCE(o.unrealized_pnl, 0), 2) AS total_pnl,
    ROUND(COALESCE(c.realized_pnl, 0), 2) AS realized_pnl,
    ROUND(COALESCE(o.unrealized_pnl, 0), 2) AS unrealized_pnl
FROM all_times t
LEFT JOIN closed_pnl_at_time c
  ON c.trade_date = t.date
 AND c.expiry_date = t.expiry
 AND c.time = t.time
LEFT JOIN open_mtm_at_time o
  ON o.trade_date = t.date
 AND o.expiry_date = t.expiry
 AND o.time = t.time
ORDER BY date, expiry, time;