CREATE MATERIALIZED VIEW mv_portfolio_final_pnl AS
WITH config AS (
    SELECT 
        portfolio_capital,
        ROUND(portfolio_profit_target_pct / 100, 3) AS portfolio_profit_target_pct,
        ROUND(portfolio_stop_loss_pct  / 100, 3) AS portfolio_stop_loss_pct,
        no_of_lots,
        lot_size,
        eod_time
    FROM v_strategy_config
),

portfolio_mtm_pnl AS (
    SELECT * FROM mv_portfolio_mtm_pnl where date='2025-04-01'
),

/* ============================================================
   1. First portfolio-level exit trigger (profit / loss)
   ============================================================ */
portfolio_exit_trigger AS (
    SELECT DISTINCT ON (date, expiry)
        date,
        expiry,
        time AS exit_time,
        total_pnl,
        CASE 
            WHEN total_pnl >= portfolio_capital * portfolio_profit_target_pct
                THEN 'Portfolio Exit - Profit'
            WHEN total_pnl <= -portfolio_capital * portfolio_stop_loss_pct
                THEN 'Portfolio Exit - Loss'
        END AS exit_reason
    FROM portfolio_mtm_pnl
    CROSS JOIN config
    WHERE total_pnl >= portfolio_capital * portfolio_profit_target_pct
       OR total_pnl <= -portfolio_capital * portfolio_stop_loss_pct
    ORDER BY date, expiry, time
),

/* ============================================================
   2. Legs open at portfolio exit time
   ============================================================ */
open_legs_at_exit AS (
    SELECT l.*
    FROM mv_all_entries_sl_tracking_adjusted l
    JOIN portfolio_exit_trigger p
      ON p.date  = l.trade_date
     AND p.expiry = l.expiry_date
    WHERE l.entry_time <= p.exit_time
      AND (l.exit_time IS NULL OR p.exit_time <= l.exit_time)
),

/* ============================================================
   3. Price legs at portfolio exit time
   ============================================================ */
exit_priced_legs AS (
    SELECT 
        l.trade_date,
        l.expiry_date,
        l.breakout_time,
        l.entry_time,
        l.spot_price,
        l.option_type,
        l.strike,
        l.entry_price,
        l.sl_level,
        l.entry_round,
        l.leg_type,
        l.transaction_type,
        p.exit_time,
        o.open AS exit_price,
        p.exit_reason
    FROM open_legs_at_exit l
    JOIN portfolio_exit_trigger p
      ON p.date  = l.trade_date
     AND p.expiry = l.expiry_date
    JOIN v_nifty_options_filtered o
      ON o.date  = l.trade_date
     AND o.expiry = l.expiry_date
     AND o.option_type = l.option_type
     AND o.strike      = l.strike
     AND o.time        = p.exit_time
),

/* ============================================================
   4. Portfolio exit PnL
   ============================================================ */
exit_legs_with_pnl AS (
    SELECT 
        e.*,
        CASE 
            WHEN e.transaction_type = 'SELL'
                THEN ROUND((e.entry_price - e.exit_price) * c.lot_size * c.no_of_lots, 2)
            ELSE ROUND((e.exit_price - e.entry_price) * c.lot_size * c.no_of_lots, 2)
        END AS pnl_amount
    FROM exit_priced_legs e
    CROSS JOIN config c
),

/* ============================================================
   5. Remove invalid legs (entry after exit)
   ============================================================ */
invalid_legs AS (
    SELECT *
    FROM mv_all_entries_sl_tracking_adjusted l
    JOIN portfolio_exit_trigger p
      ON p.date  = l.trade_date
     AND p.expiry = l.expiry_date
    WHERE l.entry_time > p.exit_time
),

valid_legs AS (
    SELECT *
    FROM mv_all_entries_sl_tracking_adjusted l
    WHERE NOT EXISTS (
        SELECT 1
        FROM invalid_legs i
        WHERE i.trade_date  = l.trade_date
          AND i.expiry_date = l.expiry_date
          AND i.option_type = l.option_type
          AND i.strike      = l.strike
          AND i.entry_round = l.entry_round
          AND i.leg_type    = l.leg_type
    )
),

/* ============================================================
   6. Combine normal exits + portfolio exits
   ============================================================ */
all_leg_exits AS (
    SELECT * FROM valid_legs
    UNION ALL
    SELECT * FROM exit_legs_with_pnl
),

/* ============================================================
   7. Hedge exit when all RE-ENTRY legs are done
   ============================================================ */
reentry_exit_summary AS (
    SELECT
        trade_date,
        expiry_date,
        MAX(exit_time) AS max_exit_time,
        COUNT(*) FILTER (WHERE exit_time IS NOT NULL) AS exited_count,
        COUNT(*) AS total_reentry_legs
    FROM all_leg_exits
    WHERE leg_type = 'RE-ENTRY'
    GROUP BY trade_date, expiry_date
),

hedge_exit_on_reentry_completion AS (
    SELECT
        h.trade_date,
        h.expiry_date,
        h.breakout_time,
        h.entry_time,
        h.spot_price,
        h.option_type,
        h.strike,
        h.entry_price,
        '0' AS sl_level,
        h.entry_round,
        h.leg_type,
        h.transaction_type,
        r.max_exit_time AS exit_time,
        o.open AS exit_price,
        'EXIT - ALL REENTRY COMPLETE' AS exit_reason,
        CASE 
            WHEN h.transaction_type = 'SELL'
                THEN ROUND((h.entry_price - o.open) * c.lot_size * c.no_of_lots, 2)
            ELSE ROUND((o.open - h.entry_price) * c.lot_size * c.no_of_lots, 2)
        END AS pnl_amount
    FROM all_leg_exits h
    JOIN reentry_exit_summary r
      ON r.trade_date  = h.trade_date
     AND r.expiry_date = h.expiry_date
    JOIN v_nifty_options_filtered o
      ON o.date  = h.trade_date
     AND o.expiry = h.expiry_date
     AND o.option_type = h.option_type
     AND o.strike      = h.strike
     AND o.time        = r.max_exit_time
    CROSS JOIN config c
    WHERE h.leg_type = 'HEDGE-REENTRY'
      AND r.exited_count = r.total_reentry_legs
      AND r.max_exit_time <> c.eod_time
),

/* ============================================================
   8. Final de-duplication
   ============================================================ */
final_legs AS (
    SELECT * FROM valid_legs
    UNION ALL
    SELECT * FROM exit_legs_with_pnl
    UNION ALL
    SELECT * FROM hedge_exit_on_reentry_completion
),

ranked_legs AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY
                   trade_date,
                   expiry_date,
                   option_type,
                   strike,
                   leg_type,
                   entry_round
               ORDER BY exit_time
           ) AS rn
    FROM final_legs
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
    sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount,
    ROUND(
        SUM(pnl_amount) OVER (PARTITION BY trade_date, expiry_date),
        2
    ) AS total_pnl_per_day
FROM ranked_legs
WHERE rn = 1
ORDER BY trade_date, expiry_date, entry_time, option_type, strike;