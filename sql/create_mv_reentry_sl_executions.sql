CREATE MATERIALIZED VIEW mv_reentry_sl_executions AS
WITH strategy AS (
    SELECT
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   ENTRY LIVE PRICES
   ===================================================== */
entry_live_prices AS (
    SELECT *
    FROM mv_reentry_live_prices
    WHERE leg_type = 'RE-ENTRY'
),

/* =====================================================
   SL HITS (ENTRY ONLY)
   ===================================================== */
sl_hits AS (
    SELECT *
    FROM mv_reentry_sl_hits
),

/* =====================================================
   FINAL SL EXECUTION (PRICE + PNL)
   ===================================================== */
sl_executed AS (
    SELECT
        lp.trade_date,
        lp.expiry_date,
        lp.breakout_time,
        lp.entry_time,
        lp.spot_price,
        lp.option_type,
        lp.strike,
        lp.entry_price,
        lp.entry_round,
        'RE-ENTRY'::TEXT AS leg_type,
        lp.transaction_type,
        lp.ltp_time AS exit_time,
        lp.option_close AS exit_price,
        sh.exit_reason,
        ROUND(
            (lp.entry_price - lp.option_close)
            * s.lot_size
            * s.no_of_lots,
            2
        ) AS pnl_amount
    FROM sl_hits sh
    JOIN entry_live_prices lp
      ON lp.trade_date  = sh.trade_date
     AND lp.expiry_date = sh.expiry_date
     AND lp.option_type = sh.option_type
     AND lp.strike      = sh.strike
     AND lp.entry_round = sh.entry_round
     AND lp.ltp_time    = sh.exit_time
    JOIN strategy s ON TRUE
)

SELECT *
FROM sl_executed
ORDER BY trade_date, expiry_date, exit_time, strike;