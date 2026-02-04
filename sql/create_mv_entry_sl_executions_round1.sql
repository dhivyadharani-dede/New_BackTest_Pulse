-- Materialized view: entry SL executions (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_sl_executions_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_sl_executions_round1 AS
WITH strategy AS (
    SELECT
        *
    FROM v_strategy_config
),

/* =====================================================
   ENTRY LIVE PRICES
   ===================================================== */
entry_live_prices AS (
    SELECT *
    FROM mv_live_prices_entry_round1
    WHERE leg_type = 'ENTRY'
),

/* =====================================================
   SL HITS (ENTRY ONLY)
   ===================================================== */
sl_hits AS (
    SELECT *
    FROM mv_entry_sl_hits_round1
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
        'ENTRY'::TEXT AS leg_type,
        lp.transaction_type,
        lp.ltp_time AS exit_time,
        CASE
            WHEN sh.exit_reason = 'SL_HIT_REGULAR_SL'
                THEN ROUND(lp.entry_price * (1 + s.sl_percentage), 2)

            WHEN sh.exit_reason = 'SL_HIT_BOX_HARD_SL'
                THEN ROUND(lp.entry_price * (1 + s.box_sl_hard_pct), 2)

            WHEN sh.exit_reason = 'SL_HIT_BOX_TRIGGER_SL'
                THEN ROUND(lp.entry_price * (1 + s.box_sl_trigger_pct), 2)

            ELSE lp.option_high
        END AS exit_price,
        sh.exit_reason,
        ROUND(
            (
                lp.entry_price
                -
                CASE
                    WHEN sh.exit_reason = 'SL_HIT_REGULAR_SL'
                        THEN ROUND(lp.entry_price * (1 + s.sl_percentage), 2)

                    WHEN sh.exit_reason = 'SL_HIT_BOX_HARD_SL'
                        THEN ROUND(lp.entry_price * (1 + s.box_sl_hard_pct), 2)

                    WHEN sh.exit_reason = 'SL_HIT_BOX_TRIGGER_SL'
                        THEN ROUND(lp.entry_price * (1 + s.box_sl_trigger_pct), 2)

                    ELSE lp.option_close
                END
            )
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
