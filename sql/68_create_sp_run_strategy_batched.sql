CREATE OR REPLACE PROCEDURE sp_run_strategy_batched(
    p_batch_type TEXT DEFAULT 'quarter'   -- 'month', 'quarter', 'halfyear', 'week', 'day'
)
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_batch_start DATE;
    v_batch_end   DATE;
    v_base_views_refreshed BOOLEAN := FALSE;
BEGIN
    -- Disable JIT for large analytical workloads
    PERFORM set_config('jit', 'off', true);

    FOR rec IN SELECT * FROM strategy_settings LOOP

        IF rec.from_date IS NULL OR rec.to_date IS NULL THEN
            RAISE EXCEPTION
                'Date range not defined for strategy %',
                rec.strategy_name;
        END IF;

        RAISE NOTICE
            'Starting strategy % (% → %)',
            rec.strategy_name, rec.from_date, rec.to_date;

        /* =========================================
           🧹 Cleanup old results for this strategy
           ========================================= */
        -- DELETE FROM strategy_run_results
        -- WHERE strategy_name = rec.strategy_name;

        DELETE FROM strategy_leg_book
        WHERE strategy_name = rec.strategy_name;

        v_batch_start := rec.from_date;
        -- Reset runtime date table for this strategy
DELETE FROM runtime_strategy_dates
WHERE strategy_name = rec.strategy_name;

INSERT INTO runtime_strategy_dates (
    strategy_name,
    from_date,
    to_date
)
VALUES (
    rec.strategy_name,
    rec.from_date,
    rec.to_date
);

IF NOT v_base_views_refreshed THEN
    RAISE NOTICE 'Refreshing base filtered MVs (once per run)';

    COMMIT;  -- must not be inside a transaction

    REFRESH MATERIALIZED VIEW CONCURRENTLY v_ha_big_filtered;
    REFRESH MATERIALIZED VIEW CONCURRENTLY v_ha_small_filtered;
    REFRESH MATERIALIZED VIEW CONCURRENTLY v_ha_1m_filtered;
    REFRESH MATERIALIZED VIEW CONCURRENTLY v_nifty_options_filtered;
    REFRESH MATERIALIZED VIEW CONCURRENTLY  v_nifty50_filtered ;
    

    v_base_views_refreshed := TRUE;

    COMMIT;  -- clean boundary before batch work starts
END IF;

        WHILE v_batch_start <= rec.to_date LOOP

            /* =========================================
               1️⃣ Resolve batch end date
               ========================================= */
            v_batch_end :=
                CASE
                    WHEN p_batch_type = 'day' THEN
                        v_batch_start
                    WHEN p_batch_type = 'week' THEN
                        LEAST(v_batch_start + INTERVAL '6 days', rec.to_date)
                    WHEN p_batch_type = 'month' THEN
                        LEAST(
                            (date_trunc('month', v_batch_start)
                             + INTERVAL '1 month - 1 day')::date,
                            rec.to_date
                        )
                    WHEN p_batch_type = 'quarter' THEN
                        LEAST(
                            (date_trunc('quarter', v_batch_start)
                             + INTERVAL '3 months - 1 day')::date,
                            rec.to_date
                        )
                    WHEN p_batch_type = 'halfyear' THEN
                        LEAST(
                            (date_trunc('year', v_batch_start)
                             + CASE WHEN EXTRACT(MONTH FROM v_batch_start) <= 6 THEN INTERVAL '6 months - 1 day'
                                   ELSE INTERVAL '1 year - 1 day' END)::date,
                            rec.to_date
                        )
                    ELSE
                        LEAST(
                            (date_trunc('month', v_batch_start)
                             + INTERVAL '1 month - 1 day')::date,
                            rec.to_date
                        )
                END;

            RAISE NOTICE
                'Running batch % → %',
                v_batch_start, v_batch_end;

            /* =========================================
               2️⃣ Reset runtime config (PER BATCH)
               ========================================= */
            TRUNCATE runtime_strategy_config;

            INSERT INTO runtime_strategy_config
            SELECT
                rec.strategy_name,
                rec.big_candle_tf,
                rec.small_candle_tf,
                rec.entry_candle,
                rec.preferred_breakout_type,
                rec.reentry_breakout_type,
                rec.breakout_threshold_pct / 100.0,
                rec.sl_type,
                rec.sl_percentage / 100.0,
                rec.box_sl_trigger_pct / 100.0,
                rec.box_sl_hard_pct / 100.0,
                rec.width_sl_pct / 100.0,
                rec.switch_pct / 100.0,
                rec.num_entry_legs,
                rec.num_hedge_legs,
                rec.option_entry_price_cap,
                rec.hedge_entry_price_cap,
                rec.hedge_exit_entry_ratio / 100.0,
                rec.hedge_exit_multiplier,
                rec.leg_profit_pct / 100.0,
                rec.portfolio_profit_target_pct / 100.0,
                rec.portfolio_stop_loss_pct / 100.0,
                rec.portfolio_capital,
                rec.no_of_lots,
                rec.lot_size,
                rec.max_reentry_rounds,
                rec.eod_time,
                v_batch_start,
                v_batch_end
            FROM strategy_settings
            WHERE strategy_name = rec.strategy_name;

            /* =========================================

               4️⃣ Refresh materialized views (optimized for batch size)
               ========================================= */

            -- Always refresh core config
            REFRESH MATERIALIZED VIEW v_strategy_config;

            -- For large batches (>30 days), refresh all filtered views
            -- For small batches, they auto-update as regular views
          
-- COMMIT;

-- IF (v_batch_end - v_batch_start) > 30 THEN
--     REFRESH MATERIALIZED VIEW CONCURRENTLY v_ha_big_filtered;
--     REFRESH MATERIALIZED VIEW CONCURRENTLY v_ha_small_filtered;
--     REFRESH MATERIALIZED VIEW CONCURRENTLY v_ha_1m_filtered;
--     REFRESH MATERIALIZED VIEW CONCURRENTLY v_nifty_options_filtered;
-- END IF;

            -- Core strategy views (always refresh)
            --REFRESH MATERIALIZED VIEW mv_nifty_options_filtered;
            REFRESH MATERIALIZED VIEW mv_all_5min_breakouts;
            REFRESH MATERIALIZED VIEW mv_ranked_breakouts_with_rounds;
            REFRESH MATERIALIZED VIEW mv_ranked_breakouts_with_rounds_for_reentry;
            REFRESH MATERIALIZED VIEW mv_base_strike_selection;
            REFRESH MATERIALIZED VIEW mv_breakout_context_round1;
            REFRESH MATERIALIZED VIEW mv_entry_and_hedge_legs;
        -- REFRESH MATERIALIZED VIEW mv_live_prices_entry_round1;
        TRUNCATE TABLE wrk_live_prices_entry_round1;

INSERT INTO wrk_live_prices_entry_round1
WITH strategy AS (
    SELECT eod_time FROM v_strategy_config LIMIT 1
),
legs AS (
    SELECT *
    FROM mv_entry_and_hedge_legs
    WHERE entry_round = 1
)
SELECT
    l.trade_date,
    l.expiry_date,
    l.breakout_time,
    l.entry_time,
    l.spot_price,
    l.option_type,
    l.strike,
    l.entry_price,
    l.entry_round,
    l.leg_type,
    l.transaction_type,
    o.time  AS ltp_time,
    o.high  AS option_high,
    o.open  AS option_open,
    o.close AS option_close
FROM legs l
JOIN strategy s ON TRUE
JOIN v_nifty_options_filtered o
  ON o.date = l.trade_date
 AND o.expiry = l.expiry_date
 AND o.option_type = l.option_type
 AND o.strike = l.strike
 AND o.time BETWEEN l.entry_time AND s.eod_time;
        REFRESH MATERIALIZED VIEW mv_entry_sl_hits_round1;
        REFRESH MATERIALIZED VIEW mv_entry_sl_executions_round1;
        REFRESH MATERIALIZED VIEW mv_entry_open_legs_round1;
        REFRESH MATERIALIZED VIEW mv_entry_profit_booking_round1;
        REFRESH MATERIALIZED VIEW mv_entry_eod_close_round1;
        REFRESH MATERIALIZED VIEW mv_entry_closed_legs_round1;
        REFRESH MATERIALIZED VIEW mv_entry_round1_stats;
        REFRESH MATERIALIZED VIEW mv_hedge_exit_on_all_entry_sl;
        REFRESH MATERIALIZED VIEW mv_hedge_exit_partial_conditions;
        REFRESH MATERIALIZED VIEW mv_hedge_closed_legs_round1;
        REFRESH MATERIALIZED VIEW mv_hedge_eod_exit_round1;
        REFRESH MATERIALIZED VIEW mv_entry_exit_on_partial_hedge_round1;
        REFRESH MATERIALIZED VIEW mv_double_buy_legs_round1;
        REFRESH MATERIALIZED VIEW mv_entry_final_exit_round1;
        REFRESH MATERIALIZED VIEW mv_rehedge_trigger_round1;
        REFRESH MATERIALIZED VIEW mv_rehedge_candidate_round1;
        REFRESH MATERIALIZED VIEW mv_rehedge_selected_round1;
        REFRESH MATERIALIZED VIEW mv_rehedge_leg_round1;
        REFRESH MATERIALIZED VIEW mv_rehedge_eod_exit_round1;
        REFRESH MATERIALIZED VIEW mv_all_legs_round1;
        CALL insert_sl_legs_into_book(rec.strategy_name);
        REFRESH MATERIALIZED VIEW mv_reentry_triggered_breakouts;
        REFRESH MATERIALIZED VIEW mv_reentry_base_strike_selection;
        REFRESH MATERIALIZED VIEW mv_reentry_legs_and_hedge_legs;
        -- REFRESH MATERIALIZED VIEW mv_reentry_live_prices;
        TRUNCATE TABLE wrk_reentry_live_prices;

INSERT INTO wrk_reentry_live_prices
WITH strategy AS (
    SELECT eod_time FROM v_strategy_config
),
legs AS (
    SELECT * FROM mv_reentry_legs_and_hedge_legs
)
SELECT
    l.trade_date,
    l.expiry_date,
    l.breakout_time,
    l.entry_time,
    l.spot_price,
    l.option_type,
    l.strike,
    l.entry_price,
    l.entry_round,
    l.leg_type,
    l.transaction_type,
    o.time  AS ltp_time,
    o.high  AS option_high,
    o.open  AS option_open,
    o.close AS option_close
FROM legs l
JOIN strategy s ON TRUE
JOIN v_nifty_options_filtered o
  ON o.date = l.trade_date
 AND o.expiry = l.expiry_date
 AND o.option_type = l.option_type
 AND o.strike = l.strike
 AND o.time BETWEEN l.entry_time AND s.eod_time;

        -- Refresh reentry views (grouped to reduce peak lock usage)
        REFRESH MATERIALIZED VIEW mv_reentry_breakout_context;
        REFRESH MATERIALIZED VIEW mv_reentry_sl_hits;
        REFRESH MATERIALIZED VIEW mv_reentry_sl_executions;
        REFRESH MATERIALIZED VIEW mv_reentry_open_legs;
        REFRESH MATERIALIZED VIEW mv_reentry_profit_booking;
        REFRESH MATERIALIZED VIEW mv_reentry_eod_close;
        REFRESH MATERIALIZED VIEW mv_reentry_final_exit;
        REFRESH MATERIALIZED VIEW mv_reentry_legs_stats;
        REFRESH MATERIALIZED VIEW mv_hedge_reentry_exit_on_all_entry_sl;
        REFRESH MATERIALIZED VIEW mv_hedge_reentry_exit_on_partial_conditions;
        REFRESH MATERIALIZED VIEW mv_hedge_reentry_closed_legs;
        REFRESH MATERIALIZED VIEW mv_hedge_reentry_eod_exit;
        REFRESH MATERIALIZED VIEW mv_reentry_exit_on_partial_hedge;
        REFRESH MATERIALIZED VIEW mv_double_buy_legs_reentry;
        REFRESH MATERIALIZED VIEW mv_rehedge_trigger_reentry;
        REFRESH MATERIALIZED VIEW mv_rehedge_candidate_reentry;
        REFRESH MATERIALIZED VIEW mv_rehedge_selected_reentry;
        REFRESH MATERIALIZED VIEW mv_rehedge_leg_reentry;
        REFRESH MATERIALIZED VIEW mv_rehedge_eod_exit_reentry;
        REFRESH MATERIALIZED VIEW mv_all_legs_reentry;
        -- CALL sp_run_reentry_loop(rec.strategy_name);
        PERFORM fn_run_reentry_loop(rec.strategy_name);
        -- REFRESH MATERIALIZED VIEW mv_entry_leg_live_prices;
        TRUNCATE TABLE wrk_entry_leg_live_prices;

INSERT INTO wrk_entry_leg_live_prices
WITH legs AS (
    SELECT * FROM mv_all_legs_reentry
    UNION ALL
    SELECT * FROM mv_all_legs_round1
)
SELECT
    l.trade_date,
    l.expiry_date,
    l.breakout_time,
    l.entry_time,
    l.spot_price,
    l.option_type,
    l.strike,
    l.entry_price,
    CASE
        WHEN l.sl_level ~ '^[0-9.]+$' THEN l.sl_level::numeric
        ELSE NULL
    END AS sl_level,
    l.entry_round,
    l.leg_type,
    l.transaction_type,
    l.exit_time,
    l.exit_reason,
    o.time  AS ltp_time,
    o.high  AS option_high,
    o.open  AS option_open,
    o.close AS option_close,
    n.high  AS nifty_high,
    n.low   AS nifty_low,
    n.time  AS nifty_time
FROM legs l
JOIN v_nifty_options_filtered o
  ON o.date = l.trade_date
 AND o.expiry = l.expiry_date
 AND o.option_type = l.option_type
 AND o.strike = l.strike
 AND o.time > l.entry_time
JOIN v_nifty50_filtered n
  ON n.date = l.trade_date
 AND n.time = o.time;

        REFRESH MATERIALIZED VIEW mv_all_entries_sl_tracking_adjusted;
        REFRESH MATERIALIZED VIEW mv_portfolio_mtm_pnl;
        REFRESH MATERIALIZED VIEW mv_portfolio_final_pnl;

-- Store final results
        INSERT INTO strategy_run_results (
            strategy_name,
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
            total_pnl_per_day
        )
        SELECT
            rec.strategy_name,
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
            total_pnl_per_day
        FROM mv_portfolio_final_pnl
        WHERE trade_date BETWEEN v_batch_start AND v_batch_end;

            -- Memory management: analyze tables after large inserts
            -- IF (SELECT COUNT(*) FROM strategy_leg_book WHERE strategy_name = rec.strategy_name) > 5000 THEN
            --     ANALYZE strategy_leg_book;
            -- END IF;

            RAISE NOTICE
                'Completed batch % → %',
                v_batch_start, v_batch_end;
/* =========================================
               🔑 RELEASE LOCKS FOR THIS BATCH
               ========================================= */
            COMMIT;  -- Ensure all locks are released before next batch starts
            v_batch_start := v_batch_end + INTERVAL '1 day';
        

        /* =========================================
           🔄 STORE ALL RESULTS for this strategy
           ========================================= */
        RAISE NOTICE 'Storing final results for strategy %', rec.strategy_name;

        
END LOOP;
        RAISE NOTICE
            'Completed strategy %',
            rec.strategy_name;

    END LOOP;

    RAISE NOTICE 'All strategies completed (batched run).';
END;
$$;