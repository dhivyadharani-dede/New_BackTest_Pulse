CREATE OR REPLACE PROCEDURE sp_run_strategy()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_from_date DATE;
    v_to_date DATE;
BEGIN
    FOR rec IN SELECT * FROM strategy_settings LOOP
        -- Truncate runtime config for each strategy
        TRUNCATE TABLE runtime_strategy_config;

        -- Resolve date range (using strategy's dates, no overrides)
        v_from_date := rec.from_date;
        v_to_date := rec.to_date;

        IF v_from_date IS NULL OR v_to_date IS NULL THEN
            RAISE EXCEPTION 'Date range not defined for strategy %', rec.strategy_name;
        END IF;

        IF v_from_date > v_to_date THEN
            RAISE EXCEPTION 'from_date (%) cannot be after to_date (%)', v_from_date, v_to_date;
        END IF;

        -- Insert into runtime_strategy_config
        INSERT INTO runtime_strategy_config (
            strategy_name,
            big_candle_tf,
            small_candle_tf,
            entry_candle,
            preferred_breakout_type,
            reentry_breakout_type,
            breakout_threshold_pct,
            sl_type,
            sl_percentage,
            box_sl_trigger_pct,
            box_sl_hard_pct,
            width_sl_pct,
            switch_pct,
            num_entry_legs,
            num_hedge_legs,
            option_entry_price_cap,
            hedge_entry_price_cap,
            hedge_exit_entry_ratio,
            hedge_exit_multiplier,
            leg_profit_pct,
            portfolio_profit_target_pct,
            portfolio_stop_loss_pct,
            portfolio_capital,
            no_of_lots,
            lot_size,
            max_reentry_rounds,
            eod_time,
            from_date,
            to_date
        )
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
            v_from_date,
            v_to_date
        FROM strategy_settings
        WHERE strategy_name = rec.strategy_name;

        -- CRITICAL: Refresh v_strategy_config before dependent views
        REFRESH MATERIALIZED VIEW v_strategy_config;

        -- Refresh filtered materialized views (now that they are materialized)
        REFRESH MATERIALIZED VIEW v_ha_big_filtered;
        REFRESH MATERIALIZED VIEW v_ha_small_filtered;
        REFRESH MATERIALIZED VIEW v_ha_1m_filtered;
        REFRESH MATERIALIZED VIEW v_nifty50_filtered;
        REFRESH MATERIALIZED VIEW v_nifty_options_filtered;

        -- Refresh all relevant materialized views
        REFRESH MATERIALIZED VIEW mv_ha_big_candle;
        REFRESH MATERIALIZED VIEW mv_ha_small_candle;
        REFRESH MATERIALIZED VIEW mv_ha_1m_candle;
        -- NOTE: v_*_filtered views are regular views, not materialized - they auto-update
        REFRESH MATERIALIZED VIEW mv_nifty_options_filtered;
        REFRESH MATERIALIZED VIEW mv_all_5min_breakouts;
        REFRESH MATERIALIZED VIEW mv_ranked_breakouts_with_rounds;
        REFRESH MATERIALIZED VIEW mv_ranked_breakouts_with_rounds_for_reentry;
        REFRESH MATERIALIZED VIEW mv_base_strike_selection;
        REFRESH MATERIALIZED VIEW mv_breakout_context_round1;
        REFRESH MATERIALIZED VIEW mv_entry_and_hedge_legs;
        REFRESH MATERIALIZED VIEW mv_live_prices_entry_round1;
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
        REFRESH MATERIALIZED VIEW mv_reentry_live_prices;
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
        CALL sp_run_reentry_loop(rec.strategy_name);
        REFRESH MATERIALIZED VIEW mv_entry_leg_live_prices;
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
        FROM mv_portfolio_final_pnl;

        RAISE NOTICE 'Completed run for strategy %', rec.strategy_name;
    END LOOP;

    RAISE NOTICE 'All strategies processed.';
END;
$$;