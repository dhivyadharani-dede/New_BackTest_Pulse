\set ON_ERROR_STOP on

-- Run this from repository root:
--   psql -h <host> -U <user> -d <db> -f sql/70_run_full_strategy_no_python.sql
-- This script mirrors scripts/refresh_matviews_sequential.py order, and then
-- allows you to run the complete batched strategy directly in Postgres.

CREATE OR REPLACE FUNCTION public.refresh_mv_if_exists(p_mv_name text)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_matviews
        WHERE schemaname = 'public'
          AND matviewname = p_mv_name
    ) THEN
        EXECUTE format('REFRESH MATERIALIZED VIEW public.%I', p_mv_name);
        RAISE NOTICE 'Refreshed materialized view: public.%', p_mv_name;
    ELSE
        RAISE NOTICE 'Skipped refresh (not a matview): public.%', p_mv_name;
    END IF;
END;
$$;

\echo ===============================
\echo Applying SQL files in sequence
\echo ===============================

\i sql/1_create_v_strategy_config.sql
SELECT public.refresh_mv_if_exists('v_strategy_config');

\i sql/2_create_filtered_views.sql
SELECT public.refresh_mv_if_exists('v_ha_big_filtered');
SELECT public.refresh_mv_if_exists('v_ha_small_filtered');
SELECT public.refresh_mv_if_exists('v_ha_1m_filtered');
SELECT public.refresh_mv_if_exists('v_nifty50_filtered');
SELECT public.refresh_mv_if_exists('v_nifty_options_filtered');

\i sql/3_create_mv_nifty_options_filtered.sql
SELECT public.refresh_mv_if_exists('mv_nifty_options_filtered');

\i sql/4_create_mv_all_5min_breakouts.sql
SELECT public.refresh_mv_if_exists('mv_all_5min_breakouts');

\i sql/5_create_mv_ranked_breakouts_with_rounds.sql
SELECT public.refresh_mv_if_exists('mv_ranked_breakouts_with_rounds');

\i sql/6_create_mv_ranked_breakouts_with_rounds_for_reentry.sql
SELECT public.refresh_mv_if_exists('mv_ranked_breakouts_with_rounds_for_reentry');

\i sql/7_create_mv_base_strike_selection.sql
SELECT public.refresh_mv_if_exists('mv_base_strike_selection');

\i sql/8_create_mv_breakout_context_round1.sql
SELECT public.refresh_mv_if_exists('mv_breakout_context_round1');

\i sql/9_create_mv_entry_and_hedge_legs.sql
SELECT public.refresh_mv_if_exists('mv_entry_and_hedge_legs');

\i sql/63_create_wrk_live_prices_entry_round1.sql

\i sql/10_create_mv_live_prices_entry_round1.sql
SELECT public.refresh_mv_if_exists('mv_live_prices_entry_round1');

\i sql/11_create_mv_entry_sl_hits_round1.sql
SELECT public.refresh_mv_if_exists('mv_entry_sl_hits_round1');

\i sql/12_create_mv_entry_sl_executions_round1.sql
SELECT public.refresh_mv_if_exists('mv_entry_sl_executions_round1');

\i sql/13_create_mv_entry_open_legs_round1.sql
SELECT public.refresh_mv_if_exists('mv_entry_open_legs_round1');

\i sql/14_create_mv_entry_profit_booking_round1.sql
SELECT public.refresh_mv_if_exists('mv_entry_profit_booking_round1');

\i sql/15_create_mv_entry_eod_close_round1.sql
SELECT public.refresh_mv_if_exists('mv_entry_eod_close_round1');

\i sql/16_create_mv_entry_closed_legs_round1.sql
SELECT public.refresh_mv_if_exists('mv_entry_closed_legs_round1');

\i sql/17_create_mv_entry_round1_stats.sql
SELECT public.refresh_mv_if_exists('mv_entry_round1_stats');

\i sql/18_create_mv_hedge_exit_on_all_entry_sl.sql
SELECT public.refresh_mv_if_exists('mv_hedge_exit_on_all_entry_sl');

\i sql/19_create_mv_hedge_exit_partial_conditions.sql
SELECT public.refresh_mv_if_exists('mv_hedge_exit_partial_conditions');

\i sql/20_create_mv_hedge_closed_legs_round1.sql
SELECT public.refresh_mv_if_exists('mv_hedge_closed_legs_round1');

\i sql/21_create_mv_hedge_eod_exit_round1.sql
SELECT public.refresh_mv_if_exists('mv_hedge_eod_exit_round1');

\i sql/22_create_mv_entry_exit_on_partial_hedge_round1.sql
SELECT public.refresh_mv_if_exists('mv_entry_exit_on_partial_hedge_round1');

\i sql/23_create_mv_double_buy_legs_round1.sql
SELECT public.refresh_mv_if_exists('mv_double_buy_legs_round1');

\i sql/24_create_mv_entry_final_exit_round1.sql
SELECT public.refresh_mv_if_exists('mv_entry_final_exit_round1');

\i sql/25_create_mv_rehedge_trigger_round1.sql
SELECT public.refresh_mv_if_exists('mv_rehedge_trigger_round1');

\i sql/26_create_mv_rehedge_candidate_round1.sql
SELECT public.refresh_mv_if_exists('mv_rehedge_candidate_round1');

\i sql/27_create_mv_rehedge_selected_round1.sql
SELECT public.refresh_mv_if_exists('mv_rehedge_selected_round1');

\i sql/28_create_mv_rehedge_leg_round1.sql
SELECT public.refresh_mv_if_exists('mv_rehedge_leg_round1');

\i sql/29_create_mv_rehedge_eod_exit_round1.sql
SELECT public.refresh_mv_if_exists('mv_rehedge_eod_exit_round1');

\i sql/30_create_mv_all_legs_round1.sql
SELECT public.refresh_mv_if_exists('mv_all_legs_round1');

\i sql/31_sp_insert_sl_legs_into_book.sql

\i sql/32_create_mv_reentry_triggered_breakouts.sql
SELECT public.refresh_mv_if_exists('mv_reentry_triggered_breakouts');

\i sql/33_create_mv_reentry_base_strike_selection.sql
SELECT public.refresh_mv_if_exists('mv_reentry_base_strike_selection');

\i sql/34_create_mv_reentry_legs_and_hedge_legs.sql
SELECT public.refresh_mv_if_exists('mv_reentry_legs_and_hedge_legs');

\i sql/64_create_wrk_reentry_live_prices.sql

\i sql/35_create_mv_reentry_live_prices.sql
SELECT public.refresh_mv_if_exists('mv_reentry_live_prices');

\i sql/36_create_mv_reentry_breakout_context.sql
SELECT public.refresh_mv_if_exists('mv_reentry_breakout_context');

\i sql/37_create_mv_reentry_sl_hits.sql
SELECT public.refresh_mv_if_exists('mv_reentry_sl_hits');

\i sql/38_create_mv_reentry_sl_executions.sql
SELECT public.refresh_mv_if_exists('mv_reentry_sl_executions');

\i sql/39_create_mv_reentry_open_legs.sql
SELECT public.refresh_mv_if_exists('mv_reentry_open_legs');

\i sql/40_create_mv_reentry_profit_booking.sql
SELECT public.refresh_mv_if_exists('mv_reentry_profit_booking');

\i sql/41_create_mv_reentry_eod_close.sql
SELECT public.refresh_mv_if_exists('mv_reentry_eod_close');

\i sql/42_create_mv_reentry_final_exit.sql
SELECT public.refresh_mv_if_exists('mv_reentry_final_exit');

\i sql/43_create_mv_reentry_legs_stats.sql
SELECT public.refresh_mv_if_exists('mv_reentry_legs_stats');

\i sql/44_create_mv_hedge_reentry_exit_on_all_entry_sl.sql
SELECT public.refresh_mv_if_exists('mv_hedge_reentry_exit_on_all_entry_sl');

\i sql/45_create_mv_hedge_reentry_exit_on_partial_conditions.sql
SELECT public.refresh_mv_if_exists('mv_hedge_reentry_exit_on_partial_conditions');

\i sql/46_create_mv_hedge_reentry_closed_legs.sql
SELECT public.refresh_mv_if_exists('mv_hedge_reentry_closed_legs');

\i sql/47_create_mv_hedge_reentry_eod_exit.sql
SELECT public.refresh_mv_if_exists('mv_hedge_reentry_eod_exit');

\i sql/48_create_mv_reentry_exit_on_partial_hedge.sql
SELECT public.refresh_mv_if_exists('mv_reentry_exit_on_partial_hedge');

\i sql/49_create_mv_double_buy_legs_reentry.sql
SELECT public.refresh_mv_if_exists('mv_double_buy_legs_reentry');

\i sql/50_create_mv_rehedge_trigger_reentry.sql
SELECT public.refresh_mv_if_exists('mv_rehedge_trigger_reentry');

\i sql/51_create_mv_rehedge_candidate_reentry.sql
SELECT public.refresh_mv_if_exists('mv_rehedge_candidate_reentry');

\i sql/52_create_mv_rehedge_selected_reentry.sql
SELECT public.refresh_mv_if_exists('mv_rehedge_selected_reentry');

\i sql/53_create_mv_rehedge_leg_reentry.sql
SELECT public.refresh_mv_if_exists('mv_rehedge_leg_reentry');

\i sql/54_create_mv_rehedge_eod_exit_reentry.sql
SELECT public.refresh_mv_if_exists('mv_rehedge_eod_exit_reentry');

\i sql/55_create_mv_all_legs_reentry.sql
SELECT public.refresh_mv_if_exists('mv_all_legs_reentry');

\i sql/56_fn_run_reentry_loop.sql
\i sql/66_run_reentry_loop.sql

\i sql/65_create_wrk_entry_leg_live_prices.sql

\i sql/57_create_mv_entry_leg_live_prices.sql
SELECT public.refresh_mv_if_exists('mv_entry_leg_live_prices');

\i sql/58_create_mv_all_entries_sl_tracking_adjusted.sql
SELECT public.refresh_mv_if_exists('mv_all_entries_sl_tracking_adjusted');

\i sql/59_create_mv_portfolio_mtm_pnl.sql
SELECT public.refresh_mv_if_exists('mv_portfolio_mtm_pnl');

\i sql/60_create_mv_portfolio_final_pnl.sql
SELECT public.refresh_mv_if_exists('mv_portfolio_final_pnl');

\i sql/61_create_strategy_run_results.sql
\i sql/68_create_sp_run_strategy_batched.sql

\echo =====================================
\echo Full SQL pipeline setup is complete.
\echo =====================================

-- Run the complete strategy directly in Postgres (no Python required):
-- CALL public.sp_run_strategy_batched('quarter');

-- Optional cleanup helper after setup:
-- DROP FUNCTION IF EXISTS public.refresh_mv_if_exists(text);
