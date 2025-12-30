#!/bin/bash
set -e

echo "Initializing Backtest_Pulse database..."

# Wait for PostgreSQL to be ready
until psql -U postgres -d Backtest_Pulse -c "SELECT 1;" > /dev/null 2>&1; do
  echo "Waiting for PostgreSQL to be ready..."
  sleep 2
done

echo "PostgreSQL is ready. Running initialization scripts..."

# Run SQL files in dependency order
cd /app/sql

# 1. Create base tables first
echo "Creating base tables..."
psql -U postgres -d Backtest_Pulse -f create_nifty50.sql
psql -U postgres -d Backtest_Pulse -f create_nifty_options.sql
psql -U postgres -d Backtest_Pulse -f create_strategy_settings.sql
psql -U postgres -d Backtest_Pulse -f create_runtime_strategy_config.sql
psql -U postgres -d Backtest_Pulse -f create_strategy_run_results.sql
psql -U postgres -d Backtest_Pulse -f create_strategy_leg_book.sql

# 2. Create Heikin-Ashi tables
echo "Creating Heikin-Ashi tables..."
psql -U postgres -d Backtest_Pulse -f create_heikin_ashi_tables.sql

# 3. Create views and materialized views
echo "Creating views and materialized views..."
psql -U postgres -d Backtest_Pulse -f create_v_strategy_config.sql
psql -U postgres -d Backtest_Pulse -f create_filtered_views.sql
psql -U postgres -d Backtest_Pulse -f create_mv_ha_candles.sql
psql -U postgres -d Backtest_Pulse -f create_mv_nifty_options_filtered.sql

# 4. Create breakout and trading logic materialized views
echo "Creating breakout and trading logic views..."
psql -U postgres -d Backtest_Pulse -f create_mv_all_5min_breakouts.sql
psql -U postgres -d Backtest_Pulse -f create_mv_ranked_breakouts_with_rounds.sql
psql -U postgres -d Backtest_Pulse -f create_mv_ranked_breakouts_with_rounds_for_reentry.sql
psql -U postgres -d Backtest_Pulse -f create_mv_base_strike_selection.sql
psql -U postgres -d Backtest_Pulse -f create_mv_breakout_context_round1.sql

# 5. Create entry/exit logic views
echo "Creating entry/exit logic views..."
psql -U postgres -d Backtest_Pulse -f create_mv_entry_and_hedge_legs.sql
psql -U postgres -d Backtest_Pulse -f create_mv_live_prices_entry_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_entry_sl_hits_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_entry_sl_executions_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_entry_open_legs_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_entry_profit_booking_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_entry_eod_close_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_entry_closed_legs_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_entry_round1_stats.sql

# 6. Create hedge logic views
echo "Creating hedge logic views..."
psql -U postgres -d Backtest_Pulse -f create_mv_hedge_exit_on_all_entry_sl.sql
psql -U postgres -d Backtest_Pulse -f create_mv_hedge_exit_partial_conditions.sql
psql -U postgres -d Backtest_Pulse -f create_mv_hedge_closed_legs_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_hedge_eod_exit_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_entry_exit_on_partial_hedge_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_double_buy_legs_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_entry_final_exit_round1.sql

# 7. Create reentry logic views
echo "Creating reentry logic views..."
psql -U postgres -d Backtest_Pulse -f create_mv_rehedge_trigger_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_rehedge_candidate_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_rehedge_selected_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_rehedge_leg_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_rehedge_eod_exit_round1.sql
psql -U postgres -d Backtest_Pulse -f create_mv_all_legs_round1.sql

# 8. Create reentry round views
echo "Creating reentry round views..."
psql -U postgres -d Backtest_Pulse -f create_mv_reentry_triggered_breakouts.sql
psql -U postgres -d Backtest_Pulse -f create_mv_reentry_base_strike_selection.sql
psql -U postgres -d Backtest_Pulse -f create_mv_reentry_legs_and_hedge_legs.sql
psql -U postgres -d Backtest_Pulse -f create_mv_reentry_live_prices.sql
psql -U postgres -d Backtest_Pulse -f create_mv_reentry_breakout_context.sql
psql -U postgres -d Backtest_Pulse -f create_mv_reentry_sl_hits.sql
psql -U postgres -d Backtest_Pulse -f create_mv_reentry_sl_executions.sql
psql -U postgres -d Backtest_Pulse -f create_mv_reentry_open_legs.sql
psql -U postgres -d Backtest_Pulse -f create_mv_reentry_profit_booking.sql
psql -U postgres -d Backtest_Pulse -f create_mv_reentry_eod_close.sql
psql -U postgres -d Backtest_Pulse -f create_mv_reentry_final_exit.sql
psql -U postgres -d Backtest_Pulse -f create_mv_double_buy_legs_reentry.sql
psql -U postgres -d Backtest_Pulse -f create_mv_reentry_legs_stats.sql

# 9. Create reentry hedge views
echo "Creating reentry hedge views..."
psql -U postgres -d Backtest_Pulse -f create_mv_hedge_reentry_exit_on_all_entry_sl.sql
psql -U postgres -d Backtest_Pulse -f create_mv_hedge_reentry_exit_on_partial_conditions.sql
psql -U postgres -d Backtest_Pulse -f create_mv_hedge_reentry_closed_legs.sql
psql -U postgres -d Backtest_Pulse -f create_mv_hedge_reentry_eod_exit.sql
psql -U postgres -d Backtest_Pulse -f create_mv_reentry_exit_on_partial_hedge.sql
psql -U postgres -d Backtest_Pulse -f create_mv_rehedge_trigger_reentry.sql
psql -U postgres -d Backtest_Pulse -f create_mv_rehedge_candidate_reentry.sql
psql -U postgres -d Backtest_Pulse -f create_mv_rehedge_selected_reentry.sql
psql -U postgres -d Backtest_Pulse -f create_mv_rehedge_leg_reentry.sql
psql -U postgres -d Backtest_Pulse -f create_mv_rehedge_eod_exit_reentry.sql
psql -U postgres -d Backtest_Pulse -f create_mv_all_legs_reentry.sql

# 10. Create portfolio and final views
echo "Creating portfolio and final views..."
psql -U postgres -d Backtest_Pulse -f create_mv_entry_leg_live_prices.sql
psql -U postgres -d Backtest_Pulse -f create_mv_all_entries_sl_tracking_adjusted.sql
psql -U postgres -d Backtest_Pulse -f create_mv_portfolio_mtm_pnl.sql
psql -U postgres -d Backtest_Pulse -f create_mv_portfolio_final_pnl.sql

# 11. Create stored procedures
echo "Creating stored procedures..."
psql -U postgres -d Backtest_Pulse -f sp_insert_sl_legs_into_book.sql
psql -U postgres -d Backtest_Pulse -f sp_run_reentry_loop.sql
psql -U postgres -d Backtest_Pulse -f sp_run_strategy.sql

# 12. Create indexes
echo "Creating indexes..."
psql -U postgres -d Backtest_Pulse -f create_indexes_matviews.sql

# 13. Set up default data
echo "Setting up default data..."
psql -U postgres -d Backtest_Pulse -f update_strategy_settings_defaults.sql
psql -U postgres -d Backtest_Pulse -f upsert_runtime_strategy_config_default.sql
psql -U postgres -d Backtest_Pulse -f set_strategy_settings_parent_values.sql

# 14. Create functions
echo "Creating functions..."
psql -U postgres -d Backtest_Pulse -f get_heikin_ashi.sql

echo "Database initialization completed successfully!"