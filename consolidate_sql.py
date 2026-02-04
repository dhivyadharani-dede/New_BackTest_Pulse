#!/usr/bin/env python3
"""
Consolidate all SQL files into one master initialization script
"""

import os
from pathlib import Path

# SQL files in execution order (from init-db.sh)
sql_files_order = [
    # 1. Create base tables first
    'create_nifty50.sql',
    'create_nifty_options.sql',
    'create_strategy_settings.sql',
    'create_runtime_strategy_config.sql',
    'create_strategy_run_results.sql',
    'create_strategy_leg_book.sql',

    # 2. Create Heikin-Ashi tables
    'create_heikin_ashi_tables.sql',

    # 3. Create views and materialized views
    'create_v_strategy_config.sql',
    'create_filtered_views.sql',
    'create_mv_ha_candles.sql',
    'create_mv_nifty_options_filtered.sql',

    # 4. Create breakout and trading logic materialized views
    'create_mv_all_5min_breakouts.sql',
    'create_mv_ranked_breakouts_with_rounds.sql',
    'create_mv_ranked_breakouts_with_rounds_for_reentry.sql',
    'create_mv_base_strike_selection.sql',
    'create_mv_breakout_context_round1.sql',

    # 5. Create entry/exit logic views
    'create_mv_entry_and_hedge_legs.sql',
    'create_temp_live_prices_entry_round1.sql',
    'create_mv_entry_sl_hits_round1.sql',
    'create_mv_entry_sl_executions_round1.sql',
    'create_mv_entry_open_legs_round1.sql',
    'create_mv_entry_profit_booking_round1.sql',
    'create_mv_entry_eod_close_round1.sql',
    'create_mv_entry_closed_legs_round1.sql',
    'create_mv_entry_round1_stats.sql',

    # 6. Create hedge logic views
    'create_mv_hedge_exit_on_all_entry_sl.sql',
    'create_mv_hedge_exit_partial_conditions.sql',
    'create_mv_hedge_closed_legs_round1.sql',
    'create_mv_hedge_eod_exit_round1.sql',
    'create_mv_entry_exit_on_partial_hedge_round1.sql',
    'create_mv_double_buy_legs_round1.sql',
    'create_mv_entry_final_exit_round1.sql',

    # 7. Create reentry logic views
    'create_mv_rehedge_trigger_round1.sql',
    'create_mv_rehedge_candidate_round1.sql',
    'create_mv_rehedge_selected_round1.sql',
    'create_mv_rehedge_leg_round1.sql',
    'create_mv_rehedge_eod_exit_round1.sql',
    'create_mv_all_legs_round1.sql',

    # 8. Create reentry round views
    'create_mv_reentry_triggered_breakouts.sql',
    'create_mv_reentry_base_strike_selection.sql',
    'create_mv_reentry_legs_and_hedge_legs.sql',
    'create_temp_reentry_live_prices.sql',
    'create_mv_reentry_breakout_context.sql',
    'create_mv_reentry_sl_hits.sql',
    'create_mv_reentry_sl_executions.sql',
    'create_mv_reentry_open_legs.sql',
    'create_mv_reentry_profit_booking.sql',
    'create_mv_reentry_eod_close.sql',
    'create_mv_reentry_final_exit.sql',
    'create_mv_double_buy_legs_reentry.sql',
    'create_mv_reentry_legs_stats.sql',

    # 9. Create reentry hedge views
    'create_mv_hedge_reentry_exit_on_all_entry_sl.sql',
    'create_mv_hedge_reentry_exit_on_partial_conditions.sql',
    'create_mv_hedge_reentry_closed_legs.sql',
    'create_mv_hedge_reentry_eod_exit.sql',
    'create_mv_reentry_exit_on_partial_hedge.sql',
    'create_mv_rehedge_trigger_reentry.sql',
    'create_mv_rehedge_candidate_reentry.sql',
    'create_mv_rehedge_selected_reentry.sql',
    'create_mv_rehedge_leg_reentry.sql',
    'create_mv_rehedge_eod_exit_reentry.sql',
    'create_mv_all_legs_reentry.sql',

    # 10. Create portfolio and final views
    'create_temp_entry_leg_live_prices.sql',
    'create_mv_all_entries_sl_tracking_adjusted.sql',
    'create_mv_portfolio_mtm_pnl.sql',
    'create_mv_portfolio_final_pnl.sql',

    # 11. Create stored procedures
    'sp_insert_sl_legs_into_book.sql',
    'sp_run_reentry_loop.sql',
    'sp_run_strategy.sql',

    # 12. Create indexes
    'create_indexes_matviews.sql',

    # 13. Set up default data
    'update_strategy_settings_defaults.sql',
    'upsert_runtime_strategy_config_default.sql',
    'set_strategy_settings_parent_values.sql',

    # 14. Create functions
    'get_heikin_ashi.sql',
]

def consolidate_sql_files():
    sql_dir = Path('sql')
    output_file = Path('consolidated_init.sql')

    with open(output_file, 'w', encoding='utf-8') as outfile:
        # Write header
        outfile.write('-- =====================================================\n')
        outfile.write('-- New_BackTest_Pulse Database Initialization Script\n')
        outfile.write('-- Consolidated SQL file for complete database setup\n')
        outfile.write('-- Generated from individual SQL files in dependency order\n')
        outfile.write('-- =====================================================\n\n')

        current_section = ""
        section_counter = 0

        for sql_file in sql_files_order:
            sql_path = sql_dir / sql_file

            if not sql_path.exists():
                print(f"Warning: {sql_file} not found, skipping...")
                continue

            # Determine section based on file prefix
            if sql_file.startswith('create_nifty'):
                section = "1. CREATE BASE TABLES"
            elif sql_file.startswith('create_strategy') or sql_file.startswith('create_runtime'):
                section = "1. CREATE BASE TABLES"
            elif sql_file == 'create_heikin_ashi_tables.sql':
                section = "2. CREATE HEIKIN-ASHI TABLES"
            elif sql_file in ['create_v_strategy_config.sql', 'create_filtered_views.sql', 'create_mv_ha_candles.sql', 'create_mv_nifty_options_filtered.sql']:
                section = "3. CREATE VIEWS AND MATERIALIZED VIEWS"
            elif sql_file in ['create_mv_all_5min_breakouts.sql', 'create_mv_ranked_breakouts_with_rounds.sql', 'create_mv_ranked_breakouts_with_rounds_for_reentry.sql', 'create_mv_base_strike_selection.sql', 'create_mv_breakout_context_round1.sql']:
                section = "4. CREATE BREAKOUT AND TRADING LOGIC VIEWS"
            elif sql_file.startswith('create_mv_entry_') and 'round1' in sql_file:
                section = "5. CREATE ENTRY/EXIT LOGIC VIEWS"
            elif sql_file.startswith('create_mv_hedge_') and 'round1' in sql_file and not 'reentry' in sql_file:
                section = "6. CREATE HEDGE LOGIC VIEWS"
            elif sql_file.startswith('create_mv_rehedge_') and 'round1' in sql_file:
                section = "7. CREATE REENTRY LOGIC VIEWS"
            elif sql_file.startswith('create_mv_reentry_'):
                section = "8. CREATE REENTRY ROUND VIEWS"
            elif sql_file.startswith('create_mv_hedge_reentry_') or (sql_file.startswith('create_mv_rehedge_') and 'reentry' in sql_file):
                section = "9. CREATE REENTRY HEDGE VIEWS"
            elif sql_file in ['create_temp_entry_leg_live_prices.sql', 'create_mv_all_entries_sl_tracking_adjusted.sql', 'create_mv_portfolio_mtm_pnl.sql', 'create_mv_portfolio_final_pnl.sql']:
                section = "10. CREATE PORTFOLIO AND FINAL VIEWS"
            elif sql_file.startswith('sp_'):
                section = "11. CREATE STORED PROCEDURES"
            elif sql_file == 'create_indexes_matviews.sql':
                section = "12. CREATE INDEXES"
            elif sql_file in ['update_strategy_settings_defaults.sql', 'upsert_runtime_strategy_config_default.sql', 'set_strategy_settings_parent_values.sql']:
                section = "13. SET UP DEFAULT DATA"
            elif sql_file == 'get_heikin_ashi.sql':
                section = "14. CREATE FUNCTIONS"
            else:
                section = f"{section_counter}. MISCELLANEOUS"

            # Write section header if changed
            if section != current_section:
                outfile.write('\n-- =====================================================\n')
                outfile.write(f'-- {section}\n')
                outfile.write('-- =====================================================\n\n')
                current_section = section
                section_counter += 1

            # Write file header
            outfile.write(f'-- File: {sql_file}\n')

            # Read and write file content
            try:
                with open(sql_path, 'r', encoding='utf-8') as infile:
                    content = infile.read().strip()
                    if content:
                        outfile.write(content)
                        outfile.write('\n\n')
                    else:
                        outfile.write('-- (empty file)\n\n')
            except Exception as e:
                outfile.write(f'-- Error reading file: {e}\n\n')

        # Write footer
        outfile.write('-- =====================================================\n')
        outfile.write('-- Database initialization completed!\n')
        outfile.write('-- =====================================================\n')

    print(f"Consolidated SQL file created: {output_file}")
    print(f"Total SQL files processed: {len(sql_files_order)}")

if __name__ == '__main__':
    consolidate_sql_files()