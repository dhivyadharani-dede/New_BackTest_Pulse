#!/usr/bin/env python3
"""
Consolidate all SQL files referenced in refresh_matviews_sequential.py into one file
"""

import os
from pathlib import Path

# SQL files in the order from refresh_matviews_sequential.py
sql_files_order = [
    'create_v_strategy_config.sql',
    # base tables / filtered views
    'create_heikin_ashi_tables.sql',
    'create_mv_ha_candles.sql',
    'create_mv_nifty_options_filtered.sql',
    'create_mv_all_5min_breakouts.sql',

    # ranking and reentry helpers (must run before strike selection)
    'create_mv_ranked_breakouts_with_rounds.sql',
    'create_mv_ranked_breakouts_with_rounds_for_reentry.sql',

    # strike selection / breakout context
    'create_mv_base_strike_selection.sql',
    'create_mv_breakout_context_round1.sql',

    # live prices and legs
    'create_mv_entry_and_hedge_legs.sql',
    'create_mv_live_prices_entry_round1.sql',

    # entry SL detection & executions
    'create_mv_entry_sl_hits_round1.sql',
    'create_mv_entry_sl_executions_round1.sql',

    # entry lifecycle
    'create_mv_entry_open_legs_round1.sql',
    'create_mv_entry_profit_booking_round1.sql',
    'create_mv_entry_eod_close_round1.sql',
    'create_mv_entry_closed_legs_round1.sql',
    'create_mv_entry_round1_stats.sql',

    # hedge exit logic
    'create_mv_hedge_exit_on_all_entry_sl.sql',
    'create_mv_hedge_exit_partial_conditions.sql',
    'create_mv_hedge_closed_legs_round1.sql',
    'create_mv_hedge_eod_exit_round1.sql',

    # entry-exit interactions
    'create_mv_entry_exit_on_partial_hedge_round1.sql',
    'create_mv_double_buy_legs_round1.sql',
    'create_mv_entry_final_exit_round1.sql',

    # rehedge pipeline
    'create_mv_rehedge_trigger_round1.sql',
    'create_mv_rehedge_candidate_round1.sql',
    'create_mv_rehedge_selected_round1.sql',
    'create_mv_rehedge_leg_round1.sql',
    'create_mv_rehedge_eod_exit_round1.sql',
    #consolidation
    'create_mv_all_legs_round1.sql',
    'sp_insert_sl_legs_into_book.sql',
    'create_mv_reentry_triggered_breakouts.sql',
    # reentry pipeline
    'create_mv_reentry_base_strike_selection.sql',
    'create_mv_reentry_legs_and_hedge_legs.sql',
    'create_mv_reentry_live_prices.sql',
    'create_mv_reentry_breakout_context.sql',
    'create_mv_reentry_sl_hits.sql',
    'create_mv_reentry_sl_executions.sql',
    'create_mv_reentry_open_legs.sql',
    'create_mv_reentry_profit_booking.sql',
    'create_mv_reentry_eod_close.sql',
    'create_mv_reentry_final_exit.sql',
    'create_mv_reentry_legs_stats.sql',
    'create_mv_hedge_reentry_exit_on_all_entry_sl.sql',
    'create_mv_hedge_reentry_exit_on_partial_conditions.sql',
    'create_mv_hedge_reentry_closed_legs.sql',
    'create_mv_hedge_reentry_eod_exit.sql',
    'create_mv_reentry_exit_on_partial_hedge.sql',
    'create_mv_double_buy_legs_reentry.sql',
    # rehedge pipeline
    'create_mv_rehedge_trigger_reentry.sql',
    'create_mv_rehedge_candidate_reentry.sql',
    'create_mv_rehedge_selected_reentry.sql',
    'create_mv_rehedge_leg_reentry.sql',
    'create_mv_rehedge_eod_exit_reentry.sql',
    # final aggregation
    'create_mv_all_legs_reentry.sql',

        # stored procedure
    'sp_run_reentry_loop.sql',
    # additional views
    'create_mv_entry_leg_live_prices.sql',
    'create_mv_all_entries_sl_tracking_adjusted.sql',
    'create_mv_portfolio_mtm_pnl.sql',
    'create_mv_portfolio_final_pnl.sql',
    'create_strategy_run_results.sql',
    'sp_run_strategy.sql',
]

def consolidate_matviews_sql():
    sql_dir = Path('sql')
    output_file = Path('consolidated_matviews.sql')

    with open(output_file, 'w', encoding='utf-8') as outfile:
        # Write header
        outfile.write('-- =====================================================\n')
        outfile.write('-- New_BackTest_Pulse Materialized Views Initialization\n')
        outfile.write('-- Consolidated SQL file for matviews sequential setup\n')
        outfile.write('-- Generated from refresh_matviews_sequential.py order\n')
        outfile.write('-- =====================================================\n\n')

        current_section = ""
        section_counter = 0

        for sql_file in sql_files_order:
            sql_path = sql_dir / sql_file

            if not sql_path.exists():
                print(f"Warning: {sql_file} not found, skipping...")
                outfile.write(f'-- Warning: {sql_file} not found, skipping...\n\n')
                continue

            # Determine section based on file content/purpose
            if sql_file == 'create_v_strategy_config.sql':
                section = "1. STRATEGY CONFIGURATION"
            elif sql_file == 'create_heikin_ashi_tables.sql':
                section = "2. HEIKIN-ASHI TABLES"
            elif sql_file in ['create_mv_ha_candles.sql', 'create_mv_nifty_options_filtered.sql']:
                section = "3. BASE FILTERED VIEWS"
            elif sql_file == 'create_mv_all_5min_breakouts.sql':
                section = "4. BREAKOUT DETECTION"
            elif 'ranked_breakouts' in sql_file:
                section = "5. BREAKOUT RANKING"
            elif 'strike_selection' in sql_file or 'breakout_context' in sql_file:
                section = "6. STRIKE SELECTION & CONTEXT"
            elif 'entry_and_hedge_legs' in sql_file or 'live_prices' in sql_file:
                section = "7. LIVE PRICES & LEGS"
            elif 'sl_hits' in sql_file or 'sl_executions' in sql_file:
                section = "8. STOP LOSS DETECTION"
            elif 'entry_' in sql_file and 'round1' in sql_file and not 'hedge' in sql_file:
                section = "9. ENTRY LIFECYCLE"
            elif 'hedge_' in sql_file and 'round1' in sql_file and not 'reentry' in sql_file:
                section = "10. HEDGE EXIT LOGIC"
            elif 'entry_exit' in sql_file or 'double_buy' in sql_file or 'final_exit' in sql_file:
                section = "11. ENTRY-EXIT INTERACTIONS"
            elif 'rehedge_' in sql_file and 'round1' in sql_file:
                section = "12. REHEDGE PIPELINE"
            elif 'all_legs_round1' in sql_file or 'sp_insert' in sql_file:
                section = "13. ROUND 1 CONSOLIDATION"
            elif 'reentry_triggered' in sql_file:
                section = "14. REENTRY TRIGGERS"
            elif 'reentry_' in sql_file and not 'rehedge' in sql_file:
                section = "15. REENTRY PIPELINE"
            elif 'rehedge_' in sql_file and 'reentry' in sql_file:
                section = "16. REENTRY HEDGE LOGIC"
            elif 'all_legs_reentry' in sql_file:
                section = "17. REENTRY CONSOLIDATION"
            elif 'sp_run_reentry' in sql_file:
                section = "18. REENTRY PROCEDURES"
            elif 'entry_leg_live' in sql_file or 'all_entries_sl' in sql_file:
                section = "19. ADDITIONAL VIEWS"
            elif 'portfolio_' in sql_file:
                section = "20. PORTFOLIO CALCULATIONS"
            elif 'strategy_run_results' in sql_file or 'sp_run_strategy' in sql_file:
                section = "21. STRATEGY EXECUTION"
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

        # Add the final CALL statement
        outfile.write('-- =====================================================\n')
        outfile.write('-- 22. EXECUTE STRATEGY\n')
        outfile.write('-- =====================================================\n\n')
        outfile.write('-- Execute the main strategy stored procedure\n')
        outfile.write('CALL sp_run_strategy();\n\n')

        # Write footer
        outfile.write('-- =====================================================\n')
        outfile.write('-- Materialized views initialization completed!\n')
        outfile.write('-- =====================================================\n')

    print(f"Consolidated matviews SQL file created: {output_file}")
    print(f"Total SQL files processed: {len(sql_files_order)}")

if __name__ == '__main__':
    consolidate_matviews_sql()