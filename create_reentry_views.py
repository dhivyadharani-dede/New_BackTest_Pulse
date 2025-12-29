import subprocess
import os

# Set environment variables
os.environ['PGHOST'] = 'localhost'
os.environ['PGPORT'] = '5432'
os.environ['PGDATABASE'] = 'Backtest_Pulse'
os.environ['PGUSER'] = 'postgres'
os.environ['PGPASSWORD'] = 'Alliswell@28'

files = [
    'sql/create_mv_ranked_breakouts_with_rounds_for_reentry.sql',
    'sql/create_mv_reentry_triggered_breakouts.sql',
    'sql/create_mv_reentry_base_strike_selection.sql',
    'sql/create_mv_reentry_legs_and_hedge_legs.sql',
    'sql/create_mv_reentry_live_prices.sql',
    'sql/create_mv_reentry_sl_hits.sql',
    'sql/create_mv_reentry_sl_executions.sql',
    'sql/create_mv_reentry_open_legs.sql',
    'sql/create_mv_reentry_legs_stats.sql',
    'sql/create_mv_reentry_profit_booking.sql',
    'sql/create_mv_reentry_eod_close.sql',
    'sql/create_mv_reentry_final_exit.sql',
    'sql/create_mv_reentry_exit_on_partial_hedge.sql',
    'sql/create_mv_hedge_reentry_eod_exit.sql',
    'sql/create_mv_rehedge_trigger_reentry.sql',
    'sql/create_mv_hedge_reentry_exit_on_partial_conditions.sql',
    'sql/create_mv_hedge_reentry_exit_on_all_entry_sl.sql',
    'sql/create_mv_rehedge_candidate_reentry.sql',
    'sql/create_mv_rehedge_selected_reentry.sql',
    'sql/create_mv_rehedge_leg_reentry.sql',
    'sql/create_mv_rehedge_eod_exit_reentry.sql',
    'sql/create_mv_all_legs_reentry.sql',
    'sql/create_mv_double_buy_legs_reentry.sql',
    'sql/create_mv_reentry_breakout_context.sql',
]

for f in files:
    print(f'Executing {f}')
    result = subprocess.run(['python', 'scripts/apply_sql_file.py', f], capture_output=True, text=True)
    if result.returncode != 0:
        print(f'Error executing {f}: {result.stderr}')
        break
    else:
        print(f'Success: {f}')