"""Apply SQL files (in sequence) and refresh the materialized views they create.

This script executes SQL files from the `sql/` folder in a dependency-aware order
and then refreshes any materialized view created by each file. Use this when you
update a base matview's SQL and want downstream matviews recreated/refreshed.

Usage:
    python .\scripts\refresh_matviews_sequential.py
"""
import re
import sys
from pathlib import Path
import subprocess
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import get_conn


SQL_DIR = repo_root / 'sql'

# Ordered list of SQL filenames (relative to repo root). Edit if you add new
# matviews that have dependencies.
ORDERED_SQL = [
    'create_v_strategy_config.sql',
    'create_filtered_views.sql'
    # base tables / filtered views
    # 'create_heikin_ashi_tables.sql',
    # 'create_mv_ha_candles.sql',
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
    # 'create_temp_live_prices_entry_round1.sql',

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
    'create_mv_reentry_triggered_breakouts.sql',
    # reentry pipeline
    'create_mv_reentry_base_strike_selection.sql',
    'create_mv_reentry_legs_and_hedge_legs.sql',
    'create_mv_reentry_live_prices.sql',
    # 'create_temp_reentry_live_prices.sql',
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
    # 'create_mv_reentry_exit_on_partial_hedge.sql',
    'create_mv_double_buy_legs_reentry.sql',
    # rehedge pipeline
    'create_mv_rehedge_trigger_reentry.sql',
    'create_mv_rehedge_candidate_reentry.sql',
    'create_mv_rehedge_selected_reentry.sql',
    'create_mv_rehedge_leg_reentry.sql',
    'create_mv_rehedge_eod_exit_reentry.sql',
    # final aggregation

    'create_mv_all_legs_reentry.sql',

    'sp_insert_sl_legs_into_book.sql',

        # stored procedure
    'sp_run_reentry_loop.sql',
    # additional views
    'create_mv_entry_leg_live_prices.sql',
    # 'create_temp_entry_leg_live_prices.sql',
    'create_mv_all_entries_sl_tracking_adjusted.sql',
    'create_mv_portfolio_mtm_pnl.sql',
    'create_mv_portfolio_final_pnl.sql',
    'create_strategy_run_results.sql',
    'sp_run_strategy.sql',


]


def extract_matview_names(sql_text):
    # crude regex to find CREATE MATERIALIZED VIEW <schema>.<name> or CREATE MATERIALIZED VIEW IF NOT EXISTS <name>
    pattern = re.compile(r'CREATE\s+MATERIALIZED\s+VIEW(?:\s+IF\s+NOT\s+EXISTS)?\s+(?:public\.)?"?([a-zA-Z0-9_]+)"?', re.IGNORECASE)
    return pattern.findall(sql_text)


def apply_sql_file(path: Path):
    sql = path.read_text(encoding='utf8')
    print(f"Applying SQL: {path.relative_to(repo_root)}")
    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql)
                conn.commit()
                print(f"Executed {path.name}")
            except Exception as e:
                conn.rollback()
                print(f"Error executing {path.name}: {e}")
                raise
    return extract_matview_names(sql)


def refresh_matview(name: str):
    print(f"Refreshing materialized view: public.{name}")
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Check if it's actually a materialized view
                cur.execute("SELECT 1 FROM pg_matviews WHERE matviewname = %s", (name,))
                if cur.fetchone():
                    cur.execute(f'REFRESH MATERIALIZED VIEW public."{name}"')
                    conn.commit()
                    print(f"Refreshed: public.{name}")
                else:
                    print(f"Skipping refresh for regular view: public.{name}")
    except Exception as e:
        print(f"Failed to refresh public.{name}: {e}")


def main():
    for fname in ORDERED_SQL:
        if fname.startswith('CALL '):
            # Execute raw SQL
            sql = fname
            print(f"Executing raw SQL: {sql}")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    try:
                        cur.execute(sql)
                        conn.commit()
                        print(f"Executed: {sql}")
                    except Exception as e:
                        conn.rollback()
                        print(f"Error executing {sql}: {e}")
                        raise
            continue

        path = SQL_DIR / fname
        if not path.exists():
            print(f"Skipping missing SQL file: {fname}")
            continue

        # Skip stored procedure files
        if fname.startswith('sp_'):
            print(f"Skipping stored procedure file: {fname}")
            continue

        try:
            mv_names = apply_sql_file(path)
        except Exception:
            print(f"Stopping due to error applying {fname}")
            return

        # refresh any materialized views declared in this SQL file
        for mv in mv_names:
            refresh_matview(mv)

    print('All done.')


if __name__ == '__main__':
    main()
