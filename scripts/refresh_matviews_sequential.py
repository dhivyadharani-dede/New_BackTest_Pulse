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
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import get_conn


SQL_DIR = repo_root / 'sql'

# Ordered list of SQL filenames (relative to repo root). Edit if you add new
# matviews that have dependencies.
ORDERED_SQL = [
    'create_mv_all_5min_breakouts.sql',
    'create_mv_base_strike_selection.sql',
    'create_mv_breakout_context_round1.sql',
    'create_mv_ranked_breakouts_with_rounds.sql',
    'create_mv_ranked_breakouts_with_rounds_for_reentry.sql',
    'create_mv_live_prices_entry_round1.sql',
    'create_mv_entry_and_hedge_legs.sql',
    'create_mv_entry_sl_hits_round1.sql',
    'create_mv_entry_sl_executions_round1.sql',
    'create_mv_entry_open_legs_round1.sql',
    'create_mv_entry_profit_booking_round1.sql',
    'create_mv_entry_eod_close_round1.sql',
    'create_mv_entry_closed_legs_round1.sql',
    'create_mv_entry_round1_stats.sql',
    'create_mv_hedge_exit_on_all_entry_sl.sql',
    'create_mv_hedge_exit_partial_conditions.sql',
    'create_mv_hedge_closed_legs_round1.sql',
    'create_mv_hedge_eod_exit_round1.sql',
    'create_mv_entry_exit_on_partial_hedge_round1.sql',
    'create_mv_double_buy_legs_round1.sql',
    'create_mv_entry_final_exit_round1.sql'
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
                cur.execute(f'REFRESH MATERIALIZED VIEW public."{name}"')
                conn.commit()
        print(f"Refreshed: public.{name}")
    except Exception as e:
        print(f"Failed to refresh public.{name}: {e}")


def main():
    for fname in ORDERED_SQL:
        path = SQL_DIR / fname
        if not path.exists():
            print(f"Skipping missing SQL file: {fname}")
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
