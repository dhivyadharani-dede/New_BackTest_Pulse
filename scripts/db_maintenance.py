#!/usr/bin/env python3
"""Run maintenance: create concurrent indexes and run VACUUM/ANALYZE.

This script uses `src.db.get_conn()` and sets `autocommit` where needed.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.db import get_conn


def main():
    with get_conn() as conn:
        conn.autocommit = True
        cur = conn.cursor()

        # handle multiple parent tables: Nifty_options and Nifty50
        parents = [
            {
                'name': 'Nifty_options',
                'indexes': [
                    ("{part}_symbol_date_idx", 'symbol, date', False),
                    ("{part}_date_idx", 'date', False),
                    ("{part}_date_brin", 'date', True),
                ],
            },
            {
                'name': 'Nifty50',
                'indexes': [
                    ("{part}_date_idx", 'date', False),
                    ("{part}_date_brin", 'date', True),
                    ("{part}_time_idx", '"time"', False),
                ],
            },
        ]

        for parent in parents:
            parent_name = parent['name']
            # parameterized lookup by parent relname to avoid quoting/case issues
            cur.execute(
                "SELECT c.relname FROM pg_inherits i JOIN pg_class p ON i.inhparent = p.oid JOIN pg_class c ON i.inhrelid = c.oid JOIN pg_namespace n ON p.relnamespace = n.oid WHERE p.relname = %s AND n.nspname = 'public';",
                (parent_name,)
            )
            parts = [r[0] for r in cur.fetchall()]
            if not parts:
                print(f"No partitions found for public.\\\"{parent_name}\\\". Create parent/partitions first.")
                continue
            for part in parts:
                for idx_tpl, cols, is_brin in parent['indexes']:
                    idx_name = idx_tpl.format(part=part)
                    if is_brin:
                        sql = f'CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} ON public."{part}" USING BRIN ({cols});'
                    else:
                        sql = f'CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} ON public."{part}" ({cols});'
                    print("Running:", sql)
                    cur.execute(sql)

        # Run VACUUM/ANALYZE on parents
        for parent in ['Nifty_options', 'Nifty50']:
            print(f'Running VACUUM (VERBOSE, ANALYZE) on {parent}')
            cur.execute(f'VACUUM (VERBOSE, ANALYZE) public."{parent}";')
            print(f'Running ANALYZE on {parent}')
            cur.execute(f'ANALYZE public."{parent}";')
        cur.close()

    print("Maintenance complete.")


if __name__ == '__main__':
    main()
