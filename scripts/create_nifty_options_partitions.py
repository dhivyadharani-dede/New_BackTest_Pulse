#!/usr/bin/env python3
"""Create monthly partitions for the `Nifty_options` partitioned table.

Usage:
    python scripts/create_nifty_options_partitions.py --start 2021-01 --end 2025-12

This script reads `sql/create_nifty_options.sql` to create the parent table, then creates
one partition per month (name: Nifty_options_YYYY_MM) with RANGE bounds.
"""
import argparse
from pathlib import Path
import sys
# make sure src is importable when running directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.db import execute_sql
from datetime import datetime, timedelta


def create_parent(sql_path: Path):
    sql = sql_path.read_text()
    execute_sql(sql)


def create_month_partition(year: int, month: int):
    part_name = f'Nifty_options_{year}_{month:02d}'
    start = f"'{year}-{month:02d}-01'"
    # compute next month
    if month == 12:
        end = f"'{year+1}-01-01'"
    else:
        end = f"'{year}-{month+1:02d}-01'"

    sql = f'''CREATE TABLE IF NOT EXISTS public."{part_name}" PARTITION OF public."Nifty_options"
    FOR VALUES FROM ({start}) TO ({end});
    CREATE INDEX IF NOT EXISTS {part_name}_date_idx ON public."{part_name}" (date);
    CREATE INDEX IF NOT EXISTS {part_name}_symbol_idx ON public."{part_name}" (symbol);
    '''
    execute_sql(sql)


def parse_ym(s: str):
    return datetime.strptime(s, "%Y-%m")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, help="Start year-month e.g. 2021-01")
    p.add_argument("--end", required=True, help="End year-month inclusive e.g. 2025-12")
    args = p.parse_args()

    sql_path = Path(__file__).resolve().parents[1] / "sql" / "create_nifty_options.sql"
    print(f"Creating parent table from: {sql_path}")
    create_parent(sql_path)

    start_dt = parse_ym(args.start)
    end_dt = parse_ym(args.end)

    cur = start_dt
    while cur <= end_dt:
        print(f"Creating partition for {cur.year}-{cur.month:02d}")
        create_month_partition(cur.year, cur.month)
        # advance by one month
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)

    print("Monthly partitions creation complete.")


if __name__ == "__main__":
    main()
