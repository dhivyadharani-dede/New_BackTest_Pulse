#!/usr/bin/env python3
"""Create yearly partitions for the `Nifty50` partitioned table.

Usage:
    python scripts/create_nifty50_partitions.py --start 2015 --end 2026

This script reads `sql/create_nifty50.sql` to create the parent table, then creates
one partition per year (name: Nifty50_yYYYY) with RANGE bounds.
"""
import argparse
from pathlib import Path
import sys
# Ensure repo root is on sys.path so `from src...` imports work when running scripts directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.db import execute_sql


def create_parent(sql_path: Path):
    sql = sql_path.read_text()
    execute_sql(sql)


def create_year_partition(year: int):
    part_name = f'Nifty50_y{year}'
    start = f"'{year}-01-01'"
    end = f"'{year+1}-01-01'"
    sql = f'''CREATE TABLE IF NOT EXISTS public."{part_name}" PARTITION OF public."Nifty50"
    FOR VALUES FROM ({start}) TO ({end});
    CREATE INDEX IF NOT EXISTS {part_name}_date_idx ON public."{part_name}" (date);
    '''
    execute_sql(sql)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=int, required=True, help="Start year (inclusive)")
    p.add_argument("--end", type=int, required=True, help="End year (exclusive)")
    args = p.parse_args()

    sql_path = Path(__file__).resolve().parents[1] / "sql" / "create_nifty50.sql"
    print(f"Creating parent table from: {sql_path}")
    create_parent(sql_path)

    for y in range(args.start, args.end):
        print(f"Creating partition for year {y}")
        create_year_partition(y)

    print("Partitions creation complete.")


if __name__ == "__main__":
    main()
