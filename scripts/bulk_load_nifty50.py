#!/usr/bin/env python3
"""Bulk load CSVs into `Nifty50` partitions; auto-create partitions by year or month.

Usage:
    python scripts/bulk_load_nifty50.py --csv data/nifty50_2015_2020.csv --partitioning yearly

Behavior:
- Reads the input CSV in chunks to avoid memory pressure.
- For each chunk, groups rows by partition key (year or year-month).
- Ensures the partition exists (creates it if missing).
- Uses `COPY FROM STDIN` to efficiently load partitioned tables.

Notes:
- Expects CSV columns: date (YYYY-MM-DD), time (HH:MM:SS), open, high, low, close, volume, oi, option_nm
- Requires `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` env vars set.
"""
from pathlib import Path
import argparse
import pandas as pd
import io
import sys
# Ensure repo root is on sys.path so `from src...` imports work when running scripts directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.db import get_conn, execute_sql


def partition_name_for_date(dt, partitioning: str):
    if partitioning == "yearly":
        return f'Nifty50_y{dt.year}'
    elif partitioning == "monthly":
        return f'Nifty50_{dt.year}_{dt.month:02d}'
    else:
        raise ValueError("partitioning must be 'yearly' or 'monthly'")


def ensure_partition_exists(dt, partitioning: str):
    name = partition_name_for_date(dt, partitioning)
    if partitioning == "yearly":
        start = f"'{dt.year}-01-01'"
        end = f"'{dt.year+1}-01-01'"
    else:
        month = dt.month
        year = dt.year
        start = f"'{year}-{month:02d}-01'"
        # compute next month
        if month == 12:
            end = f"'{year+1}-01-01'"
        else:
            end = f"'{year}-{month+1:02d}-01'"

    create_sql = f'''
    CREATE TABLE IF NOT EXISTS public."{name}" PARTITION OF public."Nifty50"
        FOR VALUES FROM ({start}) TO ({end});
    CREATE INDEX IF NOT EXISTS {name}_date_idx ON public."{name}" (date);
    '''
    execute_sql(create_sql)
    return name


def copy_rows_to_partition(conn, partition_name: str, df: pd.DataFrame):
    # Ensure ordering of columns matches table definition
    cols = ["date", "time", "open", "high", "low", "close", "volume", "oi", "option_nm"]
    buf = io.StringIO()
    df.to_csv(buf, columns=cols, index=False, header=False)
    buf.seek(0)

    with conn.cursor() as cur:
        copy_sql = f'COPY public."{partition_name}" ({",".join(cols)}) FROM STDIN WITH CSV'
        cur.copy_expert(copy_sql, buf)
    conn.commit()


def bulk_load(csv_path: Path, partitioning: str = "yearly", chunksize: int = 100_000):
    reader = pd.read_csv(csv_path, parse_dates=["date"], chunksize=chunksize)
    with get_conn() as conn:
        for chunk in reader:
            # Ensure date column is datetime
            chunk["date"] = pd.to_datetime(chunk["date"]).dt.date
            # group by partition key
            chunk["_part_key"] = chunk["date"].apply(lambda d: partition_name_for_date(pd.Timestamp(d), partitioning))
            for part, subdf in chunk.groupby("_part_key"):
                # ensure partition exists for first row's date
                sample_dt = pd.Timestamp(subdf.iloc[0]["date"])
                ensure_partition_exists(sample_dt, partitioning)
                print(f"Loading {len(subdf)} rows into partition {part}")
                copy_rows_to_partition(conn, part, subdf)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="CSV file to load")
    p.add_argument("--partitioning", choices=["yearly", "monthly"], default="yearly")
    p.add_argument("--chunksize", type=int, default=100000)
    return p.parse_args()


def main():
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")
    bulk_load(csv_path, partitioning=args.partitioning, chunksize=args.chunksize)


if __name__ == "__main__":
    main()
