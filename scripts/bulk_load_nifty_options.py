#!/usr/bin/env python3
"""Bulk load options CSVs into `Nifty_options` monthly partitions, filtering option_type to P or C.

Usage:
    python scripts/bulk_load_nifty_options.py --csv "C:\path\to\options_2021.csv" --chunksize 100000

Behavior:
- Reads CSV in chunks, filters rows where `option_type` is 'P' or 'C' (case-insensitive).
- Ensures monthly partitions exist, then COPYs rows into respective partition.
"""
from pathlib import Path
import argparse
import pandas as pd
import io
import sys
# Ensure repo root is on sys.path for direct script execution
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.db import get_conn, execute_sql


def partition_name_for_date(dt):
    return f'Nifty_options_{dt.year}_{dt.month:02d}'


def ensure_partition_exists(dt):
    name = partition_name_for_date(dt)
    start = f"'{dt.year}-{dt.month:02d}-01'"
    if dt.month == 12:
        end = f"'{dt.year+1}-01-01'"
    else:
        end = f"'{dt.year}-{dt.month+1:02d}-01'"

    create_sql = f'''
    CREATE TABLE IF NOT EXISTS public."{name}" PARTITION OF public."Nifty_options"
        FOR VALUES FROM ({start}) TO ({end});
    CREATE INDEX IF NOT EXISTS {name}_date_idx ON public."{name}" (date);
    CREATE INDEX IF NOT EXISTS {name}_symbol_idx ON public."{name}" (symbol);
    '''
    execute_sql(create_sql)
    return name


def copy_rows_to_partition(conn, partition_name: str, df: pd.DataFrame):
    cols = [
        "symbol",
        "date",
        "expiry",
        "strike",
        "option_type",
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "oi",
        "option_nm",
    ]
    buf = io.StringIO()
    df.to_csv(buf, columns=cols, index=False, header=False)
    buf.seek(0)

    with conn.cursor() as cur:
        copy_sql = f'COPY public."{partition_name}" ({",".join(cols)}) FROM STDIN WITH CSV'
        cur.copy_expert(copy_sql, buf)
    conn.commit()


def bulk_load_options(csv_path: Path, chunksize: int = 100_000):
    reader = pd.read_csv(csv_path, parse_dates=["date", "expiry"], chunksize=chunksize)
    valid_types = {"p", "c"}
    with get_conn() as conn:
        for chunk in reader:
            # Normalize column names
            chunk.columns = [c.strip() for c in chunk.columns]
            if "option_type" not in chunk.columns:
                raise SystemExit("CSV missing 'option_type' column")

            # Drop rows without valid option type
            chunk["option_type"] = chunk["option_type"].astype(str).str.strip()
            chunk["_otype"] = chunk["option_type"].str.lower()
            chunk = chunk[chunk["_otype"].isin(valid_types)]
            if chunk.empty:
                continue

            # Ensure date column is date
            chunk["date"] = pd.to_datetime(chunk["date"]).dt.date

            # Ensure required columns exist; fill missing with sensible defaults
            required_cols = [
                "symbol",
                "date",
                "expiry",
                "strike",
                "option_type",
                "time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "oi",
                "option_nm",
            ]
            for col in required_cols:
                if col not in chunk.columns:
                    if col in ("volume", "oi", "strike", "open", "high", "low", "close"):
                        chunk[col] = 0
                    else:
                        chunk[col] = ""

            # Group by partition (month)
            chunk["_part_key"] = chunk["date"].apply(lambda d: partition_name_for_date(pd.Timestamp(d)))
            for part, subdf in chunk.groupby("_part_key"):
                sample_dt = pd.Timestamp(subdf.iloc[0]["date"])
                ensure_partition_exists(sample_dt)
                print(f"Loading {len(subdf)} option rows into partition {part}")
                copy_rows_to_partition(conn, part, subdf)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="CSV file to load")
    p.add_argument("--chunksize", type=int, default=100000)
    return p.parse_args()


def main():
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")
    bulk_load_options(csv_path, chunksize=args.chunksize)


if __name__ == "__main__":
    import pandas as pd
    main()
