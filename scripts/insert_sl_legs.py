#!/usr/bin/env python3
"""Insert SL legs from mv_all_legs_pnl_entry_round1 into strategy_leg_book.

Usage:
  python .\scripts\insert_sl_legs.py [--truncate] [--batch N] [--dry-run]

Options:
  --truncate   Delete all rows from `strategy_leg_book` before inserting.
  --batch N    Number of rows per batch (default 2000).
  --dry-run    Do not execute inserts, only print counts.
"""
import argparse
from psycopg2.extras import execute_values
from src.db import get_conn, fetch_sql_to_dict


SELECT_SQL = """
SELECT
  trade_date,
  expiry_date,
  breakout_time,
  entry_time,
  exit_time,
  option_type,
  strike,
  entry_price,
  exit_price,
  transaction_type,
  leg_type,
  entry_round,
  exit_reason
FROM mv_all_legs_pnl_entry_round1
"""

INSERT_SQL = (
    "INSERT INTO strategy_leg_book ("
    "trade_date, expiry_date, breakout_time, entry_time, exit_time, "
    "option_type, strike, entry_price, exit_price, transaction_type, "
    "leg_type, entry_round, exit_reason) VALUES %s ON CONFLICT DO NOTHING RETURNING 1"
)


def run(truncate: bool, batch: int, dry_run: bool):
    total_selected = 0
    total_inserted = 0

    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS public.strategy_leg_book (
        trade_date date NOT NULL,
        expiry_date date NOT NULL,
        breakout_time time without time zone,
        entry_time time without time zone NOT NULL,
        exit_time time without time zone,
        option_type text NOT NULL,
        strike numeric NOT NULL,
        entry_price numeric NOT NULL,
        exit_price numeric,
        transaction_type text NOT NULL,
        leg_type text NOT NULL,
        entry_round integer NOT NULL DEFAULT 1,
        exit_reason text,
        CONSTRAINT strategy_leg_book_pkey PRIMARY KEY (trade_date, expiry_date, strike, option_type, entry_round, leg_type)
    );
    """

    # Ensure destination table exists
    if not dry_run:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
            conn.commit()

    if truncate:
        print('Truncating table `strategy_leg_book`')
        if not dry_run:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute('DELETE FROM strategy_leg_book')
                conn.commit()

    print('Reading rows from `mv_all_legs_reentry`')
    for chunk in fetch_sql_to_dict(SELECT_SQL, chunksize=batch):
        # fetch_sql_to_dict returns list of dict rows per chunk
        rows = chunk
        total_selected += len(rows)
        if not rows:
            continue

        tuples = [(
            r['trade_date'],
            r['expiry_date'],
            r['breakout_time'],
            r['entry_time'],
            r.get('exit_time'),
            r['option_type'],
            r['strike'],
            r['entry_price'],
            r.get('exit_price'),
            r['transaction_type'],
            r['leg_type'],
            r['entry_round'],
            r.get('exit_reason'),
        ) for r in rows]

        print(f'Prepared batch of {len(tuples)} rows')
        if dry_run:
            continue

        with get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(cur, INSERT_SQL, tuples, page_size=batch)
                inserted_rows = cur.fetchall()
            conn.commit()

        inserted_count = len(inserted_rows) if inserted_rows is not None else 0
        total_inserted += inserted_count

    print(f'Total selected: {total_selected}')
    if not dry_run:
        print(f'Total attempted inserts (ON CONFLICT DO NOTHING): {total_inserted}')
    else:
        print('Dry run, no inserts executed')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--truncate', action='store_true')
    parser.add_argument('--batch', type=int, default=2000)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    run(args.truncate, args.batch, args.dry_run)


if __name__ == '__main__':
    main()
