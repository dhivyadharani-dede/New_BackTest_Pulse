import sys
from pathlib import Path
from datetime import datetime, date, timedelta, time as dtime
import io

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import pandas as pd
from src.db import get_conn


def daterange(start: date, end: date):
    cur = start
    while cur < end:
        yield cur
        cur += timedelta(days=1)


def fetch_day_rows(conn, day: date):
    sql = "SELECT time, open, high, low, close FROM public.\"Nifty50\" WHERE date = %(d)s AND time >= '09:15:00' ORDER BY time"
    with conn.cursor() as cur:
        cur.execute(sql, {'d': day.isoformat()})
        rows = cur.fetchall()
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=['time','open','high','low','close'])
    # create datetime index
    # ensure numeric types are floats (psycopg2 returns Decimal for numeric)
    for c in ['open','high','low','close']:
        df[c] = pd.to_numeric(df[c], errors='coerce').astype(float)
    # create datetime index from date + time
    df.index = pd.to_datetime(day.isoformat() + ' ' + df['time'].astype(str))
    return df


def make_candles_for_interval(df: pd.DataFrame, interval_min: int, day: date):
    # anchor at 09:15
    day_start = datetime.combine(day, dtime(0,0))
    anchor = datetime.combine(day, dtime(9,15))
    # seconds from anchor
    seconds = (df.index - anchor).total_seconds()
    # rows before anchor are ignored (seconds < 0)
    df = df[seconds >= 0]
    if df.empty:
        return None
    seconds = (df.index - anchor).total_seconds()
    bucket = (seconds // (interval_min * 60)).astype(int)
    df2 = df.copy()
    df2['bucket'] = bucket
    grouped = df2.groupby('bucket', sort=True).agg(
        open=('open','first'),
        high=('high','max'),
        low=('low','min'),
        close=('close','last')
    ).reset_index()
    # compute candle_time
    grouped['trade_date'] = day
    grouped['candle_time'] = grouped['bucket'].apply(lambda b: (anchor + timedelta(minutes=b*interval_min)).time())
    # reorder
    grouped = grouped[['trade_date','candle_time','open','high','low','close']]
    return grouped


def compute_heikin_ashi(candles: pd.DataFrame):
    # candles expected sorted by candle_time
    if candles is None or candles.empty:
        return candles
    ha_close = (candles['open'] + candles['high'] + candles['low'] + candles['close']) / 4.0
    ha_open = [candles['open'].iloc[0]]
    for i in range(1, len(candles)):
        prev = (ha_open[i-1] + ha_close.iloc[i-1]) / 2.0
        ha_open.append(prev)
    candles = candles.copy()
    candles['ha_close'] = ha_close.round(2)
    candles['ha_open'] = pd.Series(ha_open).round(2)
    candles['ha_high'] = candles[['high','ha_open','ha_close']].max(axis=1).round(2)
    candles['ha_low'] = candles[['low','ha_open','ha_close']].min(axis=1).round(2)
    return candles


def copy_df_to_table(conn, df: pd.DataFrame, table: str):
    if df is None or df.empty:
        return 0
    buf = io.StringIO()
    # prepare CSV rows: trade_date, candle_time, open, high, low, close, ha_open, ha_high, ha_low, ha_close
    df_to_write = df[['trade_date','candle_time','open','high','low','close','ha_open','ha_high','ha_low','ha_close']]
    df_to_write.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur = conn.cursor()
    # use COPY with CSV
    cur.copy_expert(f"COPY public.{table} (trade_date, candle_time, open, high, low, close, ha_open, ha_high, ha_low, ha_close) FROM STDIN WITH CSV", buf)
    conn.commit()
    return len(df_to_write)


def process_range(start: date, end: date, interval_min: int, table: str):
    total = 0
    with get_conn(dict_cursor=False) as conn:
        for day in daterange(start, end):
            df_day = fetch_day_rows(conn, day)
            if df_day is None:
                continue
            candles = make_candles_for_interval(df_day, interval_min, day)
            if candles is None or candles.empty:
                continue
            ha = compute_heikin_ashi(candles)
            inserted = copy_df_to_table(conn, ha, table)
            total += inserted
    print(f"Inserted {total} rows into {table} for {start}..{end}")
    return total


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--strategy', default='default')
    args = parser.parse_args()

    start = datetime.strptime(args.start, '%Y-%m-%d').date()
    end = datetime.strptime(args.end, '%Y-%m-%d').date()

    # read strategy settings
    # minimal inline query to get intervals
    with get_conn(dict_cursor=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT big_candle_tf::int as big, small_candle_tf::int as small, one_m_candle_tf::int as one_m FROM public.strategy_settings WHERE strategy_name = %s", (args.strategy,))
            row = cur.fetchone()
    if not row:
        big, small, one_m = 15, 5, 1
    else:
        big, small, one_m = row

    print('Intervals:', big, small, one_m)

    targets = [ (big, 'ha_big'), (small, 'ha_small'), (one_m, 'ha_1m') ]
    for interval, table in targets:
        print('Processing', table, interval, 'minutes')
        process_range(start, end, interval, table)

    print('Done')
