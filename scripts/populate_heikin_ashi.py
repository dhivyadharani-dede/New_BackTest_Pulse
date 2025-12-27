import sys
from pathlib import Path
from datetime import datetime, timedelta, date

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import execute_sql, fetch_sql_to_dict


def get_strategy_intervals(strategy_name='default'):
    sql = "SELECT big_candle_tf::int as big, small_candle_tf::int as small, one_m_candle_tf::int as one_m FROM public.strategy_settings WHERE strategy_name = %(name)s"
    rows_outer = list(fetch_sql_to_dict(sql, {'name': strategy_name}))
    # fetch_sql_to_dict yields a single list of rows when chunksize is None
    if not rows_outer:
        return 15, 5, 1
    first = rows_outer[0]
    if isinstance(first, list):
        if not first:
            return 15, 5, 1
        r = first[0]
    else:
        r = first
    try:
        return int(r['big']), int(r['small']), int(r['one_m'])
    except Exception:
        return 15, 5, 1


def insert_from_function(interval_minutes: int, target_table: str, start_date: date, end_date: date):
    # Insert into partitioned target for the date range. Use SELECT FROM get_heikin_ashi(interval)
    sql = f"""
    INSERT INTO public.{target_table} (trade_date, candle_time, open, high, low, close, ha_open, ha_high, ha_low, ha_close)
    SELECT trade_date, candle_time, open, high, low, close, ha_open, ha_high, ha_low, ha_close
    FROM public.get_heikin_ashi(%(interval)s)
    WHERE trade_date >= %(start)s AND trade_date < %(end)s
    ON CONFLICT DO NOTHING;
    """
    params = {'interval': interval_minutes, 'start': start_date.isoformat(), 'end': end_date.isoformat()}
    execute_sql(sql, params)
    print(f"Inserted HA ({interval_minutes}m) into {target_table} for {start_date}..{end_date}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--strategy', default='default')
    parser.add_argument('--start', help='inclusive start date YYYY-MM-DD')
    parser.add_argument('--end', help='exclusive end date YYYY-MM-DD')
    args = parser.parse_args()

    big, small, one_m = get_strategy_intervals(args.strategy)
    print('Using intervals (big, small, one_m):', big, small, one_m)

    if args.end:
        end = datetime.strptime(args.end, '%Y-%m-%d').date()
    else:
        end = datetime.utcnow().date() + timedelta(days=1)
    if args.start:
        start = datetime.strptime(args.start, '%Y-%m-%d').date()
    else:
        start = end - timedelta(days=30)

    targets = [ (big, 'ha_big'), (small, 'ha_small'), (one_m, 'ha_1m') ]
    for interval, table in targets:
        insert_from_function(interval, table, start, end)

    print('Population complete')
