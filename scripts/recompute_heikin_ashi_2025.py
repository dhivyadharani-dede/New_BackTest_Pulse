import sys
from pathlib import Path
from datetime import date

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import execute_sql, get_conn
import scripts.compute_heikin_ashi_py as compute


def delete_range(table: str, start: date, end: date):
    sql = f"DELETE FROM public.{table} WHERE trade_date >= %(start)s AND trade_date < %(end)s"
    execute_sql(sql, {'start': start.isoformat(), 'end': end.isoformat()})
    print(f"Deleted rows from {table} for {start}..{end}")


if __name__ == '__main__':
    start = date(2025,1,1)
    end = date(2026,1,1)
    tables = ['ha_big','ha_small','ha_1m']
    for t in tables:
        delete_range(t, start, end)

    # process month-by-month
    m_start = start
    while m_start < end:
        if m_start.month == 12:
            m_end = date(m_start.year+1,1,1)
        else:
            m_end = date(m_start.year, m_start.month+1, 1)
        print(f"Recomputing {m_start}..{m_end}")
        # call internal function
        # compute.process_range(start, end, interval_min, table)
        # read intervals from DB
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT big_candle_tf::int as big, small_candle_tf::int as small, one_m_candle_tf::int as one_m FROM public.strategy_settings WHERE strategy_name = %s", ('default',))
                row = cur.fetchone()
        if not row:
            big, small, one_m = 15,5,1
        else:
            big, small, one_m = row
        targets = [ (big, 'ha_big'), (small, 'ha_small'), (one_m, 'ha_1m') ]
        for interval, table in targets:
            compute.process_range(m_start, m_end, interval, table)
        m_start = m_end

    print('Recompute complete')
