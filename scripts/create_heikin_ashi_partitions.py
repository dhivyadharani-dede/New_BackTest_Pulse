import sys
from pathlib import Path
from datetime import date, timedelta

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import execute_sql


def month_range(start_date: date, end_date: date):
    cur = date(start_date.year, start_date.month, 1)
    while cur <= end_date:
        yield cur
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)


def create_partition(parent: str, year: int, month: int):
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)
    part_name = f"{parent}_{year}_{month:02d}"
    sql = f"""
    CREATE TABLE IF NOT EXISTS public.{part_name} PARTITION OF public.{parent}
      FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}');
    """
    execute_sql(sql)
    # create indexes on the partition for faster queries
    idx1 = f"CREATE INDEX IF NOT EXISTS idx_{part_name}_trade_time ON public.{part_name} (trade_date, candle_time);"
    idx2 = f"CREATE INDEX IF NOT EXISTS idx_{part_name}_candle_time ON public.{part_name} (candle_time);"
    execute_sql(idx1)
    execute_sql(idx2)
    print(f"Created partition {part_name} for {start.isoformat()} to {end.isoformat()}")


if __name__ == '__main__':
    # default range: last 12 months
    from datetime import datetime
    today = datetime.utcnow().date()
    start = date(2018, 1, 1)
    end = date(2025, 12, 1)

    # ensure parent tables exist by applying SQL file
    create_tables_sql = Path(repo_root) / 'sql' / 'create_heikin_ashi_tables.sql'
    if create_tables_sql.exists():
        execute_sql(create_tables_sql.read_text())
        print("Parent HA tables ensured")

    parents = ['ha_big', 'ha_small', 'ha_1m']
    for p in parents:
        for m in month_range(start, end):
            create_partition(p, m.year, m.month)

    print('Partition creation complete')
