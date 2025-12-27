#!/usr/bin/env python3
"""Create the `strategy_settings` table by running the SQL in `sql/create_strategy_settings.sql`."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.db import execute_sql


def main():
    sql_path = Path(__file__).resolve().parents[1] / 'sql' / 'create_strategy_settings.sql'
    sql = sql_path.read_text()
    execute_sql(sql)
    print('strategy_settings table created or already exists.')


if __name__ == '__main__':
    main()
