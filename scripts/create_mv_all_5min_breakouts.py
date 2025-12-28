import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import execute_sql


def main():
    # ensure v_strategy_config exists
    v_sql = Path(repo_root) / 'sql' / 'create_v_strategy_config.sql'
    if v_sql.exists():
        execute_sql(v_sql.read_text())
    sql_file = Path(repo_root) / 'sql' / 'create_mv_all_5min_breakouts.sql'
    if not sql_file.exists():
        print('SQL file missing:', sql_file)
        return
    execute_sql(sql_file.read_text())
    print('mv_all_5min_breakouts materialized view created/updated')


if __name__ == '__main__':
    main()
