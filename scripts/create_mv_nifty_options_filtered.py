import sys
from pathlib import Path
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))
from src.db import execute_sql


def main():
    sql_file = Path(repo_root) / 'sql' / 'create_mv_nifty_options_filtered.sql'
    if not sql_file.exists():
        print('SQL file missing:', sql_file)
        return
    try:
        execute_sql(sql_file.read_text())
        print('mv_nifty_options_filtered materialized view created/updated')
    except Exception as e:
        print('Failed to create mv_nifty_options_filtered:', e)


if __name__ == '__main__':
    main()
