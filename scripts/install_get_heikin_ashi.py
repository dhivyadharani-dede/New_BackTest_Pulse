import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import execute_sql


def main():
    sql_file = Path(repo_root) / 'sql' / 'get_heikin_ashi.sql'
    if not sql_file.exists():
        print(f"SQL file not found: {sql_file}")
        return
    sql = sql_file.read_text()
    execute_sql(sql)
    print("Installed get_heikin_ashi function")


if __name__ == '__main__':
    main()
