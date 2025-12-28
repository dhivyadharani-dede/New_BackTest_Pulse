import sys
from pathlib import Path
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import get_conn


def main():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY schemaname, tablename")
            rows = cur.fetchall()
    print('All user tables:')
    for s,t in rows:
        print(f"{s}.{t}")
    print('\nTables matching "%nifty%":')
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE tablename ILIKE '%nifty%' ORDER BY schemaname, tablename")
            rows = cur.fetchall()
    for s,t in rows:
        print(f"{s}.{t}")

if __name__ == '__main__':
    main()
