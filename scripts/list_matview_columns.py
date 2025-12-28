import sys
from pathlib import Path
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import get_conn

def list_columns(matview_name):
    sql = """
SELECT a.attname AS column_name, pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE c.relkind = 'm' -- materialized view
  AND n.nspname = 'public'
  AND c.relname = %s
  AND a.attnum > 0
ORDER BY a.attnum;
"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (matview_name,))
            rows = cur.fetchall()
    if not rows:
        print('No such materialized view or no columns:', matview_name)
        return
    print('Matview columns for', matview_name)
    for r in rows:
        print(r[0], r[1])

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python list_matview_columns.py matview_name')
        sys.exit(1)
    list_columns(sys.argv[1])
