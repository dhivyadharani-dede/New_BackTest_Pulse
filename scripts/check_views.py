import sys
from pathlib import Path
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import get_conn

with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT schemaname, viewname FROM pg_catalog.pg_views WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY schemaname, viewname")
        rows = cur.fetchall()

print('All user views:')
for s,v in rows:
    print(f"{s}.{v}")

print('\nViews matching "%nifty%":')
with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT schemaname, viewname FROM pg_catalog.pg_views WHERE viewname ILIKE '%nifty%' ORDER BY schemaname, viewname")
        rows = cur.fetchall()
for s,v in rows:
    print(f"{s}.{v}")
