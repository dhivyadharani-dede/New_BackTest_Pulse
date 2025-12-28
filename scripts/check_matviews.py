import sys
from pathlib import Path
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import get_conn

with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT matviewname FROM pg_catalog.pg_matviews WHERE schemaname='public' ORDER BY matviewname")
        rows = cur.fetchall()
print('Materialized views in public:')
for r in rows:
    print('public.' + r[0])

with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname='public' AND tablename='mv_all_5min_breakouts'")
        idxs = cur.fetchall()
print('\nIndexes on mv_all_5min_breakouts:')
for i in idxs:
    print(i[0])
