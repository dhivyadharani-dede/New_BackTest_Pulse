import sys
from pathlib import Path
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import get_conn

if len(sys.argv) < 2:
    print('Usage: python run_sql.py "SELECT ..."')
    sys.exit(1)

sql = sys.argv[1]
with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
for r in rows:
    print(r)
