import sys
from pathlib import Path
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import get_conn

def describe(table_name):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position", (table_name,))
            rows = cur.fetchall()
    print('Columns for', table_name)
    for r in rows:
        print(r[0], r[1])

if __name__ == '__main__':
    describe('Nifty_options')
