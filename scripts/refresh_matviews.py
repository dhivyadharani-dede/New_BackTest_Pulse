import sys
from pathlib import Path
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import get_conn

def main():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT matviewname FROM pg_catalog.pg_matviews WHERE schemaname='public' ORDER BY matviewname")
            rows = cur.fetchall()
            matviews = [r[0] for r in rows]

    if not matviews:
        print('No materialized views found in public schema.')
        return

    for mv in matviews:
        print(f"Refreshing materialized view: public.{mv}")
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f'REFRESH MATERIALIZED VIEW public."{mv}"')
                    conn.commit()
            print(f"Refreshed: public.{mv}")
        except Exception as e:
            print(f"Failed to refresh public.{mv}: {e}")

if __name__ == '__main__':
    main()
