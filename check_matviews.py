from src.db import get_conn

print('Checking materialized views...')

try:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT matviewname FROM pg_catalog.pg_matviews WHERE schemaname='public' ORDER BY matviewname")
            rows = cur.fetchall()
            matviews = [r[0] for r in rows]

    print(f'Found {len(matviews)} materialized views:')
    for mv in matviews:
        print(f'  - {mv}')

except Exception as e:
    print(f'Error: {e}')