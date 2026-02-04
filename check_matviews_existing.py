from src.db import get_conn

with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT matviewname FROM pg_matviews ORDER BY matviewname")
        views = cur.fetchall()
        print('Existing materialized views:')
        for view in views:
            print(' -', view[0])