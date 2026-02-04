from src.db import get_conn
with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' LIMIT 5")
        rows = cur.fetchall()
        print('Tables:', rows)