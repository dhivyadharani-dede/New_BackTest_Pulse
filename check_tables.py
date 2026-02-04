from src.db import get_conn

with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = cur.fetchall()
        print('Available tables:')
        for table in tables:
            print(' -', table[0])