from src.db import get_conn

with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'Nifty50' ORDER BY ordinal_position")
        columns = cur.fetchall()
        print('Nifty50 columns:')
        for col in columns:
            print(' -', col[0])