from src.db import get_conn
with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM nifty50 WHERE date BETWEEN '2023-02-01' AND '2023-02-05'")
        count = cur.fetchone()
        print('Nifty50 data count:', count)
        cur.execute("SELECT min(date), max(date) FROM nifty50")
        dates = cur.fetchone()
        print('Nifty50 date range:', dates)