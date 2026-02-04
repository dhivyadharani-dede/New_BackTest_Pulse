from src.db import get_conn

with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'Nifty50_y2024' ORDER BY ordinal_position")
        columns = cur.fetchall()
        print('Nifty50_y2024 columns:')
        for col in columns:
            print(' -', col[0])
        
        # Also check a sample row
        cur.execute("SELECT * FROM \"Nifty50_y2024\" LIMIT 1")
        row = cur.fetchone()
        print('\\nSample row:', row)