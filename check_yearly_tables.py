from src.db import get_conn

with get_conn() as conn:
    with conn.cursor() as cur:
        # Get all Nifty50 yearly tables
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'Nifty50_y%' ORDER BY table_name")
        tables = cur.fetchall()
        print('Nifty50 yearly tables:')
        for table in tables:
            table_name = table[0]
            print(f' - {table_name}')
            
            # Get date range for each table
            try:
                cur.execute(f'SELECT MIN(date), MAX(date) FROM "{table_name}"')
                date_range = cur.fetchone()
                print(f'   Date range: {date_range[0]} to {date_range[1]}')
            except Exception as e:
                print(f'   Error getting date range: {e}')