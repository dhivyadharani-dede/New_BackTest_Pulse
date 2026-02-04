from src.db import get_conn
with get_conn() as conn:
    with conn.cursor() as cur:
        try:
            cur.execute("CALL sp_run_strategy()")
            conn.commit()
            print('sp_run_strategy executed successfully')
        except Exception as e:
            print(f'Error: {e}')
            conn.rollback()