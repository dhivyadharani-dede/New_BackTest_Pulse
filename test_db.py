from src.db import get_conn
print('Testing connection...')
try:
    with get_conn() as conn:
        print('Connection successful')
except Exception as e:
    print(f'Connection failed: {e}')