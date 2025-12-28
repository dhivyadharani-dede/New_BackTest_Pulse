import sys
from pathlib import Path
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import get_conn

if len(sys.argv) < 2:
    print('Usage: python apply_sql_raw.py path/to/file.sql')
    sys.exit(1)

p = Path(sys.argv[1])
if not p.exists():
    print('File not found:', p)
    sys.exit(1)

sql = p.read_text(encoding='utf-8')
with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
print('Executed', p)
