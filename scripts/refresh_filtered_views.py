import sys
from pathlib import Path
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import get_conn

def main():
    views_to_refresh = [
        'v_ha_big_filtered',
        'v_ha_small_filtered',
        'v_ha_1m_filtered',
        'v_nifty50_filtered',
        'v_nifty_options_filtered'
    ]

    for mv in views_to_refresh:
        print(f"Refreshing materialized view: public.{mv}")
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f'REFRESH MATERIALIZED VIEW public."{mv}"')
                    conn.commit()
            print(f"Refreshed: public.{mv}")
        except Exception as e:
            print(f"Failed to refresh public.{mv}: {e}")

if __name__ == '__main__':
    main()