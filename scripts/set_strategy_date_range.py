import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import execute_sql


def main():
    sql = '''
    UPDATE public.strategy_settings
    SET from_date = %(from_date)s,
        to_date   = %(to_date)s
    WHERE strategy_name = %(name)s;
    '''
    params = {
        'from_date': '2025-04-01',
        'to_date': '2025-05-01',
        'name': 'default'
    }
    execute_sql(sql, params)
    print(f"strategy_settings updated for {params['name']}: {params['from_date']} -> {params['to_date']}")


if __name__ == '__main__':
    main()
