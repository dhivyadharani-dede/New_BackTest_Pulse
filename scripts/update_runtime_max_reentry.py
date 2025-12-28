from pathlib import Path
import sys
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import execute_sql

def main():
    sql = '''
    INSERT INTO public.runtime_strategy_config (strategy_name, max_reentry_rounds)
    VALUES ('default', 3)
    ON CONFLICT (strategy_name) DO UPDATE SET max_reentry_rounds = EXCLUDED.max_reentry_rounds;
    '''
    execute_sql(sql)
    print('Updated runtime_strategy_config: max_reentry_rounds=3 for default')

if __name__ == '__main__':
    main()
