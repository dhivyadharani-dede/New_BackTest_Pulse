from pathlib import Path
import sys
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import execute_sql
import subprocess


def main():
    # 1) Update parent strategy_settings
    sql_update = '''
    INSERT INTO public.strategy_settings (strategy_name, max_reentry_rounds)
    VALUES ('default', 3)
    ON CONFLICT (strategy_name) DO UPDATE SET max_reentry_rounds = EXCLUDED.max_reentry_rounds;
    '''
    execute_sql(sql_update)
    print('Updated strategy_settings: max_reentry_rounds=3')

    # 2) Seed runtime_strategy_config from strategy_settings
    print('Seeding runtime_strategy_config from strategy_settings (running script)')
    subprocess.check_call([sys.executable, str(repo_root / 'scripts' / 'seed_runtime_strategy_config.py')])

    # 3) Refresh v_strategy_config materialized view
    print('Refreshing materialized view: public.v_strategy_config')
    execute_sql('REFRESH MATERIALIZED VIEW public.v_strategy_config')
    print('Refreshed public.v_strategy_config')

    # 4) Refresh all dependent matviews sequentially
    print('Running sequential refresh of matviews')
    subprocess.check_call([sys.executable, str(repo_root / 'scripts' / 'refresh_matviews_sequential.py')])

    print('All done.')


if __name__ == '__main__':
    main()
