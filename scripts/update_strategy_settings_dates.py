import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import execute_sql


def main():
    sql = '''
    ALTER TABLE public.strategy_settings
    ADD COLUMN IF NOT EXISTS from_date DATE,
    ADD COLUMN IF NOT EXISTS to_date DATE;

    UPDATE public.strategy_settings
    SET from_date = '2025-01-01',
        to_date = CURRENT_DATE
    WHERE from_date IS NULL OR to_date IS NULL;

    ALTER TABLE public.strategy_settings
    ALTER COLUMN from_date SET NOT NULL,
    ALTER COLUMN to_date SET NOT NULL;
    '''
    execute_sql(sql)
    print("strategy_settings updated: added from_date/to_date and populated")


if __name__ == '__main__':
    main()
