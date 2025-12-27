import os
import sys
from pathlib import Path

# ensure repo root on sys.path so `from src` works when running script directly
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import execute_sql


def main():
    name = os.getenv("DEFAULT_STRATEGY_NAME", "default")

    sql = """
    INSERT INTO public.strategy_settings (strategy_name)
    VALUES (%(name)s)
    ON CONFLICT (strategy_name) DO NOTHING;
    """

    execute_sql(sql, {"name": name})
    print(f"Ensured strategy_settings row exists for '{name}'")


if __name__ == "__main__":
    main()
