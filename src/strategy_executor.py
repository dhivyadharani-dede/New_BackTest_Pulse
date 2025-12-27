from pathlib import Path
import pandas as pd
from .db import fetch_sql_to_dict, get_conn


def load_sql(path: str) -> str:
    p = Path(path)
    return p.read_text()


def run_strategy_sql(sql_text: str, params: dict | None = None) -> pd.DataFrame:
    """Run user-provided SQL on Postgres and return a pandas DataFrame of results.

    The SQL should return rows ordered by timestamp. Columns expected: `ts`, `symbol`, `signal`, `price`, optional `size`.
    """
    # For typical use we expect the SQL to return all signals; use a single fetch for now.
    # If the SQL returns many millions of rows, call with chunksize in db.fetch_sql_to_dict.
    rows_gen = fetch_sql_to_dict(sql_text, params=params, chunksize=None)
    rows = next(rows_gen, [])
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
