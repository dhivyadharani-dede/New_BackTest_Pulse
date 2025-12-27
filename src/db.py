import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager


def _conn_params_from_env():
    return dict(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", 5432)),
        dbname=os.getenv("PGDATABASE", os.getenv("PGDB", "Backtest_Pulse")),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "Alliswell@28"),
    )


@contextmanager
def get_conn(dict_cursor: bool = True):
    """Yield a psycopg2 connection. Close on exit.

    Uses environment variables: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
    """
    params = _conn_params_from_env()
    conn = psycopg2.connect(**params)
    try:
        yield conn
    finally:
        conn.close()


def fetch_sql_to_dict(sql: str, params: dict | None = None, chunksize: int | None = None):
    """Execute SQL and return a generator of row lists (as dicts) or one list if chunksize is None.

    If `chunksize` is provided, a server-side cursor is used to fetch in batches.
    """
    with get_conn() as conn:
        if chunksize:
            cur = conn.cursor("server_side_cursor", cursor_factory=RealDictCursor)
            cur.itersize = chunksize
            cur.execute(sql, params or {})
            while True:
                rows = cur.fetchmany(chunksize)
                if not rows:
                    break
                yield rows
            cur.close()
        else:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params or {})
                rows = cur.fetchall()
                yield rows


def execute_sql(sql: str, params: dict | None = None):
    """Execute a SQL statement (DDL/DML). Commits automatically."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
        conn.commit()
