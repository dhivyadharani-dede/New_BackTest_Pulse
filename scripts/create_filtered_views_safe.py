import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import fetch_sql_to_dict, execute_sql

SQL_DIR = Path(repo_root) / 'sql'

VIEW_DEFS = {
        'v_ha_big_filtered': {
                'depends': ['ha_big'],
                'sql': '''
DROP MATERIALIZED VIEW IF EXISTS public.v_ha_big_filtered CASCADE;
DROP VIEW IF EXISTS public.v_ha_big_filtered CASCADE;
CREATE MATERIALIZED VIEW public.v_ha_big_filtered AS
SELECT
    r.strategy_name,
    h.trade_date,
    h.candle_time,
    h.open,
    h.high,
    h.low,
    h.close,
    h.ha_open,
    h.ha_high,
    h.ha_low,
    h.ha_close
FROM public.ha_big h
JOIN public.runtime_strategy_config r
    ON h.trade_date >= r.from_date
 AND h.trade_date <= r.to_date;
'''
        },
        'v_ha_small_filtered': {
                'depends': ['ha_small'],
                'sql': """
DROP MATERIALIZED VIEW IF EXISTS public.v_ha_small_filtered CASCADE;
DROP VIEW IF EXISTS public.v_ha_small_filtered CASCADE;
CREATE MATERIALIZED VIEW public.v_ha_small_filtered AS
SELECT
    r.strategy_name,
    h.trade_date,
    h.candle_time,
    h.open,
    h.high,
    h.low,
    h.close,
    h.ha_open,
    h.ha_high,
    h.ha_low,
    h.ha_close
FROM public.ha_small h
JOIN public.runtime_strategy_config r
    ON h.trade_date >= r.from_date
 AND h.trade_date <= r.to_date;
"""
        },
        'v_ha_1m_filtered': {
                'depends': ['ha_1m'],
                'sql': """
DROP MATERIALIZED VIEW IF EXISTS public.v_ha_1m_filtered CASCADE;
DROP VIEW IF EXISTS public.v_ha_1m_filtered CASCADE;
CREATE MATERIALIZED VIEW public.v_ha_1m_filtered AS
SELECT
    r.strategy_name,
    h.trade_date,
    h.candle_time,
    h.open,
    h.high,
    h.low,
    h.close,
    h.ha_open,
    h.ha_high,
    h.ha_low,
    h.ha_close
FROM public.ha_1m h
JOIN public.runtime_strategy_config r
    ON h.trade_date >= r.from_date
 AND h.trade_date <= r.to_date;
"""
        },
        'v_nifty50_filtered': {
                'depends': ['"Nifty50"', 'nifty50'],
                'sql': """
DROP MATERIALIZED VIEW IF EXISTS public.v_nifty50_filtered CASCADE;
DROP VIEW IF EXISTS public.v_nifty50_filtered CASCADE;
CREATE MATERIALIZED VIEW public.v_nifty50_filtered AS
SELECT
    r.strategy_name,
    m.date,
    m.time,
    m.open,
    m.high,
    m.low,
    m.close,
    m.volume,
    m.oi,
    m.option_nm
FROM public."Nifty50" m
JOIN public.runtime_strategy_config r
    ON m.date >= r.from_date
 AND m.date <= r.to_date;
"""
        },
        'v_nifty_options_filtered': {
                'depends': ['nifty_options','"Nifty_options"'],
                'sql': """
DROP MATERIALIZED VIEW IF EXISTS public.v_nifty_options_filtered CASCADE;
DROP VIEW IF EXISTS public.v_nifty_options_filtered CASCADE;
CREATE MATERIALIZED VIEW public.v_nifty_options_filtered AS
SELECT
    r.strategy_name,
    o.*
FROM public."Nifty_options" o
JOIN public.runtime_strategy_config r
    ON o.date >= r.from_date
 AND o.date <= r.to_date;
"""
        },
    'v_mv_ha_big_candle_filtered': {
        'depends': ['mv_ha_big_candle'],
        'sql': """
CREATE OR REPLACE VIEW public.v_mv_ha_big_candle_filtered AS
SELECT r.strategy_name, mv.*
FROM public.mv_ha_big_candle mv
JOIN public.runtime_strategy_config r
  ON mv.trade_date >= r.from_date
 AND mv.trade_date <= r.to_date;
"""
    },
    'v_mv_ha_small_candle_filtered': {
        'depends': ['mv_ha_small_candle'],
        'sql': """
CREATE OR REPLACE VIEW public.v_mv_ha_small_candle_filtered AS
SELECT r.strategy_name, mv.*
FROM public.mv_ha_small_candle mv
JOIN public.runtime_strategy_config r
  ON mv.trade_date >= r.from_date
 AND mv.trade_date <= r.to_date;
"""
    },
    'v_mv_ha_1m_candle_filtered': {
        'depends': ['mv_ha_1m_candle'],
        'sql': """
CREATE OR REPLACE VIEW public.v_mv_ha_1m_candle_filtered AS
SELECT r.strategy_name, mv.*
FROM public.mv_ha_1m_candle mv
JOIN public.runtime_strategy_config r
  ON mv.trade_date >= r.from_date
 AND mv.trade_date <= r.to_date;
"""
    }
}


def table_exists(name: str) -> bool:
    # Use to_regclass to check table/view existence; name can be quoted
    q = "SELECT to_regclass(%s) IS NOT NULL AS exists"
    rows = list(fetch_sql_to_dict(q, {'1': name}))
    # fetch_sql_to_dict yields a list in a generator; adjust: use direct query string
    # fallback: try simple query
    try:
        res = list(fetch_sql_to_dict("SELECT to_regclass(%s) IS NOT NULL AS exists", (name,)))
    except Exception:
        # direct SQL string with name literal
        try:
            res = list(fetch_sql_to_dict(f"SELECT to_regclass('{name}') IS NOT NULL AS exists"))
        except Exception:
            return False
    if not res:
        return False
    # res may be list of rows
    row = res[0]
    if isinstance(row, list):
        row = row[0]
    # row may be dict
    if isinstance(row, dict):
        return row.get('exists', False)
    # fallback
    return bool(row)


def main():
    created = []
    for view_name, info in VIEW_DEFS.items():
        ok = False
        for dep in info['depends']:
            try:
                # check quoted and unquoted
                qres = list(fetch_sql_to_dict(f"SELECT to_regclass('{dep}') IS NOT NULL as exists"))
                if qres and isinstance(qres[0], list):
                    exists = qres[0][0]['exists'] if qres[0] else False
                elif qres and isinstance(qres[0], dict):
                    exists = qres[0].get('exists', False)
                else:
                    exists = False
            except Exception:
                exists = False
            if exists:
                ok = True
                break
        if not ok:
            print(f"Skipping {view_name}: no dependent table found ({info['depends']})")
            continue
        try:
            execute_sql(info['sql'])
            print(f"Created/updated view {view_name}")
            created.append(view_name)
        except Exception as e:
            print(f"Failed to create {view_name}: {e}")
            continue

    if not created:
        print('No views created; missing dependencies')
    else:
        print('Created views:', ', '.join(created))


if __name__ == '__main__':
    main()
