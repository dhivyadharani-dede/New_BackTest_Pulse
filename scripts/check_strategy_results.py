#!/usr/bin/env python3
"""
Check script for strategy_run_results table.

Verifies that for each strategy, the number of entry and hedge legs matches
the configured num_entry_legs and num_hedge_legs from strategy_settings.

Checks for each (strategy_name, trade_date, expiry_date, entry_round):
- Count of leg_type = 'entry' should equal num_entry_legs
- Count of leg_type = 'hedge' should equal num_hedge_legs

Reports any discrepancies.
"""

import sys
from pathlib import Path
import pandas as pd

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import get_conn

def check_strategy_results():
    query = """
    WITH leg_counts AS (
        SELECT
            s.strategy_name,
            s.num_entry_legs,
            s.num_hedge_legs,
            r.trade_date,
            r.expiry_date,
            r.entry_round,
            r.leg_type,
            COUNT(*) as actual_count
        FROM strategy_run_results r
        JOIN strategy_settings s ON r.strategy_name = s.strategy_name
        GROUP BY s.strategy_name, s.num_entry_legs, s.num_hedge_legs,
                 r.trade_date, r.expiry_date, r.entry_round, r.leg_type
    ),
    expected AS (
        SELECT
            strategy_name,
            trade_date,
            expiry_date,
            entry_round,
            'entry' as leg_type,
            num_entry_legs as expected_count
        FROM leg_counts
        WHERE leg_type = 'entry'
        UNION ALL
        SELECT
            strategy_name,
            trade_date,
            expiry_date,
            entry_round,
            'hedge' as leg_type,
            num_hedge_legs as expected_count
        FROM leg_counts
        WHERE leg_type = 'hedge'
    ),
    discrepancies AS (
        SELECT
            lc.strategy_name,
            lc.trade_date,
            lc.expiry_date,
            lc.entry_round,
            lc.leg_type,
            lc.actual_count,
            e.expected_count
        FROM leg_counts lc
        JOIN expected e ON lc.strategy_name = e.strategy_name
                          AND lc.trade_date = e.trade_date
                          AND lc.expiry_date = e.expiry_date
                          AND lc.entry_round = e.entry_round
                          AND lc.leg_type = e.leg_type
        WHERE lc.actual_count != e.expected_count
    )
    SELECT * FROM discrepancies
    ORDER BY strategy_name, trade_date, entry_round, leg_type;
    """

    with get_conn() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        print("✅ All strategy results have correct leg counts!")
        return True
    else:
        print("❌ Found discrepancies in leg counts:")
        print(df.to_string(index=False))
        return False

if __name__ == '__main__':
    success = check_strategy_results()
    sys.exit(0 if success else 1)