-- Materialized view: hedge reentry closed legs
DROP MATERIALIZED VIEW IF EXISTS mv_hedge_reentry_closed_legs CASCADE;
CREATE MATERIALIZED VIEW mv_hedge_reentry_closed_legs AS
SELECT *
FROM mv_hedge_reentry_exit_on_all_entry_sl

UNION ALL

SELECT *
FROM mv_hedge_reentry_exit_on_partial_conditions

ORDER BY
    trade_date,
    expiry_date,
    entry_round,
    exit_time;