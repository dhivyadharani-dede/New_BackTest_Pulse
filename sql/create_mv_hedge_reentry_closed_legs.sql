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