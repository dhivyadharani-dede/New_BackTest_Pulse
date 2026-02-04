-- Materialized view: rehedge selected reentry
DROP MATERIALIZED VIEW IF EXISTS mv_rehedge_selected_reentry CASCADE;
CREATE MATERIALIZED VIEW mv_rehedge_selected_reentry AS
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trade_date, expiry_date, entry_round
            ORDER BY option_time, premium_diff
        ) AS rn
    FROM mv_rehedge_candidate_reentry
) x
WHERE rn = 1;