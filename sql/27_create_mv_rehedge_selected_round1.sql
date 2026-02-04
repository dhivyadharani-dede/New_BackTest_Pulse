-- Materialized view: rehedge selected (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_selected_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_rehedge_selected_round1 AS
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trade_date, expiry_date, entry_round
            ORDER BY option_time, premium_diff
        ) AS rn
    FROM mv_rehedge_candidate_round1
) x
WHERE rn = 1;

CREATE INDEX IF NOT EXISTS idx_mv_rehedge_selected_round1_date ON public.mv_rehedge_selected_round1 (trade_date, expiry_date, entry_round);
