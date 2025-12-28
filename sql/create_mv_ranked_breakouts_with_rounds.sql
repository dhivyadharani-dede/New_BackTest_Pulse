-- Create ranked breakouts with entry rounds
DROP MATERIALIZED VIEW IF EXISTS public.mv_ranked_breakouts_with_rounds CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_ranked_breakouts_with_rounds AS
WITH filtered AS (
    SELECT
        b.trade_date,
        b.breakout_time,
        b.breakout_type
    FROM public.mv_all_5min_breakouts b
    JOIN public.v_strategy_config s ON TRUE
    WHERE
        (
            s.preferred_breakout_type = 'full_candle_breakout'
            AND b.breakout_type IN ('full_body_bullish', 'full_body_bearish')
        )
        OR
        (
            s.preferred_breakout_type = 'pct_based_breakout'
            AND b.breakout_type IN (
                'pct_breakout_bullish',
                'pct_breakout_bearish',
                'full_body_bullish',
                'full_body_bearish'
            )
        )
),
ranked AS (
    SELECT
        trade_date,
        breakout_time,
        breakout_type,
        ROW_NUMBER() OVER (
            PARTITION BY trade_date
            ORDER BY breakout_time
        ) AS entry_round
    FROM filtered
)
SELECT
    trade_date,
    breakout_time,
    breakout_time + INTERVAL '5 minute' AS entry_time,
    breakout_type,
    CASE
        WHEN breakout_type IN ('full_body_bullish', 'pct_breakout_bullish')
        THEN 'P'
        ELSE 'C'
    END AS entry_option_type,
    entry_round
FROM ranked;

-- index to speed lookups
CREATE INDEX IF NOT EXISTS idx_mv_ranked_breakouts_date_time ON public.mv_ranked_breakouts_with_rounds (trade_date, breakout_time);
