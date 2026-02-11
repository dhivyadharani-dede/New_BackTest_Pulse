CREATE MATERIALIZED VIEW mv_nifty_options_1m_filled AS
WITH all_minutes AS (
    SELECT
        d.date,
        generate_series(
            d.date + time '09:15',
            d.date + time '15:30',
            interval '1 minute'
        )::time AS time
    FROM (
        SELECT DISTINCT date
        FROM public."Nifty_options"
    ) d
),
option_keys AS (
    SELECT DISTINCT
        date,
        expiry,
        strike,
        option_type
    FROM public."Nifty_options"
),
expanded AS (
    SELECT
        k.date,
        k.expiry,
        k.strike,
        k.option_type,
        m.time,

        n.open,
        n.high,
        n.low,
        n.close
    FROM option_keys k
    JOIN all_minutes m
      ON m.date = k.date
    LEFT JOIN public."Nifty_options" n
      ON n.date        = k.date
     AND n.expiry      = k.expiry
     AND n.strike      = k.strike
     AND n.option_type = k.option_type
     AND n.time        = m.time
)
SELECT
    date,
    expiry,
    strike,
    option_type,
    time,

    last_value(open)  IGNORE NULLS OVER w AS open,
    last_value(high)  IGNORE NULLS OVER w AS high,
    last_value(low)   IGNORE NULLS OVER w AS low,
    last_value(close) IGNORE NULLS OVER w AS close,

    CASE WHEN open IS NULL THEN true ELSE false END AS is_filled
FROM expanded
WINDOW w AS (
    PARTITION BY date, expiry, strike, option_type
    ORDER BY time
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);
