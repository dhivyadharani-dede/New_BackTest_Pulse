-- Create parent partitioned table for Nifty50
-- Note: original request used "option_nm \"char\""; using TEXT for flexibility.
CREATE TABLE IF NOT EXISTS public."Nifty50"
(
    date date,
    "time" time without time zone,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    oi numeric,
    option_nm text
)
PARTITION BY RANGE (date);

-- Example: create a default partition if desired (not created here).
