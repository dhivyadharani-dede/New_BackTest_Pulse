-- Create parent partitioned table for Nifty_options
-- Using TEXT for variable-length string fields for flexibility
CREATE TABLE IF NOT EXISTS public."Nifty_options"
(
    symbol text,
    date date,
    expiry date,
    strike numeric,
    option_type text,
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
