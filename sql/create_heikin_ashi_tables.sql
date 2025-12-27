-- Create parent Heikin-Ashi partitioned tables
CREATE TABLE IF NOT EXISTS public.ha_big (
  trade_date date NOT NULL,
  candle_time time without time zone NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  ha_open numeric,
  ha_high numeric,
  ha_low numeric,
  ha_close numeric
) PARTITION BY RANGE (trade_date);

CREATE TABLE IF NOT EXISTS public.ha_small (
  trade_date date NOT NULL,
  candle_time time without time zone NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  ha_open numeric,
  ha_high numeric,
  ha_low numeric,
  ha_close numeric
) PARTITION BY RANGE (trade_date);

CREATE TABLE IF NOT EXISTS public.ha_1m (
  trade_date date NOT NULL,
  candle_time time without time zone NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  ha_open numeric,
  ha_high numeric,
  ha_low numeric,
  ha_close numeric
) PARTITION BY RANGE (trade_date);

-- recommended indexes on parent (note: actual indexes created per-partition)
-- CREATE INDEX IF NOT EXISTS idx_ha_big_trade_time ON public.ha_big (trade_date, candle_time);
