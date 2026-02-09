-- Materialized view for Nifty options filtered by runtime_strategy_config date ranges
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_nifty_options_filtered AS
SELECT
  r.strategy_name,
  o.date,
  o.expiry,
  o.time,
  o.open,
  o.high,
  o.low,
  o.close,
  o.volume,
  o.oi,
  o.option_type,
  o.strike
FROM public."Nifty_options" o
JOIN public.runtime_strategy_config r
  ON o.date >= r.from_date
 AND o.date <= r.to_date;

CREATE UNIQUE INDEX CONCURRENTLY ux_mv_nifty_options_filtered
ON mv_nifty_options_filtered
(date, expiry, option_type, strike, time);
