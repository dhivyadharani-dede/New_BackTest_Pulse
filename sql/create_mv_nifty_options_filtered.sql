-- Materialized view for Nifty options filtered by runtime_strategy_config date ranges
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_nifty_options_filtered AS
SELECT
  r.strategy_name,
  o.date,
  o.time,
  o.open,
  o.high,
  o.low,
  o.close,
  o.volume,
  o.oi,
  o.option_type,
  o.strike_price
FROM public.Nifty_options o
JOIN public.runtime_strategy_config r
  ON o.date >= r.from_date
 AND o.date <= r.to_date;

CREATE INDEX IF NOT EXISTS idx_mv_nifty_options_filtered_date_time ON public.mv_nifty_options_filtered (date, time);
