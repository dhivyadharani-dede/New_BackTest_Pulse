-- Materialized views that cache per-strategy filtered rows using runtime_strategy_config
DROP MATERIALIZED VIEW IF EXISTS public.v_ha_big_filtered CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.v_ha_small_filtered CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.v_ha_1m_filtered CASCADE;

DROP MATERIALIZED VIEW IF EXISTS public.v_nifty50_filtered CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.v_nifty_options_filtered CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.v_ha_big_filtered AS
SELECT
  -- r.strategy_name,
  h.trade_date,
  h.candle_time,
  h.open,
  h.high,
  h.low,
  h.close,
  h.ha_open,
  h.ha_high,
  h.ha_low,
  h.ha_close
FROM public.ha_big h
WHERE  h.trade_date >= (SELECT from_date FROM runtime_strategy_dates LIMIT 1)
  AND  h.trade_date <= (SELECT to_date FROM runtime_strategy_dates LIMIT 1);

CREATE MATERIALIZED VIEW IF NOT EXISTS public.v_ha_small_filtered AS
SELECT
  -- r.strategy_name,
  h.trade_date,
  h.candle_time,
  h.open,
  h.high,
  h.low,
  h.close,
  h.ha_open,
  h.ha_high,
  h.ha_low,
  h.ha_close
FROM public.ha_small h
WHERE h.trade_date >= (SELECT from_date FROM runtime_strategy_dates LIMIT 1)
  AND h.trade_date <= (SELECT to_date FROM runtime_strategy_dates LIMIT 1);

CREATE MATERIALIZED VIEW IF NOT EXISTS public.v_ha_1m_filtered AS
SELECT
  -- r.strategy_name,
  h.trade_date,
  h.candle_time,
  h.open,
  h.high,
  h.low,
  h.close,
  h.ha_open,
  h.ha_high,
  h.ha_low,
  h.ha_close
FROM public.ha_1m h
WHERE h.trade_date >= (SELECT from_date FROM runtime_strategy_dates LIMIT 1)
  AND h.trade_date <= (SELECT to_date FROM runtime_strategy_dates LIMIT 1);

-- Source market data views
CREATE MATERIALIZED VIEW IF NOT EXISTS public.v_nifty50_filtered AS
SELECT
  -- r.strategy_name,
  m.date,
  m.time,
  m.open,
  m.high,
  m.low,
  m.close,
  m.volume,
  m.oi,
  m.option_nm
FROM public."Nifty50" m
WHERE m.date >= (SELECT from_date FROM runtime_strategy_dates LIMIT 1)
  AND m.date <= (SELECT to_date FROM runtime_strategy_dates LIMIT 1);

CREATE MATERIALIZED VIEW IF NOT EXISTS public.v_nifty_options_filtered AS
SELECT
  -- r.strategy_name,
  o.date,
  o.time,
  o.open,
  o.high,
  o.low,
  o.close,
  o.volume,
  o.oi,
  o.option_type,
  o.strike,
  o.expiry
FROM public."Nifty_options" o
WHERE o.date >= (SELECT from_date FROM runtime_strategy_dates LIMIT 1)
  AND o.date <= (SELECT to_date FROM runtime_strategy_dates LIMIT 1);

DROP INDEX IF EXISTS ux_v_ha_big_filtered;
CREATE UNIQUE INDEX ux_v_ha_big_filtered
ON public.v_ha_big_filtered (trade_date, candle_time);

DROP INDEX IF EXISTS ux_v_ha_small_filtered;
CREATE UNIQUE INDEX ux_v_ha_small_filtered
ON public.v_ha_small_filtered (trade_date, candle_time);

DROP INDEX IF EXISTS ux_v_ha_1m_filtered;
CREATE UNIQUE INDEX ux_v_ha_1m_filtered
ON public.v_ha_1m_filtered (trade_date, candle_time);

DROP INDEX IF EXISTS ux_v_nifty50_filtered;
CREATE UNIQUE INDEX ux_v_nifty50_filtered
ON public.v_nifty50_filtered (date, time);

DROP INDEX IF EXISTS ux_v_nifty_options_filtered;
CREATE UNIQUE INDEX ux_v_nifty_options_filtered
ON public.v_nifty_options_filtered
(date, expiry, option_type, strike, time);
