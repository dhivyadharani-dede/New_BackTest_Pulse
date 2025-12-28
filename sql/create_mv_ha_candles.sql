-- Create materialized views for HA candle parents if they don't already exist
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_ha_big_candle AS
SELECT trade_date, candle_time, ha_open, ha_high, ha_low, ha_close
FROM public.ha_big;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_ha_small_candle AS
SELECT trade_date, candle_time, ha_open, ha_high, ha_low, ha_close
FROM public.ha_small;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_ha_1m_candle AS
SELECT trade_date, candle_time, ha_open, ha_high, ha_low, ha_close
FROM public.ha_1m;

CREATE INDEX IF NOT EXISTS idx_mv_ha_big_date_time ON public.mv_ha_big_candle (trade_date, candle_time);
CREATE INDEX IF NOT EXISTS idx_mv_ha_small_date_time ON public.mv_ha_small_candle (trade_date, candle_time);
CREATE INDEX IF NOT EXISTS idx_mv_ha_1m_date_time ON public.mv_ha_1m_candle (trade_date, candle_time);
