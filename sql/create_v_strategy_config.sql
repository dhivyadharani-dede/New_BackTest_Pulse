-- view exposing runtime config for easy joins
CREATE OR REPLACE VIEW public.v_strategy_config AS
SELECT
  strategy_name,
  COALESCE(big_candle_tf, 15) AS big_candle_tf,
  COALESCE(small_candle_tf, 5) AS small_candle_tf,
  COALESCE(entry_candle, 1) AS entry_candle,
  COALESCE(breakout_threshold_pct, 0.6) AS breakout_threshold_pct,
  COALESCE(switch_pct, 0.2) AS switch_pct
FROM public.runtime_strategy_config;
