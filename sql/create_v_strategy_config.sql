-- view exposing runtime config for easy joins
DROP VIEW IF EXISTS public.v_strategy_config CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.v_strategy_config CASCADE;
CREATE MATERIALIZED VIEW public.v_strategy_config AS
SELECT
  r.strategy_name,

  COALESCE(r.big_candle_tf, s.big_candle_tf, 15) AS big_candle_tf,
  COALESCE(r.small_candle_tf, s.small_candle_tf, 5) AS small_candle_tf,
  COALESCE(r.entry_candle, s.entry_candle, 1) AS entry_candle,

  COALESCE(r.preferred_breakout_type, s.preferred_breakout_type) AS preferred_breakout_type,
  COALESCE(r.reentry_breakout_type, s.reentry_breakout_type) AS reentry_breakout_type,
  COALESCE(r.breakout_threshold_pct, s.breakout_threshold_pct/100.0) AS breakout_threshold_pct,

  COALESCE(r.sl_type, s.sl_type) AS sl_type,
  COALESCE(r.sl_percentage, s.sl_percentage) AS sl_percentage,
  COALESCE(r.box_sl_trigger_pct, s.box_sl_trigger_pct) AS box_sl_trigger_pct,
  COALESCE(r.box_sl_hard_pct, s.box_sl_hard_pct) AS box_sl_hard_pct,
  COALESCE(r.width_sl_pct, s.width_sl_pct) AS width_sl_pct,
  COALESCE(r.switch_pct, s.switch_pct) AS switch_pct,

  COALESCE(r.num_entry_legs, s.num_entry_legs) AS num_entry_legs,
  COALESCE(r.num_hedge_legs, s.num_hedge_legs) AS num_hedge_legs,
  COALESCE(r.option_entry_price_cap, s.option_entry_price_cap) AS option_entry_price_cap,
  COALESCE(r.hedge_entry_price_cap, s.hedge_entry_price_cap) AS hedge_entry_price_cap,
  COALESCE(r.hedge_exit_entry_ratio, s.hedge_exit_entry_ratio/100.0) AS hedge_exit_entry_ratio,
  COALESCE(r.hedge_exit_multiplier, s.hedge_exit_multiplier) AS hedge_exit_multiplier,

  COALESCE(r.leg_profit_pct, s.leg_profit_pct/100.0) AS leg_profit_pct,
  COALESCE(r.portfolio_profit_target_pct, s.portfolio_profit_target_pct/100.0) AS portfolio_profit_target_pct,
  COALESCE(r.portfolio_stop_loss_pct, s.portfolio_stop_loss_pct/100.0) AS portfolio_stop_loss_pct,

  COALESCE(r.portfolio_capital, s.portfolio_capital) AS portfolio_capital,
  COALESCE(r.no_of_lots, s.no_of_lots) AS no_of_lots,
  COALESCE(r.lot_size, s.lot_size) AS lot_size,

  COALESCE(r.max_reentry_rounds, s.max_reentry_rounds) AS max_reentry_rounds,
  COALESCE(r.eod_time, s.eod_time) AS eod_time,

  COALESCE(r.from_date, s.from_date) AS v_from_date,
  COALESCE(r.to_date, s.to_date) AS v_to_date
FROM public.runtime_strategy_config r
LEFT JOIN public.strategy_settings s
  ON r.strategy_name = s.strategy_name;
