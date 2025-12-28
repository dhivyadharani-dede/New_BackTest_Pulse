-- Create runtime_strategy_config table for dynamic multi-strategy runs
DROP TABLE IF EXISTS public.runtime_strategy_config;

CREATE TABLE public.runtime_strategy_config (
    strategy_name TEXT PRIMARY KEY,

    big_candle_tf INT,
    small_candle_tf INT,
    entry_candle INT,

    preferred_breakout_type TEXT,
    reentry_breakout_type TEXT,
    breakout_threshold_pct NUMERIC,

    sl_type TEXT,
    sl_percentage NUMERIC,
    box_sl_trigger_pct NUMERIC,
    box_sl_hard_pct NUMERIC,
    width_sl_pct NUMERIC,
    switch_pct NUMERIC,

    num_entry_legs INT,
    num_hedge_legs INT,
    option_entry_price_cap NUMERIC,
    hedge_entry_price_cap NUMERIC,
    hedge_exit_entry_ratio NUMERIC,
    hedge_exit_multiplier NUMERIC,

    leg_profit_pct NUMERIC,
    portfolio_profit_target_pct NUMERIC,
    portfolio_stop_loss_pct NUMERIC,

    portfolio_capital NUMERIC,
    no_of_lots INT,
    lot_size INT,

    max_reentry_rounds INT,
    eod_time TIME,

    from_date DATE,
    to_date   DATE
);
