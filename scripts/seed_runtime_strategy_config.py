import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import execute_sql


def main():
    sql = '''
    INSERT INTO public.runtime_strategy_config (
        strategy_name,
        big_candle_tf,
        small_candle_tf,
        entry_candle,
        preferred_breakout_type,
        reentry_breakout_type,
        breakout_threshold_pct,
        sl_type,
        sl_percentage,
        box_sl_trigger_pct,
        box_sl_hard_pct,
        width_sl_pct,
        switch_pct,
        num_entry_legs,
        num_hedge_legs,
        option_entry_price_cap,
        hedge_entry_price_cap,
        hedge_exit_entry_ratio,
        hedge_exit_multiplier,
        leg_profit_pct,
        portfolio_profit_target_pct,
        portfolio_stop_loss_pct,
        portfolio_capital,
        no_of_lots,
        lot_size,
        max_reentry_rounds,
        eod_time,
        from_date,
        to_date
    )
    SELECT
        strategy_name,
        big_candle_tf,
        small_candle_tf,
        entry_candle,
        preferred_breakout_type,
        reentry_breakout_type,
        breakout_threshold_pct / 100.0,
        sl_type,
        sl_percentage / 100.0,
        box_sl_trigger_pct / 100.0,
        box_sl_hard_pct / 100.0,
        width_sl_pct / 100.0,
        switch_pct / 100.0,
        num_entry_legs,
        num_hedge_legs,
        option_entry_price_cap,
        hedge_entry_price_cap,
        hedge_exit_entry_ratio / 100.0,
        hedge_exit_multiplier,
        leg_profit_pct / 100.0,
        portfolio_profit_target_pct / 100.0,
        portfolio_stop_loss_pct / 100.0,
        portfolio_capital,
        no_of_lots,
        lot_size,
        max_reentry_rounds,
        eod_time,
        from_date,
        to_date
    FROM public.strategy_settings
    WHERE strategy_name = 'default'
    ON CONFLICT (strategy_name) DO UPDATE SET
        big_candle_tf = EXCLUDED.big_candle_tf,
        small_candle_tf = EXCLUDED.small_candle_tf,
        entry_candle = EXCLUDED.entry_candle,
        preferred_breakout_type = EXCLUDED.preferred_breakout_type,
        reentry_breakout_type = EXCLUDED.reentry_breakout_type,
        breakout_threshold_pct = EXCLUDED.breakout_threshold_pct,
        sl_type = EXCLUDED.sl_type,
        sl_percentage = EXCLUDED.sl_percentage,
        box_sl_trigger_pct = EXCLUDED.box_sl_trigger_pct,
        box_sl_hard_pct = EXCLUDED.box_sl_hard_pct,
        width_sl_pct = EXCLUDED.width_sl_pct,
        switch_pct = EXCLUDED.switch_pct,
        num_entry_legs = EXCLUDED.num_entry_legs,
        num_hedge_legs = EXCLUDED.num_hedge_legs,
        option_entry_price_cap = EXCLUDED.option_entry_price_cap,
        hedge_entry_price_cap = EXCLUDED.hedge_entry_price_cap,
        hedge_exit_entry_ratio = EXCLUDED.hedge_exit_entry_ratio,
        hedge_exit_multiplier = EXCLUDED.hedge_exit_multiplier,
        leg_profit_pct = EXCLUDED.leg_profit_pct,
        portfolio_profit_target_pct = EXCLUDED.portfolio_profit_target_pct,
        portfolio_stop_loss_pct = EXCLUDED.portfolio_stop_loss_pct,
        portfolio_capital = EXCLUDED.portfolio_capital,
        no_of_lots = EXCLUDED.no_of_lots,
        lot_size = EXCLUDED.lot_size,
        max_reentry_rounds = EXCLUDED.max_reentry_rounds,
        eod_time = EXCLUDED.eod_time,
        from_date = EXCLUDED.from_date,
        to_date = EXCLUDED.to_date;
    '''
    execute_sql(sql)
    print('Seeded runtime_strategy_config from strategy_settings (default)')


if __name__ == '__main__':
    main()
