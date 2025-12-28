-- Upsert values into runtime_strategy_config for the default strategy
INSERT INTO public.runtime_strategy_config (
    strategy_name,
    sl_percentage,
    box_sl_trigger_pct,
    box_sl_hard_pct,
    width_sl_pct,
    switch_pct
)
VALUES (
    'default',
    20,
    25,
    35,
    40,
    20
)
ON CONFLICT (strategy_name) DO UPDATE SET
    sl_percentage = EXCLUDED.sl_percentage,
    box_sl_trigger_pct = EXCLUDED.box_sl_trigger_pct,
    box_sl_hard_pct = EXCLUDED.box_sl_hard_pct,
    width_sl_pct = EXCLUDED.width_sl_pct,
    switch_pct = EXCLUDED.switch_pct;

-- Show runtime row
SELECT strategy_name, sl_percentage, box_sl_trigger_pct, box_sl_hard_pct, width_sl_pct, switch_pct
FROM public.runtime_strategy_config
WHERE strategy_name = 'default';
