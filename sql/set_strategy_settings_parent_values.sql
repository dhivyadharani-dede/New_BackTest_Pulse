-- Update parent strategy_settings with whole-number percentage values
UPDATE public.strategy_settings
SET sl_percentage = 20,
    box_sl_trigger_pct = 25,
    box_sl_hard_pct = 35,
    width_sl_pct = 40,
    switch_pct = 20
WHERE strategy_name = 'default';

-- Show updated row
SELECT strategy_name, sl_percentage, box_sl_trigger_pct, box_sl_hard_pct, width_sl_pct, switch_pct
FROM public.strategy_settings
WHERE strategy_name = 'default';
