-- Alter column defaults and update existing default strategy row
ALTER TABLE public.strategy_settings
    ALTER COLUMN box_sl_trigger_pct SET DEFAULT 25,
    ALTER COLUMN box_sl_hard_pct SET DEFAULT 35,
    ALTER COLUMN width_sl_pct SET DEFAULT 40;

-- Update existing row for default strategy
UPDATE public.strategy_settings
SET box_sl_trigger_pct = 25,
    box_sl_hard_pct = 35,
    width_sl_pct = 40,
    sl_percentage = 20,
    switch_pct = 20
WHERE strategy_name = 'default';

-- Show updated row
SELECT strategy_name, sl_percentage, box_sl_trigger_pct, box_sl_hard_pct, width_sl_pct, switch_pct
FROM public.strategy_settings
WHERE strategy_name = 'default';
