-- view exposing runtime config for easy joins

DROP VIEW IF EXISTS public.v_strategy_config CASCADE;
CREATE VIEW public.v_strategy_config AS
SELECT * FROM runtime_strategy_config;