-- view exposing runtime config for easy joins

DROP MATERIALIZED VIEW IF EXISTS public.v_strategy_config CASCADE;
CREATE MATERIALIZED VIEW public.v_strategy_config AS
SELECT * FROM runtime_strategy_config;