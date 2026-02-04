DO $$
DECLARE
    v_strategy_name TEXT;
BEGIN
    SELECT strategy_name
    INTO v_strategy_name
    FROM v_strategy_config
    LIMIT 1;

    CALL sp_run_reentry_loop(v_strategy_name);
END $$;