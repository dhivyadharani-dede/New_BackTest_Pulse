--DELETE FROM strategy_leg_book;
CREATE OR REPLACE PROCEDURE insert_sl_legs_into_book(p_strategy_name TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM strategy_leg_book ;--WHERE strategy_name = p_strategy_name;
    INSERT INTO strategy_leg_book (
        strategy_name,
        trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        exit_time,
        option_type,
        strike,
        entry_price,
        exit_price,
        transaction_type,
        leg_type,
        entry_round,
        exit_reason
    )
    SELECT DISTINCT ON (
    p_strategy_name    ,
    trade_date,
    expiry_date,
    option_type,
    strike,
    entry_round,
    leg_type
)
    p_strategy_name        AS strategy_name,
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    exit_time,
    option_type,
    strike,
    entry_price,
    exit_price,
    transaction_type,
    leg_type,
    entry_round,
    exit_reason
FROM mv_all_legs_round1 sl
ORDER BY
p_strategy_name,
    trade_date,
    expiry_date,
    option_type,
    strike,
    entry_round,
    leg_type,
    exit_time;

    RAISE NOTICE '✅ SL legs inserted into strategy_leg_book for strategy %', p_strategy_name;
END;
$$;


DO $$
DECLARE
    strat_name TEXT;
BEGIN
    SELECT strategy_name INTO strat_name FROM v_strategy_config LIMIT 1;
    CALL insert_sl_legs_into_book(strat_name);
END $$;