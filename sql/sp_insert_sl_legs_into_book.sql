DELETE FROM strategy_leg_book;
CREATE OR REPLACE PROCEDURE insert_sl_legs_into_book(p_strategy_name TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
   -- DELETE FROM strategy_leg_book;
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
    SELECT 
        p_strategy_name,
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
    WHERE NOT EXISTS (
        SELECT 1
        FROM strategy_leg_book b
        WHERE b.strategy_name = p_strategy_name
          AND b.trade_date = sl.trade_date
          AND b.expiry_date = sl.expiry_date
          AND b.option_type = sl.option_type
          AND b.strike = sl.strike
          AND b.entry_round = sl.entry_round
          AND b.leg_type = sl.leg_type
    );

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