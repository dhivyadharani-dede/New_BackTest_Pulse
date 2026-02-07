CREATE OR REPLACE FUNCTION fn_run_reentry_loop(p_strategy_name TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_max_rounds    INT;
    v_current_round INT;
    v_inserted_rows INT;
BEGIN
    SELECT max_reentry_rounds
    INTO v_max_rounds
    FROM strategy_settings
    WHERE strategy_name = p_strategy_name;

    IF v_max_rounds IS NULL THEN
        RAISE EXCEPTION
            'No max_reentry_rounds found for strategy_name = %',
            p_strategy_name;
    END IF;

    RAISE NOTICE
        'Re-entry loop started for strategy %, max rounds = %',
        p_strategy_name, v_max_rounds;

    LOOP
        SELECT COALESCE(MAX(entry_round), 0)
        INTO v_current_round
        FROM strategy_leg_book
        WHERE strategy_name = p_strategy_name;

        IF v_current_round >= v_max_rounds THEN
            RAISE NOTICE
                'Reached max re-entry round %, stopping.',
                v_current_round;
            EXIT;
        END IF;

        RAISE NOTICE
            'Processing re-entry round %',
            v_current_round + 1;

        /* ===============================
           REFRESH RE-ENTRY VIEWS
           =============================== */

        REFRESH MATERIALIZED VIEW mv_ranked_breakouts_with_rounds_for_reentry;
        REFRESH MATERIALIZED VIEW mv_reentry_triggered_breakouts;
        REFRESH MATERIALIZED VIEW mv_reentry_base_strike_selection;
        REFRESH MATERIALIZED VIEW mv_reentry_breakout_context;
        REFRESH MATERIALIZED VIEW mv_reentry_legs_and_hedge_legs;

        TRUNCATE TABLE wrk_reentry_live_prices;

        INSERT INTO wrk_reentry_live_prices
        SELECT
            l.trade_date,
            l.expiry_date,
            l.breakout_time,
            l.entry_time,
            l.spot_price,
            l.option_type,
            l.strike,
            l.entry_price,
            l.entry_round,
            l.leg_type,
            l.transaction_type,
            o.time,
            o.high,
            o.open,
            o.close
        FROM mv_reentry_legs_and_hedge_legs l
        JOIN v_strategy_config s ON TRUE
        JOIN v_nifty_options_filtered o
          ON o.date = l.trade_date
         AND o.expiry = l.expiry_date
         AND o.option_type = l.option_type
         AND o.strike = l.strike
         AND o.time BETWEEN l.entry_time AND s.eod_time;

        REFRESH MATERIALIZED VIEW mv_reentry_sl_hits;
        REFRESH MATERIALIZED VIEW mv_reentry_sl_executions;
        REFRESH MATERIALIZED VIEW mv_reentry_open_legs;
        REFRESH MATERIALIZED VIEW mv_reentry_profit_booking;
        REFRESH MATERIALIZED VIEW mv_reentry_eod_close;
        REFRESH MATERIALIZED VIEW mv_reentry_final_exit;
        REFRESH MATERIALIZED VIEW mv_reentry_legs_stats;

        REFRESH MATERIALIZED VIEW mv_all_legs_reentry;

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
        FROM mv_all_legs_reentry
        ON CONFLICT DO NOTHING;

        GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

        -- COMMIT;  -- Removed: COMMIT not allowed inside stored procedures in transaction context

        IF v_inserted_rows = 0 THEN
            RAISE NOTICE
                'No re-entry legs generated for round %, stopping.',
                v_current_round + 1;
            EXIT;
        END IF;

        RAISE NOTICE
            'Inserted % re-entry legs for round %',
            v_inserted_rows,
            v_current_round + 1;
    END LOOP;

    RAISE NOTICE
        'Re-entry loop completed for strategy %',
        p_strategy_name;
END;
$$;


DO $$
DECLARE
    strat_name TEXT;
BEGIN
    SELECT strategy_name INTO strat_name FROM v_strategy_config LIMIT 1;
    PERFORM fn_run_reentry_loop(strat_name);
END $$;