CREATE OR REPLACE PROCEDURE sp_run_reentry_loop(p_strategy_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_max_rounds        INT;
    v_current_round     INT;
    v_inserted_rows     INT;
BEGIN
    /* =====================================================
       1. Load max re-entry rounds
       ===================================================== */
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

    /* =====================================================
       2. Re-entry loop
       ===================================================== */
    LOOP
        /* Current highest round already inserted */
        SELECT COALESCE(MAX(entry_round), 1)
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

        /* =================================================
           3. REFRESH ONLY RE-ENTRY VIEWS
           ================================================= */

        -- Breakout detection
        REFRESH MATERIALIZED VIEW mv_ranked_breakouts_with_rounds_for_reentry;
        REFRESH MATERIALIZED VIEW mv_reentry_triggered_breakouts;

        -- Strike selection & leg creation
        REFRESH MATERIALIZED VIEW mv_reentry_base_strike_selection;
        REFRESH MATERIALIZED VIEW mv_reentry_breakout_context;
        REFRESH MATERIALIZED VIEW mv_reentry_legs_and_hedge_legs;

        -- Price streams & context
        REFRESH MATERIALIZED VIEW mv_reentry_live_prices;
       

        -- SL detection
        REFRESH MATERIALIZED VIEW mv_reentry_sl_hits;
        REFRESH MATERIALIZED VIEW mv_reentry_sl_executions;
        REFRESH MATERIALIZED VIEW mv_reentry_open_legs;
        REFRESH MATERIALIZED VIEW mv_reentry_profit_booking;
        REFRESH MATERIALIZED VIEW mv_reentry_eod_close;
        REFRESH MATERIALIZED VIEW mv_reentry_final_exit;
        REFRESH MATERIALIZED VIEW mv_reentry_legs_stats;

        REFRESH MATERIALIZED VIEW mv_hedge_reentry_exit_on_all_entry_sl;
        REFRESH MATERIALIZED VIEW mv_hedge_reentry_exit_on_partial_conditions;
        REFRESH MATERIALIZED VIEW mv_hedge_reentry_eod_exit;

        -- Re-hedge chain
        REFRESH MATERIALIZED VIEW mv_rehedge_trigger_reentry;
        REFRESH MATERIALIZED VIEW mv_rehedge_candidate_reentry;
        REFRESH MATERIALIZED VIEW mv_rehedge_selected_reentry;
        REFRESH MATERIALIZED VIEW mv_rehedge_leg_reentry;
        REFRESH MATERIALIZED VIEW mv_rehedge_eod_exit_reentry;

        -- Profit / EOD / double-buy
        REFRESH MATERIALIZED VIEW mv_reentry_profit_booking;
        REFRESH MATERIALIZED VIEW mv_reentry_eod_close;
        REFRESH MATERIALIZED VIEW mv_double_buy_legs_reentry;

        -- FINAL CONSOLIDATION
        REFRESH MATERIALIZED VIEW mv_all_legs_reentry;

        /* =================================================
           4. Insert ONLY consolidated re-entry legs
           ================================================= */
        INSERT INTO strategy_leg_book (
            strategy_name,
            trade_date,
            expiry_date,
            breakout_time,
            entry_time,
            exit_time,
          --  spot_price,
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
            r.trade_date,
            r.expiry_date,
            r.breakout_time,
            r.entry_time,
            r.exit_time,
         --   r.spot_price,
            r.option_type,
            r.strike,
            r.entry_price,
            r.exit_price,
            r.transaction_type,
            r.leg_type,
            r.entry_round,
            r.exit_reason
        FROM mv_all_legs_reentry r
        WHERE r.entry_round = v_current_round + 1
          AND NOT EXISTS (
              SELECT 1
              FROM strategy_leg_book b
              WHERE b.strategy_name = p_strategy_name
                AND b.trade_date    = r.trade_date
                AND b.expiry_date   = r.expiry_date
                AND b.option_type   = r.option_type
                AND b.strike        = r.strike
                AND b.entry_round   = r.entry_round
                AND b.leg_type      = r.leg_type
          );

        GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

 
   
        INSERT INTO strategy_leg_book (
            strategy_name,
            trade_date,
            expiry_date,
            breakout_time,
            entry_time,
            exit_time,
          --  spot_price,
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
            r.trade_date,
            r.expiry_date,
            r.breakout_time,
            r.entry_time,
            r.exit_time,
         --   r.spot_price,
            r.option_type,
            r.strike,
            r.entry_price,
            r.exit_price,
            r.transaction_type,
            r.leg_type,
            r.entry_round,
            r.exit_reason
        FROM mv_all_legs_reentry r
        WHERE r.entry_round = v_current_round + 1
          AND NOT EXISTS (
              SELECT 1
              FROM strategy_leg_book b
              WHERE b.strategy_name = p_strategy_name
                AND b.trade_date    = r.trade_date
                AND b.expiry_date   = r.expiry_date
                AND b.option_type   = r.option_type
                AND b.strike        = r.strike
                AND b.entry_round   = r.entry_round
                AND b.leg_type      = r.leg_type
          );

        GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

        /* =================================================
           5. Stop if nothing new is generated
           ================================================= */
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

EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Error: %', SQLERRM;
        RAISE NOTICE
            'Re-entry loop aborted for strategy %',
            p_strategy_name;
END;
$$;

CALL sp_run_reentry_loop('default');
