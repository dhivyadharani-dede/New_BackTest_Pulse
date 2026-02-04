CREATE OR REPLACE PROCEDURE sp_run_strategy()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_from_date DATE;
    v_to_date DATE;
BEGIN
    FOR rec IN SELECT * FROM strategy_settings ORDER BY strategy_name LOOP
        -- Truncate runtime config for each strategy
        TRUNCATE TABLE runtime_strategy_config;

        -- Resolve date range (using strategy's dates, no overrides)
        v_from_date := rec.from_date;
        v_to_date := rec.to_date;

        IF v_from_date IS NULL OR v_to_date IS NULL THEN
            RAISE EXCEPTION 'Date range not defined for strategy %', rec.strategy_name;
        END IF;

        IF v_from_date > v_to_date THEN
            RAISE EXCEPTION 'from_date (%) cannot be after to_date (%)', v_from_date, v_to_date;
        END IF;

        -- Insert into runtime_strategy_config
        INSERT INTO runtime_strategy_config (
            strategy_name,
            big_candle_tf,
            small_candle_tf,
            entry_candle,
            preferred_breakout_type,
            reentry_breakout_type,
            breakout_threshold_pct,
            sl_type,
            sl_percentage,
            box_sl_trigger_pct,
            box_sl_hard_pct,
            width_sl_pct,
            switch_pct,
            num_entry_legs,
            num_hedge_legs,
            option_entry_price_cap,
            hedge_entry_price_cap,
            hedge_exit_entry_ratio,
            hedge_exit_multiplier,
            leg_profit_pct,
            portfolio_profit_target_pct,
            portfolio_stop_loss_pct,
            portfolio_capital,
            no_of_lots,
            lot_size,
            max_reentry_rounds,
            eod_time,
            from_date,
            to_date
        ) VALUES (
            rec.strategy_name,
            rec.big_candle_tf,
            rec.small_candle_tf,
            rec.entry_candle,
            rec.preferred_breakout_type,
            rec.reentry_breakout_type,
            rec.breakout_threshold_pct / 100.0,
            rec.sl_type,
            rec.sl_percentage / 100.0,
            rec.box_sl_trigger_pct / 100.0,
            rec.box_sl_hard_pct / 100.0,
            rec.width_sl_pct / 100.0,
            rec.switch_pct / 100.0,
            rec.num_entry_legs,
            rec.num_hedge_legs,
            rec.option_entry_price_cap,
            rec.hedge_entry_price_cap,
            rec.hedge_exit_entry_ratio / 100.0,
            rec.hedge_exit_multiplier,
            rec.leg_profit_pct / 100.0,
            rec.portfolio_profit_target_pct / 100.0,
            rec.portfolio_stop_loss_pct / 100.0,
            rec.portfolio_capital,
            rec.no_of_lots,
            rec.lot_size,
            rec.max_reentry_rounds,
            rec.eod_time,
            v_from_date,
            v_to_date
        );

        -- CRITICAL: v_strategy_config is now a regular view, no refresh needed

        -- Refresh filtered materialized views (now that they are materialized)
        REFRESH MATERIALIZED VIEW v_ha_big_filtered;
        REFRESH MATERIALIZED VIEW v_ha_small_filtered;
        REFRESH MATERIALIZED VIEW v_ha_1m_filtered;
        REFRESH MATERIALIZED VIEW v_nifty50_filtered;
        REFRESH MATERIALIZED VIEW v_nifty_options_filtered;

        -- Refresh all relevant materialized views
        REFRESH MATERIALIZED VIEW mv_ha_big_candle;
        REFRESH MATERIALIZED VIEW mv_ha_small_candle;
        REFRESH MATERIALIZED VIEW mv_ha_1m_candle;
        -- NOTE: v_*_filtered views are regular views, not materialized - they auto-update
        REFRESH MATERIALIZED VIEW mv_nifty_options_filtered;
        REFRESH MATERIALIZED VIEW mv_all_5min_breakouts;
        REFRESH MATERIALIZED VIEW mv_ranked_breakouts_with_rounds;
        REFRESH MATERIALIZED VIEW mv_ranked_breakouts_with_rounds_for_reentry;
        REFRESH MATERIALIZED VIEW mv_base_strike_selection;
        REFRESH MATERIALIZED VIEW mv_breakout_context_round1;
        REFRESH MATERIALIZED VIEW mv_entry_and_hedge_legs;
        -- Create temp table for live prices entry round 1
        DROP TABLE IF EXISTS temp_live_prices_entry_round1 CASCADE;
        CREATE TEMP TABLE temp_live_prices_entry_round1 AS
        WITH strategy AS (
            SELECT eod_time FROM public.v_strategy_config LIMIT 1
        ),
        legs AS (
            SELECT *
            FROM public.mv_entry_and_hedge_legs
            WHERE entry_round = 1
        )
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
            o.time  AS ltp_time,
            o.high  AS option_high,
            o.open  AS option_open,
            o.close AS option_close
        FROM legs l
        JOIN strategy s ON TRUE
        JOIN public.v_nifty_options_filtered o
          ON o.date = l.trade_date
         AND o.expiry = l.expiry_date
         AND o.option_type = l.option_type
         AND o.strike = l.strike
         AND o.time BETWEEN l.entry_time AND s.eod_time;
        CREATE INDEX IF NOT EXISTS idx_temp_live_prices_entry_round1_date_time ON temp_live_prices_entry_round1 (trade_date, entry_time);
        -- Create temp table for entry SL hits round 1
        DROP TABLE IF EXISTS temp_entry_sl_hits_round1 CASCADE;
        CREATE TEMP TABLE temp_entry_sl_hits_round1 AS
        WITH strategy AS (
            SELECT
                'box_with_buffer_sl' AS sl_type,
                'pct_based_breakout' AS preferred_breakout_type,
                sl_percentage,
                box_sl_trigger_pct,
                box_sl_hard_pct,
                width_sl_pct,
                switch_pct
            FROM v_strategy_config
        ),

        /* =====================================================
           ENTRY-ONLY LIVE PRICES
           ===================================================== */
        entry_live_prices AS (
            SELECT *
            FROM temp_live_prices_entry_round1
            WHERE leg_type = 'ENTRY'
            AND ltp_time > entry_time
        ),

        /* =====================================================
           REGULAR SYSTEM SL
           ===================================================== */
        regular_sl AS (
            SELECT
                trade_date,
                expiry_date,
                option_type,
                strike,
                entry_round,
                MIN(ltp_time) AS exit_time,
                'SL_HIT_REGULAR_SL' AS exit_reason
            FROM entry_live_prices lp
            JOIN strategy s ON TRUE
            WHERE s.sl_type = 'regular_system_sl'
              AND lp.option_high >= ROUND(lp.entry_price * (1 + s.sl_percentage), 2)
            GROUP BY trade_date, expiry_date, option_type, strike, entry_round
        ),

        /* =====================================================
           BOX HARD SL
           ===================================================== */
        box_hard_sl AS (
            SELECT
                trade_date,
                expiry_date,
                option_type,
                strike,
                entry_round,
                MIN(ltp_time) AS exit_time,
                'SL_HIT_BOX_HARD_SL' AS exit_reason
            FROM entry_live_prices lp
            JOIN strategy s ON TRUE
            WHERE s.sl_type = 'box_with_buffer_sl'
              AND lp.option_high >= ROUND(lp.entry_price * (1 + s.box_sl_hard_pct), 2)
            GROUP BY trade_date, expiry_date, option_type, strike, entry_round
        ),

        /* =====================================================
           BOX TRIGGER SL — PRICE HIT
           ===================================================== */
        box_trigger_price_hit AS (
            SELECT lp.*
            FROM entry_live_prices lp
            JOIN strategy s ON TRUE
            WHERE s.sl_type = 'box_with_buffer_sl'
              AND lp.option_high >= ROUND(lp.entry_price * (1 + s.box_sl_trigger_pct), 2)
        )	,
        option_universe AS (
            SELECT DISTINCT
                trade_date,
                expiry_date,
                option_type,
                strike,
                entry_round
            FROM box_trigger_price_hit
        ),
        trigger_times AS (
            SELECT
                t.trade_date,
                t.expiry_date,
                t.option_type,
                t.strike,
                t.entry_round,
                t.ltp_time AS trigger_time,
        		/* previous completed 5-min candle for this LTP */
                date_trunc('minute',  t.ltp_time)
                - (EXTRACT(minute FROM  t.ltp_time)::int % 5) * INTERVAL '1 minute'
                - INTERVAL '5 minutes'
                AS prev_candle_time
            FROM box_trigger_price_hit t
           
        )
        --SELECt * FROM trigger_times where trade_date in ('2025-09-29','2025-04-30')
        ,
        breakout_candles AS (
            SELECT
                tr.trade_date,
                tr.expiry_date,
                tr.option_type,
                tr.strike,
                tr.entry_round,
                n.candle_time,
                n.ha_open,
                n.ha_close,
                nr.breakout_high,
                nr.breakout_low
            FROM v_ha_small_filtered n 

            JOIN option_universe tr
              ON n.trade_date = tr.trade_date

            JOIN mv_breakout_context_round1 nr
              ON nr.trade_date = tr.trade_date
             AND nr.temp_entry_round = '1'

            JOIN strategy s ON TRUE

            WHERE
                s.preferred_breakout_type = 'pct_based_breakout'
                AND (
                    /* PUT breakout */
                    (
                        tr.option_type = 'P'
                        AND ((nr.breakout_high - LEAST(n.ha_open, n.ha_close))::numeric
                             / NULLIF(ABS(n.ha_open - n.ha_close), 0)) >= s.switch_pct
                    )
                    OR
                    /* CALL breakout */
                    (
                        tr.option_type = 'C'
                        AND ((GREATEST(n.ha_open, n.ha_close) - nr.breakout_low)::numeric
                             / NULLIF(ABS(n.ha_open - n.ha_close), 0)) >= s.switch_pct
                    )
                )
        )
        -- SELECT * FROM breakout_candles where trade_date in ('2025-09-29','2025-04-30')
        ,

        box_trigger_sl AS (
            SELECT  distinct on (        
        		l.trade_date,
                l.expiry_date,
                l.option_type,
                l.strike,
                l.entry_round
                 )
                l.trade_date,
                l.expiry_date,
                l.option_type,
                l.strike,
                l.entry_round,
                l.trigger_time AS exit_time,
        		'SL_HIT_BOX_TRIGGER_SL' AS exit_reason
            FROM trigger_times l
            JOIN breakout_candles bc
              ON bc.trade_date  = l.trade_date
             AND bc.expiry_date = l.expiry_date
             AND bc.option_type = l.option_type
             AND bc.strike      = l.strike
             AND bc.entry_round = l.entry_round
             AND bc.candle_time = l.prev_candle_time
        	 order by l.trade_date,l.expiry_date,l.option_type,l.strike,l.entry_round,l.trigger_time
        )
        -- SELECT * fROM box_trigger_sl where trade_date in ('2025-09-29','2025-04-30') 
        -- order by  strike,exit_time
        ,

        /* =====================================================
           BOX WIDTH SL
           ===================================================== */
        box_width_sl AS (
            SELECT
                lp.trade_date,
                lp.expiry_date,
                lp.option_type,
                lp.strike,
                lp.entry_round,
                MIN(lp.ltp_time) AS exit_time,
                'SL_HIT_BOX_WIDTH_SL' AS exit_reason
            FROM entry_live_prices lp
            JOIN mv_breakout_context_round1 nr
              ON nr.trade_date = lp.trade_date
            JOIN v_ha_1m_filtered n
              ON n.trade_date = lp.trade_date
             AND n.candle_time = lp.ltp_time
            JOIN strategy s ON TRUE
            WHERE
                (lp.option_type = 'P'
                 AND n.ha_close <=
                     nr.breakout_high
                     - (nr.breakout_high - nr.breakout_low) * s.width_sl_pct)
                OR
                (lp.option_type = 'C'
                 AND n.ha_close >=
                     nr.breakout_low
                     + (nr.breakout_high - nr.breakout_low) * s.width_sl_pct)
            GROUP BY
                lp.trade_date,
                lp.expiry_date,
                lp.option_type,
                lp.strike,
                lp.entry_round
        ),

        /* =====================================================
           ALL SLs → EARLIEST WINS
           ===================================================== */
        all_sl AS (
            SELECT * FROM regular_sl
            UNION ALL
            SELECT * FROM box_hard_sl
            UNION ALL
            SELECT * FROM box_trigger_sl
            UNION ALL
            SELECT * FROM box_width_sl
        ),

        ranked_sl AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        trade_date,
                        expiry_date,
                        option_type,
                        strike,
                        entry_round
                    ORDER BY
                        exit_time,
                        CASE exit_reason
                            WHEN 'SL_HIT_BOX_HARD_SL'    THEN 1
                            WHEN 'SL_HIT_BOX_TRIGGER_SL' THEN 2
                            WHEN 'SL_HIT_BOX_WIDTH_SL'   THEN 3
                            WHEN 'SL_HIT_REGULAR_SL'     THEN 4
                            ELSE 99
                        END
                ) AS rn
            FROM all_sl
        )

        SELECT
            trade_date,
            expiry_date,
            option_type,
            strike,
            entry_round,
            exit_time,
            exit_reason
        FROM ranked_sl
        WHERE rn = 1;
        -- Create temp table for entry SL executions round 1
        DROP TABLE IF EXISTS temp_entry_sl_executions_round1 CASCADE;
        CREATE TEMP TABLE temp_entry_sl_executions_round1 AS
        WITH strategy AS (
            SELECT
                *
            FROM v_strategy_config
        ),

        /* =====================================================
           ENTRY LIVE PRICES
           ===================================================== */
        entry_live_prices AS (
            SELECT *
            FROM temp_live_prices_entry_round1
            WHERE leg_type = 'ENTRY'
        ),

        /* =====================================================
           SL HITS (ENTRY ONLY)
           ===================================================== */
        sl_hits AS (
            SELECT *
            FROM temp_entry_sl_hits_round1
        ),

        /* =====================================================
           FINAL SL EXECUTION (PRICE + PNL)
           ===================================================== */
        sl_executed AS (
            SELECT
                lp.trade_date,
                lp.expiry_date,
                lp.breakout_time,
                lp.entry_time,
                lp.spot_price,
                lp.option_type,
                lp.strike,
                lp.entry_price,
                lp.entry_round,
                'ENTRY'::TEXT AS leg_type,
                lp.transaction_type,
                lp.ltp_time AS exit_time,
                CASE
                    WHEN sh.exit_reason = 'SL_HIT_REGULAR_SL'
                        THEN ROUND(lp.entry_price * (1 + s.sl_percentage), 2)

                    WHEN sh.exit_reason = 'SL_HIT_BOX_HARD_SL'
                        THEN ROUND(lp.entry_price * (1 + s.box_sl_hard_pct), 2)

                    WHEN sh.exit_reason = 'SL_HIT_BOX_TRIGGER_SL'
                        THEN ROUND(lp.entry_price * (1 + s.box_sl_trigger_pct), 2)

                    ELSE lp.option_high
                END AS exit_price,
                sh.exit_reason,
                ROUND(
                    (
                        lp.entry_price
                        -
                        CASE
                            WHEN sh.exit_reason = 'SL_HIT_REGULAR_SL'
                                THEN ROUND(lp.entry_price * (1 + s.sl_percentage), 2)

                            WHEN sh.exit_reason = 'SL_HIT_BOX_HARD_SL'
                                THEN ROUND(lp.entry_price * (1 + s.box_sl_hard_pct), 2)

                            WHEN sh.exit_reason = 'SL_HIT_BOX_TRIGGER_SL'
                                THEN ROUND(lp.entry_price * (1 + s.box_sl_trigger_pct), 2)

                            ELSE lp.option_close
                        END
                    )
                    * s.lot_size
                    * s.no_of_lots,
                    2
                ) AS pnl_amount

            FROM sl_hits sh
            JOIN entry_live_prices lp
              ON lp.trade_date  = sh.trade_date
             AND lp.expiry_date = sh.expiry_date
             AND lp.option_type = sh.option_type
             AND lp.strike      = sh.strike
             AND lp.entry_round = sh.entry_round
             AND lp.ltp_time    = sh.exit_time
            JOIN strategy s ON TRUE
        )

        SELECT *
        FROM sl_executed
        ORDER BY trade_date, expiry_date, exit_time, strike;
        -- Create temp table for entry open legs round 1
        DROP TABLE IF EXISTS temp_entry_open_legs_round1 CASCADE;
        CREATE TEMP TABLE temp_entry_open_legs_round1 AS
        WITH

        /* =====================================================
           ALL ENTRY LEGS (ROUND 1)
           ===================================================== */
        entry_legs AS (
            SELECT *
            FROM mv_entry_and_hedge_legs
            WHERE leg_type = 'ENTRY'
              AND entry_round = 1
        ),

        /* =====================================================
           ENTRY SL HITS
           ===================================================== */
        sl_hit_keys AS (
            SELECT
                trade_date,
                expiry_date,
                option_type,
                strike,
                entry_round
            FROM temp_entry_sl_hits_round1
        )

        /* =====================================================
           OPEN ENTRY LEGS (NO SL HIT)
           ===================================================== */
        SELECT
            e.trade_date,
            e.expiry_date,
            e.breakout_time,
            e.entry_time,
            e.spot_price,
            e.option_type,
            e.strike,
            e.entry_price,
            e.entry_round,
            e.leg_type,
            e.transaction_type
        FROM entry_legs e
        WHERE NOT EXISTS (
            SELECT 1
            FROM sl_hit_keys s
            WHERE s.trade_date  = e.trade_date
              AND s.expiry_date = e.expiry_date
              AND s.option_type = e.option_type
              AND s.strike      = e.strike
              AND s.entry_round = e.entry_round
        );
        REFRESH MATERIALIZED VIEW mv_entry_profit_booking_round1;
        REFRESH MATERIALIZED VIEW mv_entry_eod_close_round1;
        REFRESH MATERIALIZED VIEW mv_entry_closed_legs_round1;
        REFRESH MATERIALIZED VIEW mv_entry_round1_stats;
        REFRESH MATERIALIZED VIEW mv_hedge_exit_on_all_entry_sl;
        REFRESH MATERIALIZED VIEW mv_hedge_exit_partial_conditions;
        REFRESH MATERIALIZED VIEW mv_hedge_closed_legs_round1;
        REFRESH MATERIALIZED VIEW mv_hedge_eod_exit_round1;
        REFRESH MATERIALIZED VIEW mv_entry_exit_on_partial_hedge_round1;
        REFRESH MATERIALIZED VIEW mv_double_buy_legs_round1;
        REFRESH MATERIALIZED VIEW mv_entry_final_exit_round1;
        REFRESH MATERIALIZED VIEW mv_rehedge_trigger_round1;
        REFRESH MATERIALIZED VIEW mv_rehedge_candidate_round1;
        REFRESH MATERIALIZED VIEW mv_rehedge_selected_round1;
        REFRESH MATERIALIZED VIEW mv_rehedge_leg_round1;
        REFRESH MATERIALIZED VIEW mv_rehedge_eod_exit_round1;
        REFRESH MATERIALIZED VIEW mv_all_legs_round1;
        CALL insert_sl_legs_into_book(rec.strategy_name);
        -- REFRESH MATERIALIZED VIEW mv_reentry_triggered_breakouts;
        -- REFRESH MATERIALIZED VIEW mv_reentry_base_strike_selection;
        -- REFRESH MATERIALIZED VIEW mv_reentry_legs_and_hedge_legs;
        -- Create temp table for reentry legs and hedge legs
        DROP TABLE IF EXISTS temp_reentry_legs_and_hedge_legs CASCADE;
        CREATE TEMP TABLE temp_reentry_legs_and_hedge_legs AS
        SELECT * FROM mv_reentry_legs_and_hedge_legs;
        -- Create temp table for reentry live prices
        DROP TABLE IF EXISTS temp_reentry_live_prices CASCADE;
        CREATE TEMP TABLE temp_reentry_live_prices AS
        WITH strategy AS (
            SELECT eod_time FROM v_strategy_config
        ),
        legs AS (
            SELECT *
            FROM temp_reentry_legs_and_hedge_legs
           -- WHERE entry_round = 1
        )
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
            o.time  AS ltp_time,
            o.high  AS option_high,
            o.open  AS option_open,
            o.close AS option_close
        FROM legs l
        JOIN strategy s ON TRUE
        JOIN v_nifty_options_filtered o
          ON o.date = l.trade_date
         AND o.expiry = l.expiry_date
         AND o.option_type = l.option_type
         AND o.strike = l.strike
         AND o.time BETWEEN l.entry_time AND s.eod_time;
        -- REFRESH MATERIALIZED VIEW mv_reentry_breakout_context;
        -- REFRESH MATERIALIZED VIEW mv_reentry_sl_hits;
        -- REFRESH MATERIALIZED VIEW mv_reentry_sl_executions;
        -- REFRESH MATERIALIZED VIEW mv_reentry_open_legs;
        -- REFRESH MATERIALIZED VIEW mv_reentry_profit_booking;
        -- REFRESH MATERIALIZED VIEW mv_reentry_eod_close;
        -- REFRESH MATERIALIZED VIEW mv_reentry_final_exit;
        -- REFRESH MATERIALIZED VIEW mv_reentry_legs_stats;
        -- REFRESH MATERIALIZED VIEW mv_hedge_reentry_exit_on_all_entry_sl;
        -- REFRESH MATERIALIZED VIEW mv_hedge_reentry_exit_on_partial_conditions;
        -- REFRESH MATERIALIZED VIEW mv_hedge_reentry_closed_legs;
        -- REFRESH MATERIALIZED VIEW mv_hedge_reentry_eod_exit;
        -- REFRESH MATERIALIZED VIEW mv_reentry_exit_on_partial_hedge;
        -- REFRESH MATERIALIZED VIEW mv_double_buy_legs_reentry;
        -- REFRESH MATERIALIZED VIEW mv_rehedge_trigger_reentry;
        -- REFRESH MATERIALIZED VIEW mv_rehedge_candidate_reentry;
        -- REFRESH MATERIALIZED VIEW mv_rehedge_selected_reentry;
        -- REFRESH MATERIALIZED VIEW mv_rehedge_leg_reentry;
        -- REFRESH MATERIALIZED VIEW mv_rehedge_eod_exit_reentry;
        -- REFRESH MATERIALIZED VIEW mv_all_legs_reentry;
        -- CALL sp_run_reentry_loop(rec.strategy_name);
        -- REFRESH MATERIALIZED VIEW mv_all_legs_reentry;
        -- Create temp table for entry leg live prices
        DROP TABLE IF EXISTS temp_entry_leg_live_prices CASCADE;
        CREATE TEMP TABLE temp_entry_leg_live_prices AS
        WITH legs AS (
            -- SELECT * FROM temp_all_legs_reentry
            -- UNION ALL
            SELECT * FROM mv_all_legs_round1
        )
        SELECT
            l.*,
            o.time      AS ltp_time,
            o.high      AS option_high,
            o.open      AS option_open,
        	o.close      AS option_close,
            n.high      AS nifty_high,
            n.low       AS nifty_low,
            n.time      AS nifty_time
        FROM legs l
        JOIN v_nifty_options_filtered o   -- ✅ filtered options
          ON o.date  = l.trade_date
         AND o.expiry = l.expiry_date
         AND o.option_type = l.option_type
         AND o.strike      = l.strike
         AND o.time        > l.entry_time
        JOIN v_nifty50_filtered n         -- ✅ filtered spot
          ON n.date = l.trade_date
         AND n.time       = o.time;
        -- REFRESH MATERIALIZED VIEW mv_all_entries_sl_tracking_adjusted;
        -- REFRESH MATERIALIZED VIEW mv_portfolio_mtm_pnl;
        -- REFRESH MATERIALIZED VIEW mv_portfolio_final_pnl;

        -- Store final results
        INSERT INTO strategy_run_results (
            strategy_name,
            execution_time,
            trade_date,
            expiry_date,
            breakout_time,
            entry_time,
            spot_price,
            option_type,
            strike,
            entry_price,
            sl_level,
            entry_round,
            leg_type,
            transaction_type,
            exit_time,
            exit_price,
            exit_reason,
            pnl_amount,
            total_pnl_per_day
        )
        SELECT
            rec.strategy_name,
            NOW(),
            trade_date,
            expiry_date,
            breakout_time,
            entry_time,
            spot_price,
            option_type,
            strike,
            entry_price,
            sl_level,
            entry_round,
            leg_type,
            transaction_type,
            exit_time,
            exit_price,
            exit_reason,
            pnl_amount,
            SUM(pnl_amount) OVER (PARTITION BY trade_date) as total_pnl_per_day
        FROM mv_entry_final_exit_round1;

        -- Run re-entry loop for additional rounds
        CALL sp_run_reentry_loop(rec.strategy_name);

        RAISE NOTICE 'Completed run for strategy %', rec.strategy_name;
    END LOOP;

    RAISE NOTICE 'All strategies processed.';
END;
$$;