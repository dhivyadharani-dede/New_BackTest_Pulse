-- Materialized view: entry SL hits (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_sl_hits_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_sl_hits_round1 AS
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
    FROM mv_live_prices_entry_round1
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
