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
),

/* =====================================================
   BOX TRIGGER SL — BREAKOUT CONFIRMATION
   ===================================================== */
box_trigger_sl AS (
    SELECT
        t.trade_date,
        t.expiry_date,
        t.option_type,
        t.strike,
        t.entry_round,
        MIN(t.ltp_time) AS exit_time,
        'SL_HIT_BOX_TRIGGER_SL' AS exit_reason
    FROM box_trigger_price_hit t
    JOIN mv_breakout_context_round1 nr
      ON nr.trade_date = t.trade_date
    JOIN v_ha_small_filtered n
      ON n.trade_date = t.trade_date
     AND n.candle_time = t.ltp_time
    JOIN strategy s ON TRUE
    WHERE
        (
            s.preferred_breakout_type = 'full_candle_breakout'
            AND (
                (t.option_type = 'P'
                 AND n.ha_open < nr.breakout_low
                 AND n.ha_close < nr.breakout_low)
                OR
                (t.option_type = 'C'
                 AND n.ha_open > nr.breakout_high
                 AND n.ha_close > nr.breakout_high)
            )
        )
        OR
        (
            s.preferred_breakout_type = 'pct_based_breakout'
            AND (
                (t.option_type = 'P'
                 AND ((nr.breakout_high - LEAST(n.ha_open, n.ha_close))::numeric
                      / NULLIF(ABS(n.ha_open - n.ha_close), 0)) >= s.switch_pct)
                OR
                (t.option_type = 'C'
                 AND ((GREATEST(n.ha_open, n.ha_close) - nr.breakout_low)::numeric
                      / NULLIF(ABS(n.ha_open - n.ha_close), 0)) >= s.switch_pct)
            )
        )
    GROUP BY
        t.trade_date,
        t.expiry_date,
        t.option_type,
        t.strike,
        t.entry_round
),

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
)

SELECT DISTINCT ON (trade_date, expiry_date, option_type, strike, entry_round)
    trade_date,
    expiry_date,
    option_type,
    strike,
    entry_round,
    exit_time,
    exit_reason
FROM all_sl
ORDER BY
    trade_date,
    expiry_date,
    option_type,
    strike,
    entry_round,
    exit_time;

CREATE INDEX IF NOT EXISTS idx_mv_entry_sl_hits_round1_date ON public.mv_entry_sl_hits_round1 (trade_date, expiry_date);
