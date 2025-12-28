-- Materialized view: live option prices for entry round 1
DROP MATERIALIZED VIEW IF EXISTS public.mv_live_prices_entry_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_live_prices_entry_round1 AS
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

CREATE INDEX IF NOT EXISTS idx_mv_live_prices_entry_round1_date_time ON public.mv_live_prices_entry_round1 (trade_date, entry_time);
