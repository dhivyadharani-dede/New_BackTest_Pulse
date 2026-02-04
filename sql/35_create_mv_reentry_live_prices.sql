TRUNCATE TABLE public.wrk_reentry_live_prices;

INSERT INTO public.wrk_reentry_live_prices (
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    entry_round,
    leg_type,
    transaction_type,
    ltp_time,
    option_high,
    option_open,
    option_close
)
WITH strategy AS (
    SELECT eod_time FROM v_strategy_config
),
legs AS (
    SELECT *
    FROM mv_reentry_legs_and_hedge_legs
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