TRUNCATE TABLE public.wrk_entry_leg_live_prices;

INSERT INTO public.wrk_entry_leg_live_prices
WITH legs AS (
    SELECT * FROM mv_all_legs_reentry
    UNION ALL
    SELECT * FROM mv_all_legs_round1
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
   CAST(l.sl_level AS numeric) AS sl_level,
    l.entry_round,
    l.leg_type,
    l.transaction_type,
    l.exit_time,
    l.exit_reason,

    o.time      AS ltp_time,
    o.high      AS option_high,
    o.open      AS option_open,
    o.close     AS option_close,

    n.high      AS nifty_high,
    n.low       AS nifty_low,
    n.time      AS nifty_time
FROM legs l
JOIN public.v_nifty_options_filtered o
  ON o.date        = l.trade_date
 AND o.expiry      = l.expiry_date
 AND o.option_type = l.option_type
 AND o.strike      = l.strike
 AND o.time        > l.entry_time
JOIN public.v_nifty50_filtered n
  ON n.date = l.trade_date
 AND n.time = o.time;
