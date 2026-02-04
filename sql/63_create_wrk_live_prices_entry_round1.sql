-- Working table: live option prices for entry round 1
DROP TABLE IF EXISTS public.wrk_live_prices_entry_round1 CASCADE;
CREATE TABLE IF NOT EXISTS public.wrk_live_prices_entry_round1 (
    trade_date       date,
    expiry_date      date,
    breakout_time    time,
    entry_time       time,
    spot_price       numeric,
    option_type      text,
    strike           numeric,
    entry_price      numeric,
    entry_round      int,
    leg_type         text,
    transaction_type text,
    ltp_time         time,
    option_high      numeric,
    option_open      numeric,
    option_close     numeric
);
CREATE INDEX IF NOT EXISTS idx_wrk_lpr1_main
ON public.wrk_live_prices_entry_round1
(trade_date, expiry_date, option_type, strike, ltp_time);