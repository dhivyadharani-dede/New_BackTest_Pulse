-- Working table: entry leg live prices with exit info and nifty context
DROP TABLE IF EXISTS public.wrk_entry_leg_live_prices CASCADE;
CREATE TABLE IF NOT EXISTS public.wrk_entry_leg_live_prices (
    trade_date       date,
    expiry_date      date,
    breakout_time    time,
    entry_time       time,
    spot_price       numeric,
    option_type      text,
    strike           numeric,
    entry_price      numeric,
    sl_level         numeric,
    entry_round      int,
    leg_type         text,
    transaction_type text,
    exit_time        time,
    exit_reason      text,

    -- live prices
    ltp_time         time,
    option_high      numeric,
    option_open      numeric,
    option_close     numeric,

    -- nifty context
    nifty_high       numeric,
    nifty_low        numeric,
    nifty_time       time
);
CREATE INDEX IF NOT EXISTS idx_wrk_elp_main
ON public.wrk_entry_leg_live_prices
(trade_date, expiry_date, option_type, strike, ltp_time);

CREATE INDEX IF NOT EXISTS idx_wrk_elp_exit_time
ON public.wrk_entry_leg_live_prices
(trade_date, ltp_time);