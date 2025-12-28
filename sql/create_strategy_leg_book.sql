-- Create strategy_leg_book table if it does not exist
CREATE TABLE IF NOT EXISTS public.strategy_leg_book (
    trade_date date NOT NULL,
    expiry_date date NOT NULL,
    breakout_time time without time zone,
    entry_time time without time zone NOT NULL,
    exit_time time without time zone,
    option_type text COLLATE pg_catalog."default" NOT NULL,
    strike numeric NOT NULL,
    entry_price numeric NOT NULL,
    exit_price numeric,
    transaction_type text COLLATE pg_catalog."default" NOT NULL,
    leg_type text COLLATE pg_catalog."default" NOT NULL,
    entry_round integer NOT NULL DEFAULT 1,
    exit_reason text COLLATE pg_catalog."default",
    CONSTRAINT strategy_leg_book_pkey PRIMARY KEY (trade_date, expiry_date, strike, option_type, entry_round, leg_type)
);
