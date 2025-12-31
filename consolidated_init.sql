-- =====================================================
-- New_BackTest_Pulse Database Initialization Script
-- Consolidated SQL file for complete database setup
-- Generated from individual SQL files in dependency order
-- =====================================================


-- =====================================================
-- 1. CREATE BASE TABLES
-- =====================================================

-- File: create_nifty50.sql
-- Create parent partitioned table for Nifty50
-- Note: original request used "option_nm \"char\""; using TEXT for flexibility.
CREATE TABLE IF NOT EXISTS public."Nifty50"
(
    date date,
    "time" time without time zone,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    oi numeric,
    option_nm text
)
PARTITION BY RANGE (date);

-- Example: create a default partition if desired (not created here).

-- File: create_nifty_options.sql
-- Create parent partitioned table for Nifty_options
-- Using TEXT for variable-length string fields for flexibility
CREATE TABLE IF NOT EXISTS public."Nifty_options"
(
    symbol text,
    date date,
    expiry date,
    strike numeric,
    option_type text,
    "time" time without time zone,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    oi numeric,
    option_nm text
)
PARTITION BY RANGE (date);

-- File: create_strategy_settings.sql
-- Create strategy_settings table
CREATE TABLE IF NOT EXISTS public."strategy_settings"
(
    strategy_name text COLLATE pg_catalog."default" NOT NULL DEFAULT 'default'::text,
    big_candle_tf numeric DEFAULT 15,
    small_candle_tf numeric DEFAULT 5,
    preferred_breakout_type text COLLATE pg_catalog."default" DEFAULT 'full_candle_breakout'::text,
    breakout_threshold_pct numeric DEFAULT 60,
    option_entry_price_cap numeric DEFAULT 80,
    hedge_entry_price_cap numeric DEFAULT 50,
    num_entry_legs integer DEFAULT 4,
    num_hedge_legs integer DEFAULT 1,
    sl_percentage numeric DEFAULT 20,
    eod_time time without time zone DEFAULT '15:20:00'::time without time zone,
    no_of_lots integer DEFAULT 1,
    lot_size integer DEFAULT 75,
    hedge_exit_entry_ratio numeric DEFAULT 50,
    hedge_exit_multiplier numeric DEFAULT 3,
    leg_profit_pct numeric DEFAULT 84,
    portfolio_profit_target_pct numeric DEFAULT 2,
    portfolio_stop_loss_pct numeric DEFAULT 2,
    portfolio_capital numeric DEFAULT 900000,
    max_reentry_rounds numeric DEFAULT 1,
    sl_type text COLLATE pg_catalog."default" DEFAULT 'regular_system_sl'::text,
    reentry_breakout_type text COLLATE pg_catalog."default" DEFAULT 'full_candle_breakout'::text,
    one_m_candle_tf integer DEFAULT 1,
    entry_candle integer DEFAULT 1,
    box_sl_trigger_pct numeric DEFAULT 25,
    box_sl_hard_pct numeric DEFAULT 35,
    switch_pct numeric(10,2) DEFAULT 20,
    width_sl_pct numeric(10,2) DEFAULT 40,
    CONSTRAINT strategy_settings_pkey PRIMARY KEY (strategy_name),
    CONSTRAINT strategy_settings_preferred_breakout_type_check CHECK (preferred_breakout_type = ANY (ARRAY['full_candle_breakout'::text, 'pct_based_breakout'::text])),
    CONSTRAINT strategy_settings_sl_type_check CHECK (sl_type = ANY (ARRAY['regular_system_sl'::text, 'box_with_buffer_sl'::text]))
);

-- File: create_runtime_strategy_config.sql
-- Create runtime_strategy_config table for dynamic multi-strategy runs
DROP TABLE IF EXISTS public.runtime_strategy_config;

CREATE TABLE public.runtime_strategy_config (
    strategy_name TEXT PRIMARY KEY,

    big_candle_tf INT,
    small_candle_tf INT,
    entry_candle INT,

    preferred_breakout_type TEXT,
    reentry_breakout_type TEXT,
    breakout_threshold_pct NUMERIC,

    sl_type TEXT,
    sl_percentage NUMERIC,
    box_sl_trigger_pct NUMERIC,
    box_sl_hard_pct NUMERIC,
    width_sl_pct NUMERIC,
    switch_pct NUMERIC,

    num_entry_legs INT,
    num_hedge_legs INT,
    option_entry_price_cap NUMERIC,
    hedge_entry_price_cap NUMERIC,
    hedge_exit_entry_ratio NUMERIC,
    hedge_exit_multiplier NUMERIC,

    leg_profit_pct NUMERIC,
    portfolio_profit_target_pct NUMERIC,
    portfolio_stop_loss_pct NUMERIC,

    portfolio_capital NUMERIC,
    no_of_lots INT,
    lot_size INT,

    max_reentry_rounds INT,
    eod_time TIME,

    from_date DATE,
    to_date   DATE
);

-- File: create_strategy_run_results.sql
CREATE TABLE IF NOT EXISTS strategy_run_results (
    strategy_name TEXT NOT NULL,
    execution_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    trade_date DATE,
    expiry_date DATE,
    breakout_time TIME,
    entry_time TIME,
    spot_price NUMERIC,
    option_type TEXT,
    strike NUMERIC,
    entry_price NUMERIC,
    sl_level TEXT,
    entry_round INTEGER,
    leg_type TEXT,
    transaction_type TEXT,
    exit_time TIME,
    exit_price NUMERIC,
    exit_reason TEXT,
    pnl_amount NUMERIC,
    total_pnl_per_day NUMERIC
);

-- File: create_strategy_leg_book.sql
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


-- =====================================================
-- 2. CREATE HEIKIN-ASHI TABLES
-- =====================================================

-- File: create_heikin_ashi_tables.sql
-- Create parent Heikin-Ashi partitioned tables
CREATE TABLE IF NOT EXISTS public.ha_big (
  trade_date date NOT NULL,
  candle_time time without time zone NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  ha_open numeric,
  ha_high numeric,
  ha_low numeric,
  ha_close numeric
) PARTITION BY RANGE (trade_date);

CREATE TABLE IF NOT EXISTS public.ha_small (
  trade_date date NOT NULL,
  candle_time time without time zone NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  ha_open numeric,
  ha_high numeric,
  ha_low numeric,
  ha_close numeric
) PARTITION BY RANGE (trade_date);

CREATE TABLE IF NOT EXISTS public.ha_1m (
  trade_date date NOT NULL,
  candle_time time without time zone NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  ha_open numeric,
  ha_high numeric,
  ha_low numeric,
  ha_close numeric
) PARTITION BY RANGE (trade_date);

-- recommended indexes on parent (note: actual indexes created per-partition)
-- CREATE INDEX IF NOT EXISTS idx_ha_big_trade_time ON public.ha_big (trade_date, candle_time);


-- =====================================================
-- 3. CREATE VIEWS AND MATERIALIZED VIEWS
-- =====================================================

-- File: create_v_strategy_config.sql
-- view exposing runtime config for easy joins

DROP MATERIALIZED VIEW IF EXISTS public.v_strategy_config CASCADE;
CREATE MATERIALIZED VIEW public.v_strategy_config AS
SELECT * FROM runtime_strategy_config;

-- File: create_filtered_views.sql
-- Materialized views that cache per-strategy filtered rows using runtime_strategy_config

CREATE MATERIALIZED VIEW IF NOT EXISTS public.v_ha_big_filtered AS
SELECT
  r.strategy_name,
  h.trade_date,
  h.candle_time,
  h.open,
  h.high,
  h.low,
  h.close,
  h.ha_open,
  h.ha_high,
  h.ha_low,
  h.ha_close
FROM public.ha_big h
JOIN public.runtime_strategy_config r
  ON h.trade_date >= r.from_date
 AND h.trade_date <= r.to_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.v_ha_small_filtered AS
SELECT
  r.strategy_name,
  h.trade_date,
  h.candle_time,
  h.open,
  h.high,
  h.low,
  h.close,
  h.ha_open,
  h.ha_high,
  h.ha_low,
  h.ha_close
FROM public.ha_small h
JOIN public.runtime_strategy_config r
  ON h.trade_date >= r.from_date
 AND h.trade_date <= r.to_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.v_ha_1m_filtered AS
SELECT
  r.strategy_name,
  h.trade_date,
  h.candle_time,
  h.open,
  h.high,
  h.low,
  h.close,
  h.ha_open,
  h.ha_high,
  h.ha_low,
  h.ha_close
FROM public.ha_1m h
JOIN public.runtime_strategy_config r
  ON h.trade_date >= r.from_date
 AND h.trade_date <= r.to_date;

-- Source market data views
CREATE MATERIALIZED VIEW IF NOT EXISTS public.v_nifty50_filtered AS
SELECT
  r.strategy_name,
  m.date,
  m.time,
  m.open,
  m.high,
  m.low,
  m.close,
  m.volume,
  m.oi,
  m.option_nm
FROM public."Nifty50" m
JOIN public.runtime_strategy_config r
  ON m.date >= r.from_date
 AND m.date <= r.to_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.v_nifty_options_filtered AS
SELECT
  r.strategy_name,
  o.date,
  o.time,
  o.open,
  o.high,
  o.low,
  o.close,
  o.volume,
  o.oi,
  o.option_type,
  o.strike_price
FROM public.Nifty_options o
JOIN public.runtime_strategy_config r
  ON o.date >= r.from_date
 AND o.date <= r.to_date;

-- Convenience MVs views
CREATE OR REPLACE VIEW public.v_mv_ha_big_candle_filtered AS
SELECT r.strategy_name, mv.*
FROM public.mv_ha_big_candle mv
JOIN public.runtime_strategy_config r
  ON mv.trade_date >= r.from_date
 AND mv.trade_date <= r.to_date;

CREATE OR REPLACE VIEW public.v_mv_ha_small_candle_filtered AS
SELECT r.strategy_name, mv.*
FROM public.mv_ha_small_candle mv
JOIN public.runtime_strategy_config r
  ON mv.trade_date >= r.from_date
 AND mv.trade_date <= r.to_date;

CREATE OR REPLACE VIEW public.v_mv_ha_1m_candle_filtered AS
SELECT r.strategy_name, mv.*
FROM public.mv_ha_1m_candle mv
JOIN public.runtime_strategy_config r
  ON mv.trade_date >= r.from_date
 AND mv.trade_date <= r.to_date;

-- File: create_mv_ha_candles.sql
-- Create materialized views for HA candle parents if they don't already exist
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_ha_big_candle AS
SELECT trade_date, candle_time, ha_open, ha_high, ha_low, ha_close
FROM public.ha_big;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_ha_small_candle AS
SELECT trade_date, candle_time, ha_open, ha_high, ha_low, ha_close
FROM public.ha_small;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_ha_1m_candle AS
SELECT trade_date, candle_time, ha_open, ha_high, ha_low, ha_close
FROM public.ha_1m;

CREATE INDEX IF NOT EXISTS idx_mv_ha_big_date_time ON public.mv_ha_big_candle (trade_date, candle_time);
CREATE INDEX IF NOT EXISTS idx_mv_ha_small_date_time ON public.mv_ha_small_candle (trade_date, candle_time);
CREATE INDEX IF NOT EXISTS idx_mv_ha_1m_date_time ON public.mv_ha_1m_candle (trade_date, candle_time);

-- File: create_mv_nifty_options_filtered.sql
-- Materialized view for Nifty options filtered by runtime_strategy_config date ranges
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_nifty_options_filtered AS
SELECT
  r.strategy_name,
  o.date,
  o.expiry,
  o.time,
  o.open,
  o.high,
  o.low,
  o.close,
  o.volume,
  o.oi,
  o.option_type,
  o.strike
FROM public."Nifty_options" o
JOIN public.runtime_strategy_config r
  ON o.date >= r.from_date
 AND o.date <= r.to_date;

CREATE INDEX IF NOT EXISTS idx_mv_nifty_options_filtered_date_time ON public.mv_nifty_options_filtered (date, time);


-- =====================================================
-- 4. CREATE BREAKOUT AND TRADING LOGIC VIEWS
-- =====================================================

-- File: create_mv_all_5min_breakouts.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_all_5min_breakouts CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_all_5min_breakouts AS
WITH ha_bounds AS (
    SELECT
        h.trade_date,
        h.candle_time,
        h.ha_high,
        h.ha_low
    FROM (
        SELECT 
            trade_date,
            candle_time,
            ha_high,
            ha_low,
            ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY candle_time) AS rn
        FROM v_ha_big_filtered
    ) h
    JOIN v_strategy_config s
      ON h.rn = s.entry_candle
),

combined AS (
    SELECT
        f.trade_date,
        f.candle_time,
        f.ha_open,
        f.ha_close,
        f.ha_high,
        f.ha_low,

        h.ha_high AS ha_15m_high,
        h.ha_low  AS ha_15m_low,

        s.breakout_threshold_pct,
        f.ha_close - f.ha_open AS candle_body,

        CASE
            WHEN f.ha_open  > h.ha_high
             AND f.ha_close > h.ha_high
             AND f.ha_high  > h.ha_high
             AND f.ha_low   > h.ha_high
            THEN 'full_body_bullish'

            WHEN f.ha_close >
                 (h.ha_high + ABS(f.ha_close - f.ha_open) * s.breakout_threshold_pct)
             AND f.ha_high > h.ha_high
            THEN 'pct_breakout_bullish'

            WHEN f.ha_open  < h.ha_low
             AND f.ha_close < h.ha_low
             AND f.ha_high  < h.ha_low
             AND f.ha_low   < h.ha_low
            THEN 'full_body_bearish'

            WHEN f.ha_close <
                 (h.ha_low - ABS(f.ha_close - f.ha_open) * s.breakout_threshold_pct)
             AND f.ha_low < h.ha_low
            THEN 'pct_breakout_bearish'

            ELSE NULL
        END AS breakout_type
    FROM v_ha_small_filtered f
    JOIN ha_bounds h
      ON f.trade_date = h.trade_date
    JOIN v_strategy_config s
      ON TRUE
    WHERE f.candle_time >=
          TIME '09:15:00'
          + (s.entry_candle * s.big_candle_tf || ' minutes')::interval
      AND f.trade_date BETWEEN s.from_date AND s.to_date
)

SELECT
    trade_date,
    candle_time AS breakout_time,
    candle_time + (s.small_candle_tf || ' minutes')::interval AS entry_time,
    ha_open,
    ha_close,
    ha_high,
    ha_low,
    ha_15m_high,
    ha_15m_low,
    breakout_type
FROM combined
JOIN v_strategy_config s ON TRUE
WHERE breakout_type IS NOT NULL
  AND trade_date BETWEEN s.from_date AND s.to_date;

-- create an index to speed lookups
CREATE INDEX IF NOT EXISTS idx_mv_all_5min_breakouts_date_time ON public.mv_all_5min_breakouts (trade_date, breakout_time);

-- File: create_mv_ranked_breakouts_with_rounds.sql
-- Create ranked breakouts with entry rounds
DROP MATERIALIZED VIEW IF EXISTS public.mv_ranked_breakouts_with_rounds CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_ranked_breakouts_with_rounds AS
WITH filtered AS (
    SELECT
        b.trade_date,
        b.breakout_time,
        b.breakout_type
    FROM public.mv_all_5min_breakouts b
    JOIN public.v_strategy_config s ON TRUE
    WHERE
        (
            s.preferred_breakout_type = 'full_candle_breakout'
            AND b.breakout_type IN ('full_body_bullish', 'full_body_bearish')
        )
        OR
        (
            s.preferred_breakout_type = 'pct_based_breakout'
            AND b.breakout_type IN (
                'pct_breakout_bullish',
                'pct_breakout_bearish',
                'full_body_bullish',
                'full_body_bearish'
            )
        )
),
ranked AS (
    SELECT
        trade_date,
        breakout_time,
        breakout_type,
        ROW_NUMBER() OVER (
            PARTITION BY trade_date
            ORDER BY breakout_time
        ) AS entry_round
    FROM filtered
)
SELECT
    trade_date,
    breakout_time,
    breakout_time + INTERVAL '5 minute' AS entry_time,
    breakout_type,
    CASE
        WHEN breakout_type IN ('full_body_bullish', 'pct_breakout_bullish')
        THEN 'P'
        ELSE 'C'
    END AS entry_option_type,
    entry_round
FROM ranked;

-- index to speed lookups
CREATE INDEX IF NOT EXISTS idx_mv_ranked_breakouts_date_time ON public.mv_ranked_breakouts_with_rounds (trade_date, breakout_time);

-- File: create_mv_ranked_breakouts_with_rounds_for_reentry.sql
-- Materialized view for reentry-ranked breakouts
DROP MATERIALIZED VIEW IF EXISTS public.mv_ranked_breakouts_with_rounds_for_reentry CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_ranked_breakouts_with_rounds_for_reentry AS
WITH strategy AS (
    SELECT reentry_breakout_type FROM public.strategy_settings LIMIT 1
),
filtered_breakouts AS (
    SELECT
        b.trade_date,
        b.breakout_time,
        b.entry_time,
        b.ha_open,
        b.ha_close,
        b.ha_high,
        b.ha_low,
        b.ha_15m_high,
        b.ha_15m_low,
        b.breakout_type
    FROM public.mv_all_5min_breakouts b
    CROSS JOIN strategy s
    JOIN public.mv_ranked_breakouts_with_rounds r
      ON b.trade_date = r.trade_date
    WHERE b.breakout_type IS NOT NULL
      AND (
            (s.reentry_breakout_type = 'full_candle_breakout' AND b.breakout_type IN ('full_body_bullish', 'full_body_bearish'))
         OR (s.reentry_breakout_type = 'pct_based_breakout' AND b.breakout_type IN ('pct_breakout_bullish', 'pct_breakout_bearish','full_body_bullish', 'full_body_bearish'))
      )
      AND r.entry_round = 1
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY breakout_time) AS entry_round
    FROM filtered_breakouts
)
SELECT
    trade_date,
    breakout_time AS breakout_time,
    (breakout_time + INTERVAL '5 minute') AS entry_time,
    breakout_type,
    CASE
        WHEN breakout_type LIKE '%bullish%' THEN 'P'
        WHEN breakout_type LIKE '%bearish%' THEN 'C'
        ELSE NULL
    END AS entry_option_type,
    entry_round
FROM ranked;

CREATE INDEX IF NOT EXISTS idx_mv_ranked_breakouts_reentry_date_time ON public.mv_ranked_breakouts_with_rounds_for_reentry (trade_date, breakout_time);

-- File: create_mv_base_strike_selection.sql
-- Create base strike selection materialized view
DROP MATERIALIZED VIEW IF EXISTS public.mv_base_strike_selection CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_base_strike_selection AS
WITH 
-- 1️⃣ Only required breakout rows
breakout_info AS (
    SELECT
        trade_date,
        entry_time,
        breakout_time,
        breakout_type,
        entry_option_type,
        entry_round
    FROM public.mv_ranked_breakouts_with_rounds
    WHERE entry_round = 1
),

-- 2️⃣ Spot price at entry time
base AS (
    SELECT 
        b.trade_date,
        b.breakout_time,
        b.entry_time,
        b.breakout_type AS breakout_direction,
        b.entry_option_type,
        b.entry_round,
        n.open AS spot_price
    FROM breakout_info b
    JOIN public.v_nifty50_filtered n
      ON n.date = b.trade_date
     AND n.time = b.entry_time
),

-- 3️⃣ Get expiry once per date + option type
expiry_map AS (
    SELECT DISTINCT
        o.date,
        o.option_type,
        o.expiry
    FROM public.v_nifty_options_filtered o
),

-- 4️⃣ Attach expiry + ATM in one go
atm_calc AS (
    SELECT
        b.*,
        e.expiry AS expiry_date,
        CASE
            WHEN (b.spot_price / 50.0 - FLOOR(b.spot_price / 50.0)) > 0.5
            THEN CEIL(b.spot_price / 50.0) * 50
            ELSE FLOOR(b.spot_price / 50.0) * 50
        END AS atm_strike
    FROM base b
    JOIN expiry_map e
      ON e.date = b.trade_date
     AND e.option_type = b.entry_option_type
),

-- 5️⃣ Candidate strikes (time + expiry constrained)
strike_candidates AS (
    SELECT 
        b.*,
        o.strike,
        o.open AS entry_price,
        s.option_entry_price_cap,
        CASE
            WHEN (
                (b.entry_option_type = 'P' AND o.strike > b.atm_strike)
             OR (b.entry_option_type = 'C' AND o.strike < b.atm_strike)
            )
            AND o.open <= s.option_entry_price_cap
            THEN 1
            ELSE 2
        END AS priority,
        ABS(o.open - s.option_entry_price_cap) AS premium_diff
    FROM atm_calc b
    JOIN public.v_nifty_options_filtered o
      ON o.date   = b.trade_date
     AND o.time   = b.entry_time
     AND o.expiry = b.expiry_date
     AND o.option_type = b.entry_option_type
    JOIN public.v_strategy_config s ON TRUE
),

-- 6️⃣ Rank once
ranked_strikes AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY trade_date, expiry_date
               ORDER BY priority, premium_diff
           ) AS rn
    FROM strike_candidates
)

SELECT *
FROM ranked_strikes
WHERE rn = 1;

-- optional index for lookups
CREATE INDEX IF NOT EXISTS idx_mv_base_strike_selection_date_time ON public.mv_base_strike_selection (trade_date, breakout_time);

-- File: create_mv_breakout_context_round1.sql
-- Materialized view: breakout context (high/low) for entry round 1
DROP MATERIALIZED VIEW IF EXISTS public.mv_breakout_context_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_breakout_context_round1 AS
WITH strategy AS (
    SELECT entry_candle FROM public.v_strategy_config LIMIT 1
)
SELECT
    x.trade_date,
    x.ha_high AS breakout_high,
    x.ha_low  AS breakout_low
FROM (
    SELECT 
        trade_date,
        candle_time,
        ha_high,
        ha_low,
        ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY candle_time) AS rn
    FROM public.v_ha_big_filtered
) x
JOIN strategy s ON TRUE
WHERE x.rn = s.entry_candle;

CREATE INDEX IF NOT EXISTS idx_mv_breakout_context_round1_date ON public.mv_breakout_context_round1 (trade_date);


-- =====================================================
-- 4. MISCELLANEOUS
-- =====================================================

-- File: create_mv_entry_and_hedge_legs.sql
-- Materialized view: entry and hedge legs (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_and_hedge_legs CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_and_hedge_legs AS
WITH strategy AS (
    SELECT
        num_entry_legs,
        num_hedge_legs,
        hedge_entry_price_cap
    FROM v_strategy_config
),

/* =========================
   ENTRY LEGS
   ========================= */
entry_strike_cte AS (
    SELECT 
        o.date AS trade_date,
        o.expiry AS expiry_date,
        s.breakout_time,
        s.entry_time,
        s.breakout_direction,
        s.entry_option_type AS option_type,
        s.spot_price,
        o.strike,
        o.open AS entry_price,
        s.entry_round,
        'ENTRY'::TEXT AS leg_type,
        'SELL'::TEXT AS transaction_type
    FROM mv_base_strike_selection s
    JOIN strategy st ON TRUE
    JOIN v_nifty_options_filtered o 
      ON o.date   = s.trade_date
     AND o.expiry = s.expiry_date
     AND o.time   = s.entry_time
     AND o.option_type = s.entry_option_type
     AND (
          (s.entry_option_type = 'P' AND o.strike >= s.strike)
       OR (s.entry_option_type = 'C' AND o.strike <= s.strike)
     )
     AND o.strike BETWEEN
         s.strike - (50 * (st.num_entry_legs - 1))
     AND s.strike + (50 * (st.num_entry_legs - 1))
),

/* =========================
   HEDGE BASE STRIKE (RANKED)
   ========================= */
hedge_ranked AS (
    SELECT
        b.trade_date,
        b.breakout_time,
        b.entry_time,
        b.breakout_direction,
        b.expiry_date,
        b.entry_round,
        CASE 
            WHEN b.entry_option_type = 'C' THEN 'P'
            WHEN b.entry_option_type = 'P' THEN 'C'
        END AS hedge_option_type,
        b.spot_price,
        o.strike,
        o.open AS hedge_price,

        CASE
            WHEN o.strike = b.atm_strike
             AND o.open <= s.hedge_entry_price_cap
            THEN 0
            ELSE 1
        END AS atm_valid_priority,

        ABS(o.open - s.hedge_entry_price_cap) AS premium_diff,

        ROW_NUMBER() OVER (
            PARTITION BY b.trade_date, b.expiry_date, b.entry_round
            ORDER BY
                CASE
                    WHEN o.strike = b.atm_strike
                     AND o.open <= s.hedge_entry_price_cap
                    THEN 0
                    ELSE 1
                END,
                ABS(o.open - s.hedge_entry_price_cap)
        ) AS rn

    FROM mv_base_strike_selection b
    JOIN strategy s ON TRUE
    JOIN v_nifty_options_filtered o 
      ON o.date   = b.trade_date
     AND o.time   = b.entry_time
     AND o.expiry = b.expiry_date
     AND (
          (b.entry_option_type = 'C' AND o.option_type = 'P')
       OR (b.entry_option_type = 'P' AND o.option_type = 'C')
     )
),

selected_hedge_base_strike AS (
    SELECT *
    FROM hedge_ranked
    WHERE rn = 1
),

/* =========================
   HEDGE LEGS
   ========================= */
hedge_strike_cte AS (
    SELECT 
        o.date AS trade_date,
        o.expiry AS expiry_date,
        s.breakout_time,
        s.entry_time,
        s.breakout_direction,
        s.hedge_option_type AS option_type,
        s.spot_price,
        o.strike,
        o.open AS entry_price,
        s.entry_round,
        'HEDGE'::TEXT AS leg_type,
        'SELL'::TEXT AS transaction_type
    FROM selected_hedge_base_strike s
    JOIN strategy st ON TRUE
    JOIN v_nifty_options_filtered o 
      ON o.date   = s.trade_date
     AND o.expiry = s.expiry_date
     AND o.time   = s.entry_time
     AND o.option_type = s.hedge_option_type
     AND (
          (s.hedge_option_type = 'P' AND o.strike <= s.strike)
       OR (s.hedge_option_type = 'C' AND o.strike >= s.strike)
     )
     AND o.strike BETWEEN
         s.strike - (50 * (st.num_hedge_legs - 1))
     AND s.strike + (50 * (st.num_hedge_legs - 1))
)

/* =========================
   FINAL OUTPUT
   ========================= */
SELECT * FROM entry_strike_cte
UNION ALL
SELECT * FROM hedge_strike_cte;

CREATE INDEX IF NOT EXISTS idx_mv_entry_and_hedge_legs_date_time ON public.mv_entry_and_hedge_legs (trade_date, breakout_time);


-- =====================================================
-- 5. MISCELLANEOUS
-- =====================================================

-- File: create_mv_live_prices_entry_round1.sql
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


-- =====================================================
-- 5. CREATE ENTRY/EXIT LOGIC VIEWS
-- =====================================================

-- File: create_mv_entry_sl_hits_round1.sql
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

-- File: create_mv_entry_sl_executions_round1.sql
-- Materialized view: entry SL executions (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_sl_executions_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_sl_executions_round1 AS
WITH strategy AS (
    SELECT
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   ENTRY LIVE PRICES
   ===================================================== */
entry_live_prices AS (
    SELECT *
    FROM mv_live_prices_entry_round1
    WHERE leg_type = 'ENTRY'
),

/* =====================================================
   SL HITS (ENTRY ONLY)
   ===================================================== */
sl_hits AS (
    SELECT *
    FROM mv_entry_sl_hits_round1
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
        lp.option_close AS exit_price,
        sh.exit_reason,
        ROUND(
            (lp.entry_price - lp.option_close)
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

CREATE INDEX IF NOT EXISTS idx_mv_entry_sl_executions_round1_date ON public.mv_entry_sl_executions_round1 (trade_date, expiry_date);

-- File: create_mv_entry_open_legs_round1.sql
-- Materialized view: open entry legs (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_open_legs_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_open_legs_round1 AS
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
    FROM mv_entry_sl_hits_round1
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

CREATE INDEX IF NOT EXISTS idx_mv_entry_open_legs_round1_date ON public.mv_entry_open_legs_round1 (trade_date, expiry_date);

-- File: create_mv_entry_profit_booking_round1.sql
-- Materialized view: profit bookings for entry legs (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_profit_booking_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_profit_booking_round1 AS
WITH strategy AS (
    SELECT
        leg_profit_pct,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   OPEN ENTRY LEGS
   ===================================================== */
open_entry_legs AS (
    SELECT *
    FROM mv_entry_open_legs_round1
),

/* =====================================================
   LIVE PRICES AFTER ENTRY
   ===================================================== */
live_prices AS (
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
        o.open  AS option_open
    FROM open_entry_legs l
    JOIN v_nifty_options_filtered o
      ON o.date = l.trade_date
     AND o.expiry = l.expiry_date
     AND o.option_type = l.option_type
     AND o.strike = l.strike
     AND o.time > l.entry_time
),

/* =====================================================
   PROFIT HIT DETECTION
   ===================================================== */
profit_hit AS (
    SELECT
        lp.trade_date,
        lp.expiry_date,
        lp.option_type,
        lp.strike,
        lp.entry_round,
        MIN(lp.ltp_time) AS exit_time
    FROM live_prices lp
    JOIN strategy s ON TRUE
    WHERE lp.option_open
          <= ROUND(lp.entry_price * (1 - s.leg_profit_pct), 2)
    GROUP BY
        lp.trade_date,
        lp.expiry_date,
        lp.option_type,
        lp.strike,
        lp.entry_round
)

/* =====================================================
   FINAL PROFIT BOOKED LEGS
   ===================================================== */
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
    lp.leg_type,
    lp.transaction_type,
    p.exit_time,
    lp.option_open AS exit_price,
    'PROFIT_BOOKED' AS exit_reason,
    ROUND(
        (lp.entry_price - lp.option_open)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount
FROM profit_hit p
JOIN live_prices lp
  ON lp.trade_date  = p.trade_date
 AND lp.expiry_date = p.expiry_date
 AND lp.option_type = p.option_type
 AND lp.strike      = p.strike
 AND lp.entry_round = p.entry_round
 AND lp.ltp_time    = p.exit_time
JOIN strategy s ON TRUE
ORDER BY trade_date, expiry_date, exit_time, strike;

CREATE INDEX IF NOT EXISTS idx_mv_entry_profit_booking_round1_date ON public.mv_entry_profit_booking_round1 (trade_date, expiry_date);

-- File: create_mv_entry_eod_close_round1.sql
-- Materialized view: entry EOD close exits (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_eod_close_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_eod_close_round1 AS
WITH strategy AS (
    SELECT
        sl_type,
        sl_percentage,
        box_sl_hard_pct,
        eod_time,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   OPEN ENTRY LEGS (NO SL, NO PROFIT)
   ===================================================== */
open_entry_legs AS (
    SELECT *
    FROM mv_entry_open_legs_round1
    WHERE NOT EXISTS (
        SELECT 1
        FROM mv_entry_profit_booking_round1 p
        WHERE p.trade_date  = mv_entry_open_legs_round1.trade_date
          AND p.expiry_date = mv_entry_open_legs_round1.expiry_date
          AND p.option_type = mv_entry_open_legs_round1.option_type
          AND p.strike      = mv_entry_open_legs_round1.strike
          AND p.entry_round = mv_entry_open_legs_round1.entry_round
    )
),

/* =====================================================
   EOD PRICE
   ===================================================== */
eod_prices AS (
    SELECT
        o.date   AS trade_date,
        o.expiry AS expiry_date,
        o.option_type,
        o.strike,
        o.time   AS exit_time,
        o.open   AS exit_price
    FROM v_nifty_options_filtered o
    JOIN strategy s ON TRUE
    WHERE o.time::TIME = s.eod_time::TIME
)

/* =====================================================
   FINAL EOD EXIT
   ===================================================== */
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
    e.exit_time,
    e.exit_price,
    'EOD_CLOSE' AS exit_reason,
    ROUND(
        (l.entry_price - e.exit_price)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount
FROM open_entry_legs l
JOIN eod_prices e
  ON e.trade_date  = l.trade_date
 AND e.expiry_date = l.expiry_date
 AND e.option_type = l.option_type
 And e.strike      = l.strike
JOIN strategy s ON TRUE
ORDER BY trade_date, expiry_date, strike;

CREATE INDEX IF NOT EXISTS idx_mv_entry_eod_close_round1_date ON public.mv_entry_eod_close_round1 (trade_date, expiry_date);

-- File: create_mv_entry_closed_legs_round1.sql
-- Materialized view: closed entry legs (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_closed_legs_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_closed_legs_round1 AS
WITH

/* =====================================================
   SL EXECUTED ENTRY LEGS
   ===================================================== */
sl_exits AS (
    SELECT
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
        exit_time,
        exit_price,
        exit_reason,
        pnl_amount
    FROM mv_entry_sl_executions_round1
),

/* =====================================================
   PROFIT BOOKED ENTRY LEGS
   ===================================================== */
profit_exits AS (
    SELECT
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
        exit_time,
        exit_price,
        exit_reason,
        pnl_amount
    FROM mv_entry_profit_booking_round1
),

/* =====================================================
   EOD CLOSED ENTRY LEGS
   ===================================================== */
eod_exits AS (
    SELECT
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
        exit_time,
        exit_price,
        exit_reason,
        pnl_amount
    FROM mv_entry_eod_close_round1
),

/* =====================================================
   UNION ALL ENTRY EXITS
   ===================================================== */
all_entry_exits AS (
    SELECT * FROM sl_exits
    UNION ALL
    SELECT * FROM profit_exits
    UNION ALL
    SELECT * FROM eod_exits
),

/* =====================================================
   SAFETY: EARLIEST EXIT WINS
   ===================================================== */
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY
                   trade_date,
                   expiry_date,
                   option_type,
                   strike,
                   entry_round
               ORDER BY exit_time
           ) AS rn
    FROM all_entry_exits
)

SELECT
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
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM ranked
WHERE rn = 1
ORDER BY trade_date, expiry_date, entry_time, strike;

CREATE INDEX IF NOT EXISTS idx_mv_entry_closed_legs_round1_date ON public.mv_entry_closed_legs_round1 (trade_date, expiry_date);

-- File: create_mv_entry_round1_stats.sql
-- Materialized view: entry round1 stats
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_round1_stats CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_round1_stats AS
WITH

/* =====================================================
   ALL LEGS (ROUND 1)
   ===================================================== */
legs AS (
    SELECT *
    FROM mv_entry_and_hedge_legs
    WHERE entry_round = 1
),

/* =====================================================
   ENTRY SL-HIT LEGS
   ===================================================== */
entry_sl_hits AS (
    SELECT
        trade_date,
        expiry_date,
        option_type,
        strike,
        entry_round
    FROM mv_entry_closed_legs_round1
    WHERE leg_type = 'ENTRY'
      AND exit_reason LIKE 'SL_HIT%'
),

/* =====================================================
   LIVE PRICES (TIME-ALIGNED)
   ===================================================== */
live_prices AS (
    SELECT *
    FROM mv_live_prices_entry_round1
)

/* =====================================================
   FINAL AGGREGATION (TIME-SAFE)
   ===================================================== */
SELECT
    lp.trade_date,
    lp.expiry_date,
    lp.entry_round,
    lp.ltp_time,

    /* ---------- ENTRY COUNTS ---------- */
    COUNT(*) FILTER (WHERE l.leg_type = 'ENTRY') AS total_entry_legs,

    COUNT(*) FILTER (
        WHERE l.leg_type = 'ENTRY'
          AND EXISTS (
              SELECT 1
              FROM entry_sl_hits s
              WHERE s.trade_date  = l.trade_date
                AND s.expiry_date = l.expiry_date
                AND s.option_type = l.option_type
                AND s.strike      = l.strike
                AND s.entry_round = l.entry_round
          )
    ) AS sl_hit_legs,

    /* ---------- TIME-ALIGNED PREMIUMS ---------- */
    SUM(lp.option_open) FILTER (WHERE l.leg_type = 'ENTRY')
        AS total_entry_ltp,

    MAX(lp.option_open) FILTER (WHERE l.leg_type = 'HEDGE')
        AS hedge_ltp

FROM live_prices lp
JOIN legs l
  ON l.trade_date  = lp.trade_date
 AND l.expiry_date = lp.expiry_date
 AND l.option_type = lp.option_type
 AND l.strike      = lp.strike
 AND l.entry_round = lp.entry_round

GROUP BY
    lp.trade_date,
    lp.expiry_date,
    lp.entry_round,
    lp.ltp_time;

CREATE INDEX IF NOT EXISTS idx_mv_entry_round1_stats_date ON public.mv_entry_round1_stats (trade_date, expiry_date, entry_round);


-- =====================================================
-- 7. MISCELLANEOUS
-- =====================================================

-- File: create_mv_hedge_exit_on_all_entry_sl.sql
-- Materialized view: hedge exit when all entry legs hit SL (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_hedge_exit_on_all_entry_sl CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_hedge_exit_on_all_entry_sl AS
WITH strategy AS (
    SELECT
        no_of_lots,
        lot_size
    FROM v_strategy_config
),
/* =====================================================
   1. LAST ENTRY SL EXIT TIME (ACTUAL EXECUTION)
   ===================================================== */
entry_last_sl_time AS (
    SELECT
        trade_date,
        expiry_date,
        entry_round,
        MAX(exit_time) AS exit_time
    FROM mv_entry_closed_legs_round1
    WHERE leg_type = 'ENTRY'
      AND exit_reason LIKE 'SL_%'
    GROUP BY
        trade_date,
        expiry_date,
        entry_round
),

/* =====================================================
   2. CONFIRM ALL ENTRY LEGS HIT SL (USING STATS MV)
   ===================================================== */
all_entry_sl_completed AS (
    SELECT
        s.trade_date,
        s.expiry_date,
        s.entry_round,
        t.exit_time
    FROM mv_entry_round1_stats s
    JOIN entry_last_sl_time t
      ON s.trade_date  = t.trade_date
     AND s.expiry_date = t.expiry_date
     AND s.entry_round = t.entry_round
     AND s.ltp_time = t.exit_time
    WHERE s.sl_hit_legs = s.total_entry_legs
),

/* =====================================================
   3. ACTUAL HEDGE LEGS (TRUE ENTRY PRICE)
   ===================================================== */
hedge_legs AS (
    SELECT *
    FROM mv_entry_and_hedge_legs
    WHERE leg_type = 'HEDGE'
),

/* =====================================================
   4. HEDGE LIVE PRICES (EXIT PRICE)
   ===================================================== */
hedge_prices AS (
    SELECT *
    FROM mv_live_prices_entry_round1
    WHERE leg_type = 'HEDGE'
)

/* =====================================================
   5. FINAL HEDGE EXIT
   ===================================================== */
SELECT
    h.trade_date,
    h.expiry_date,
    h.breakout_time,
    h.entry_time,
    h.spot_price,
    h.option_type,
    h.strike,

    /* ✅ true hedge entry price */
    h.entry_price,

    0 AS sl_level,
    h.entry_round,
    'HEDGE'::TEXT AS leg_type,
    h.transaction_type,

    a.exit_time,

    /* ✅ hedge exit price at correct minute */
    p.option_open AS exit_price,

    'ALL_ENTRY_SL' AS exit_reason,

    ROUND(
        (h.entry_price - p.option_open)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount

FROM all_entry_sl_completed a
JOIN hedge_legs h
  ON h.trade_date  = a.trade_date
 AND h.expiry_date = a.expiry_date
 AND h.entry_round = a.entry_round

JOIN hedge_prices p
  ON p.trade_date  = h.trade_date
 AND p.expiry_date = h.expiry_date
 AND p.option_type = h.option_type
 AND p.strike      = h.strike
 AND p.entry_round = h.entry_round
 AND p.ltp_time    = a.exit_time

JOIN strategy s ON TRUE

ORDER BY
    h.trade_date,
    h.expiry_date,
    a.exit_time;

CREATE INDEX IF NOT EXISTS idx_mv_hedge_exit_on_all_entry_sl_date ON public.mv_hedge_exit_on_all_entry_sl (trade_date, expiry_date);


-- =====================================================
-- 8. MISCELLANEOUS
-- =====================================================

-- File: create_mv_hedge_exit_partial_conditions.sql
-- Materialized view: hedge exit partial conditions
DROP MATERIALIZED VIEW IF EXISTS public.mv_hedge_exit_partial_conditions CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_hedge_exit_partial_conditions AS
WITH strategy AS (
    SELECT
        hedge_exit_entry_ratio,
        hedge_exit_multiplier,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   1. EXCLUDE ALL-ENTRY-SL ROUNDS
   ===================================================== */
excluded_rounds AS (
    SELECT DISTINCT
        trade_date,
        expiry_date,
        entry_round
    FROM mv_hedge_exit_on_all_entry_sl
),

/* =====================================================
   2. CANDIDATE EXIT MINUTES (STATS-DRIVEN)
   ===================================================== */
exit_candidates AS (
    SELECT
        s.trade_date,
        s.expiry_date,
        s.entry_round,
        s.ltp_time AS exit_time,
        CASE
            WHEN s.sl_hit_legs = 0
             AND s.hedge_ltp * c.hedge_exit_entry_ratio > s.total_entry_ltp
                THEN 'EXIT_50PCT_ENTRY_LT_HEDGE'

            WHEN s.sl_hit_legs > 0
             AND s.sl_hit_legs < s.total_entry_legs
             AND s.hedge_ltp > c.hedge_exit_multiplier * s.total_entry_ltp
                THEN 'EXIT_3X_HEDGE'
        END AS exit_reason
    FROM mv_entry_round1_stats s
    JOIN strategy c ON TRUE
    LEFT JOIN excluded_rounds e
      ON e.trade_date  = s.trade_date
     AND e.expiry_date = s.expiry_date
     AND e.entry_round = s.entry_round
    WHERE e.trade_date IS NULL
      AND (
            (s.sl_hit_legs = 0
             AND s.hedge_ltp * c.hedge_exit_entry_ratio > s.total_entry_ltp)
         OR (s.sl_hit_legs > 0
             AND s.sl_hit_legs < s.total_entry_legs
             AND s.hedge_ltp > c.hedge_exit_multiplier * s.total_entry_ltp)
      )
),

/* =====================================================
   3. EARLIEST VALID EXIT PER ROUND
   ===================================================== */
earliest_exit AS (
    SELECT DISTINCT ON (trade_date, expiry_date, entry_round)
        trade_date,
        expiry_date,
        entry_round,
        exit_time,
        exit_reason
    FROM exit_candidates
    ORDER BY trade_date, expiry_date, entry_round, exit_time
),

/* =====================================================
   4. ACTUAL HEDGE LEGS (TRUE ENTRY PRICE)
   ===================================================== */
hedge_legs AS (
    SELECT *
    FROM mv_entry_and_hedge_legs
    WHERE leg_type = 'HEDGE'
),

/* =====================================================
   5. HEDGE LIVE PRICES (EXIT PRICE)
   ===================================================== */
hedge_prices AS (
    SELECT *
    FROM mv_live_prices_entry_round1
    WHERE leg_type = 'HEDGE'
)

/* =====================================================
   6. FINAL HEDGE EXIT (PARTIAL CONDITIONS)
   ===================================================== */
SELECT
    h.trade_date,
    h.expiry_date,
    h.breakout_time,
    h.entry_time,
    h.spot_price,
    h.option_type,
    h.strike,

    /* true hedge entry price */
    h.entry_price,

    0 AS sl_level,
    h.entry_round,
    'HEDGE'::TEXT AS leg_type,
    h.transaction_type,

    e.exit_time,

    /* hedge price at exit minute */
    p.option_open AS exit_price,

    e.exit_reason,

    ROUND(
        (h.entry_price - p.option_open)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount

FROM earliest_exit e
JOIN hedge_legs h
  ON h.trade_date  = e.trade_date
 AND h.expiry_date = e.expiry_date
 AND h.entry_round = e.entry_round

JOIN hedge_prices p
  ON p.trade_date  = h.trade_date
 AND p.expiry_date = h.expiry_date
 AND p.option_type = h.option_type
 AND p.strike      = h.strike
 AND p.entry_round = h.entry_round
 AND p.ltp_time    = e.exit_time

JOIN strategy s ON TRUE

ORDER BY
    h.trade_date,
    h.expiry_date,
    e.exit_time;

CREATE INDEX IF NOT EXISTS idx_mv_hedge_exit_partial_conditions_date ON public.mv_hedge_exit_partial_conditions (trade_date, expiry_date);


-- =====================================================
-- 6. CREATE HEDGE LOGIC VIEWS
-- =====================================================

-- File: create_mv_hedge_closed_legs_round1.sql
-- Materialized view: hedge closed legs (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_hedge_closed_legs_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_hedge_closed_legs_round1 AS
SELECT *
FROM mv_hedge_exit_on_all_entry_sl

UNION ALL

SELECT *
FROM mv_hedge_exit_partial_conditions

ORDER BY
    trade_date,
    expiry_date,
    entry_round,
    exit_time;

CREATE INDEX IF NOT EXISTS idx_mv_hedge_closed_legs_round1_date ON public.mv_hedge_closed_legs_round1 (trade_date, expiry_date);

-- File: create_mv_hedge_eod_exit_round1.sql
-- Materialized view: hedge EOD exits (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_hedge_eod_exit_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_hedge_eod_exit_round1 AS
WITH strategy AS (
    SELECT
        eod_time,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   1. ALL HEDGE LEGS
   ===================================================== */
hedge_legs AS (
    SELECT *
    FROM mv_entry_and_hedge_legs
    WHERE leg_type = 'HEDGE'
),

/* =====================================================
   2. ALREADY CLOSED HEDGE LEGS
   ===================================================== */
closed_hedges AS (
    SELECT DISTINCT
        trade_date,
        expiry_date,
        entry_round
    FROM mv_hedge_closed_legs_round1
),

/* =====================================================
   3. OPEN HEDGE LEGS (NO EXIT YET)
   ===================================================== */
open_hedges AS (
    SELECT h.*
    FROM hedge_legs h
    LEFT JOIN closed_hedges c
      ON c.trade_date  = h.trade_date
     AND c.expiry_date = h.expiry_date
     AND c.entry_round = h.entry_round
    WHERE c.trade_date IS NULL
),

/* =====================================================
   4. HEDGE PRICE AT EOD
   ===================================================== */
hedge_eod_price AS (
    SELECT *
    FROM mv_live_prices_entry_round1
    WHERE leg_type = 'HEDGE'
)

/* =====================================================
   5. FINAL HEDGE EOD EXIT
   ===================================================== */
SELECT
    h.trade_date,
    h.expiry_date,
    h.breakout_time,
    h.entry_time,
    h.spot_price,
    h.option_type,
    h.strike,

    /* true hedge entry price */
    h.entry_price,

    0 AS sl_level,
    h.entry_round,
    'HEDGE'::TEXT AS leg_type,
    h.transaction_type,

    s.eod_time AS exit_time,

    p.option_open AS exit_price,

    'EOD_CLOSE' AS exit_reason,

    ROUND(
        (h.entry_price - p.option_open)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount

FROM open_hedges h
JOIN strategy s ON TRUE
JOIN hedge_eod_price p
  ON p.trade_date  = h.trade_date
 AND p.expiry_date = h.expiry_date
 AND p.option_type = h.option_type
 AND p.strike      = h.strike
 AND p.entry_round = h.entry_round
 AND p.ltp_time::TIME = s.eod_time::TIME

ORDER BY
    h.trade_date,
    h.expiry_date,
    s.eod_time;

CREATE INDEX IF NOT EXISTS idx_mv_hedge_eod_exit_round1_date ON public.mv_hedge_eod_exit_round1 (trade_date, expiry_date);


-- =====================================================
-- 5. CREATE ENTRY/EXIT LOGIC VIEWS
-- =====================================================

-- File: create_mv_entry_exit_on_partial_hedge_round1.sql
-- Materialized view: entry exit on partial hedge (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_exit_on_partial_hedge_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_exit_on_partial_hedge_round1 AS
WITH strategy AS (
    SELECT
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   1. PARTIAL HEDGE EXIT TIMES
   ===================================================== */
partial_hedge_exit AS (
    SELECT
        trade_date,
        expiry_date,
        entry_round,
        exit_time
    FROM mv_hedge_exit_partial_conditions
),

/* =====================================================
   2. ENTRY LEGS
   ===================================================== */
entry_legs AS (
    SELECT *
    FROM mv_entry_and_hedge_legs
    WHERE leg_type = 'ENTRY'
),

/* =====================================================
   3. ENTRY LIVE PRICES
   ===================================================== */
entry_prices AS (
    SELECT *
    FROM mv_live_prices_entry_round1
    WHERE leg_type = 'ENTRY'
)

/* =====================================================
   4. FORCE ENTRY EXIT
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
    0 AS sl_level,
    e.entry_round,
    'ENTRY'::TEXT AS leg_type,
    e.transaction_type,

    p.exit_time,

    p_price.option_open AS exit_price,

    'EXIT_ON_PARTIAL_HEDGE' AS exit_reason,

    ROUND(
        (e.entry_price - p_price.option_open)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount

FROM partial_hedge_exit p
JOIN entry_legs e
  ON e.trade_date  = p.trade_date
 AND e.expiry_date = p.expiry_date
 AND e.entry_round = p.entry_round

JOIN entry_prices p_price
  ON p_price.trade_date  = e.trade_date
 AND p_price.expiry_date = e.expiry_date
 AND p_price.option_type = e.option_type
 AND p_price.strike      = e.strike
 AND p_price.entry_round = e.entry_round
 AND p_price.ltp_time    = p.exit_time

JOIN strategy s ON TRUE

ORDER BY
    e.trade_date,
    e.expiry_date,
    p.exit_time;

CREATE INDEX IF NOT EXISTS idx_mv_entry_exit_on_partial_hedge_round1_date ON public.mv_entry_exit_on_partial_hedge_round1 (trade_date, expiry_date);


-- =====================================================
-- 11. MISCELLANEOUS
-- =====================================================

-- File: create_mv_double_buy_legs_round1.sql
-- Materialized view: double buy legs (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_double_buy_legs_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_double_buy_legs_round1 AS
WITH strategy AS (
    SELECT
        eod_time,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   1. ENTRY SL-EXITED LEGS
   ===================================================== */
sl_exited_entries AS (
    SELECT *
    FROM mv_entry_closed_legs_round1
    WHERE exit_reason LIKE 'SL_HIT%'
),

/* =====================================================
   2. ENTRY LEG DETAILS
   ===================================================== */
entry_legs AS (
    SELECT *
    FROM mv_entry_and_hedge_legs
    WHERE leg_type = 'ENTRY'
),

/* =====================================================
   3. EOD PRICES
   ===================================================== */
eod_prices AS (
    SELECT *
    FROM mv_live_prices_entry_round1
    WHERE leg_type = 'ENTRY'
)

/* =====================================================
   4. DOUBLE BUY LEG
   ===================================================== */
SELECT
    e.trade_date,
    e.expiry_date,
    e.breakout_time,
    s.exit_time AS entry_time,   -- double buy entry = SL exit time
    e.spot_price,
    e.option_type,
    e.strike,

    s.exit_price AS entry_price, -- buy at SL price

    0 AS sl_level,
    e.entry_round,
    'DOUBLE_BUY' AS leg_type,
    'BUY' AS transaction_type,

    c.eod_time AS exit_time,
    p.option_close AS exit_price,

    'DOUBLE_BUY_EOD_EXIT' AS exit_reason,

    ROUND(
        (p.option_close - s.exit_price)
        * c.lot_size
        * c.no_of_lots,
        2
    ) AS pnl_amount

FROM sl_exited_entries s
JOIN entry_legs e
  ON e.trade_date  = s.trade_date
 AND e.expiry_date = s.expiry_date
 AND e.option_type = s.option_type
 AND e.strike      = s.strike
 AND e.entry_round = s.entry_round

JOIN strategy c ON TRUE
JOIN eod_prices p
  ON p.trade_date  = e.trade_date
 AND p.expiry_date = e.expiry_date
 AND p.option_type = e.option_type
 AND p.strike      = e.strike
 AND p.entry_round = e.entry_round
 AND p.ltp_time::TIME = c.eod_time::TIME;

CREATE INDEX IF NOT EXISTS idx_mv_double_buy_legs_round1_date ON public.mv_double_buy_legs_round1 (trade_date, expiry_date);


-- =====================================================
-- 5. CREATE ENTRY/EXIT LOGIC VIEWS
-- =====================================================

-- File: create_mv_entry_final_exit_round1.sql
-- Materialized view: final entry exits (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_entry_final_exit_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entry_final_exit_round1 AS
WITH

/* =====================================================
   1. ALL ENTRY EXIT CANDIDATES
   ===================================================== */
all_entry_exits AS (

    /* 1️⃣ ENTRY SL exits */
    SELECT
        trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        spot_price,
        option_type,
        strike,
        entry_price,
      --  sl_level,
        entry_round,
        leg_type,
        transaction_type,
        exit_time,
        exit_price,
        exit_reason,
        pnl_amount
    FROM mv_entry_closed_legs_round1
    WHERE leg_type = 'ENTRY'
     -- AND exit_reason LIKE 'SL_HIT%'

    UNION ALL

    /* 2️⃣ ENTRY exits due to partial hedge exit */
    SELECT
        trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        spot_price,
        option_type,
        strike,
        entry_price,
   --     sl_level,
        entry_round,
        leg_type,
        transaction_type,
        exit_time,
        exit_price,
        exit_reason,
        pnl_amount
    FROM mv_entry_exit_on_partial_hedge_round1

    
),

/* =====================================================
   2. EARLIEST EXIT PER ENTRY LEG WINS
   ===================================================== */
ranked_entry_exits AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY
                   trade_date,
                   expiry_date,
                   option_type,
                   strike,
                   entry_round
               ORDER BY exit_time
           ) AS rn
    FROM all_entry_exits
)

/* =====================================================
   3. FINAL ENTRY EXIT
   ===================================================== */
SELECT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
   '0' sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM ranked_entry_exits
WHERE rn = 1
ORDER BY
    trade_date,
    expiry_date,
    entry_round,
    exit_time,
    strike;

CREATE INDEX IF NOT EXISTS idx_mv_entry_final_exit_round1_date ON public.mv_entry_final_exit_round1 (trade_date, expiry_date);


-- =====================================================
-- 7. CREATE REENTRY LOGIC VIEWS
-- =====================================================

-- File: create_mv_rehedge_trigger_round1.sql
-- Materialized view: rehedge trigger (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_trigger_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_rehedge_trigger_round1 AS
SELECT
    s.trade_date,
    s.expiry_date,
    s.entry_round,
    MAX(sl.exit_time) AS rehedge_trigger_time
FROM mv_entry_round1_stats s
JOIN mv_entry_sl_hits_round1 sl
  ON s.trade_date  = sl.trade_date
 AND s.expiry_date = sl.expiry_date
 AND s.entry_round = sl.entry_round
WHERE s.sl_hit_legs = s.total_entry_legs   -- 🔑 ALL ENTRY SL
GROUP BY
    s.trade_date,
    s.expiry_date,
    s.entry_round;

CREATE INDEX IF NOT EXISTS idx_mv_rehedge_trigger_round1_date ON public.mv_rehedge_trigger_round1 (trade_date, expiry_date, entry_round);

-- File: create_mv_rehedge_candidate_round1.sql
-- Materialized view: rehedge candidates (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_candidate_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_rehedge_candidate_round1 AS
SELECT
    h.trade_date,
    h.expiry_date,
    h.entry_round,
    t.rehedge_trigger_time + INTERVAL '1 minute' AS entry_time,
    h.spot_price,
    CASE
        WHEN h.option_type = 'C' THEN 'P'
        ELSE 'C'
    END AS option_type,
    o.strike,
    o.open AS entry_price,
    ABS(o.open - h.exit_price) AS premium_diff,
    h.exit_price AS prev_hedge_exit_price,
    o.time AS option_time
FROM mv_hedge_exit_on_all_entry_sl h
JOIN mv_rehedge_trigger_round1 t
  ON h.trade_date  = t.trade_date
 AND h.expiry_date = t.expiry_date
 AND h.entry_round = t.entry_round
 -- AND h.exit_time = t.rehedge_trigger_time
JOIN v_nifty_options_filtered o
  ON o.date   = h.trade_date
 AND o.expiry = h.expiry_date
 AND o.time = (h.exit_time + INTERVAL '1 minute')
 AND o.option_type =
   CASE WHEN h.option_type = 'C' THEN 'P' ELSE 'C' END
 AND o.time > t.rehedge_trigger_time
;

CREATE INDEX IF NOT EXISTS idx_mv_rehedge_candidate_round1_date ON public.mv_rehedge_candidate_round1 (trade_date, expiry_date, entry_round);

-- File: create_mv_rehedge_selected_round1.sql
-- Materialized view: rehedge selected (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_selected_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_rehedge_selected_round1 AS
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trade_date, expiry_date, entry_round
            ORDER BY option_time, premium_diff
        ) AS rn
    FROM mv_rehedge_candidate_round1
) x
WHERE rn = 1;

CREATE INDEX IF NOT EXISTS idx_mv_rehedge_selected_round1_date ON public.mv_rehedge_selected_round1 (trade_date, expiry_date, entry_round);

-- File: create_mv_rehedge_leg_round1.sql
-- Materialized view: rehedge leg (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_leg_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_rehedge_leg_round1 AS
SELECT
    trade_date,
    expiry_date,
    NULL::TIME AS breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    0 AS sl_level,
    entry_round,
    'REHEDGE' AS leg_type,
    'SELL' AS transaction_type,
    NULL::TIME AS exit_time,
    NULL::NUMERIC AS exit_price,
    'REHEDGE ON ALL ENTRY SL' AS exit_reason,
    0 AS pnl_amount
FROM mv_rehedge_selected_round1;

CREATE INDEX IF NOT EXISTS idx_mv_rehedge_leg_round1_date ON public.mv_rehedge_leg_round1 (trade_date, expiry_date, entry_round);

-- File: create_mv_rehedge_eod_exit_round1.sql
-- Materialized view: rehedge EOD exits (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_eod_exit_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_rehedge_eod_exit_round1 AS
WITH strategy AS (
    SELECT
        eod_time,
        no_of_lots,
        lot_size
    FROM v_strategy_config
)

SELECT
    h.trade_date,
    h.expiry_date,
    h.breakout_time,
    h.entry_time,
    h.spot_price,
    h.option_type,
    h.strike,
    h.entry_price,
    0 AS sl_level,
    h.entry_round,
    h.leg_type,
    h.transaction_type,

    s.eod_time::TIME AS exit_time,
    o.open AS exit_price,

    'EOD CLOSE' AS exit_reason,

    ROUND(
        (h.entry_price - o.open)
        * s.no_of_lots
        * s.lot_size,
        2
    ) AS pnl_amount

FROM mv_rehedge_leg_round1 h
JOIN strategy s ON TRUE
JOIN v_nifty_options_filtered o
  ON o.date = h.trade_date
 AND o.expiry = h.expiry_date
 AND o.option_type = h.option_type
 AND o.strike = h.strike
 AND o.time::TIME = s.eod_time::TIME;

CREATE INDEX IF NOT EXISTS idx_mv_rehedge_eod_exit_round1_date ON public.mv_rehedge_eod_exit_round1 (trade_date, expiry_date, entry_round);


-- =====================================================
-- 14. MISCELLANEOUS
-- =====================================================

-- File: create_mv_all_legs_round1.sql
-- Materialized view: all legs (round 1)
DROP MATERIALIZED VIEW IF EXISTS public.mv_all_legs_round1 CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_all_legs_round1 AS

/* =====================================================
   ENTRY – FINAL EXIT (risk + soft exits)
   ===================================================== */
SELECT
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
    pnl_amount
FROM mv_entry_final_exit_round1
UNION ALL
SELECT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    '0' AS sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM mv_double_buy_legs_round1
UNION ALL
SELECT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
   '0' AS sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM mv_hedge_closed_legs_round1

UNION ALL
SELECT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
   '0' AS sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM mv_hedge_eod_exit_round1

/* =====================================================
   RE-HEDGE – EOD EXIT
   ===================================================== */
UNION ALL
SELECT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    '0' AS sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM mv_rehedge_eod_exit_round1

ORDER BY
    trade_date,
    expiry_date,
    entry_round,
    entry_time,
    exit_time,
    strike,
    leg_type;

CREATE INDEX IF NOT EXISTS idx_mv_all_legs_round1_date ON public.mv_all_legs_round1 (trade_date, expiry_date, entry_round);


-- =====================================================
-- 8. CREATE REENTRY ROUND VIEWS
-- =====================================================

-- File: create_mv_reentry_triggered_breakouts.sql
-- Materialized view: re-entry triggered breakouts
DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_triggered_breakouts CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_reentry_triggered_breakouts AS
WITH config AS (
    SELECT 
        max_reentry_rounds,
        reentry_breakout_type
    FROM public.v_strategy_config
    LIMIT 1
),

/* =====================================================
   STEP 1: FIRST SL HIT TIME PER ENTRY ROUND
   ===================================================== */
first_sl_hit AS (
    SELECT 
        trade_date,
        expiry_date,
        entry_round,
        MIN(exit_time) AS first_sl_exit_time
    FROM public.mv_all_legs_round1
    WHERE exit_reason LIKE 'SL_HIT_%'
    GROUP BY trade_date, expiry_date, entry_round
),

/* =====================================================
   STEP 2: CALCULATE SCAN START TIME (NEXT 5-MIN CANDLE)
   ===================================================== */
scan_start_time AS (
    SELECT 
        f.trade_date,
        f.expiry_date,
        f.entry_round + 1 AS next_entry_round,
        (
            date_trunc('hour', f.first_sl_exit_time)
            + INTERVAL '1 minute'
              * CEIL(EXTRACT(MINUTE FROM f.first_sl_exit_time)::INT / 5.0) * 5
        ) AS scan_start_time,
        c.max_reentry_rounds
    FROM first_sl_hit f
    JOIN config c ON TRUE
    WHERE f.entry_round < c.max_reentry_rounds
),

/* =====================================================
   STEP 3: FIND NEXT VALID BREAKOUT AFTER SL
   ===================================================== */
ranked_next_breakouts AS (
    SELECT 
        b.trade_date,
        b.breakout_time,
        b.breakout_type,
        b.entry_option_type,
        s.next_entry_round,
        ROW_NUMBER() OVER (
            PARTITION BY b.trade_date, s.next_entry_round
            ORDER BY b.breakout_time
        ) AS rn
    FROM public.mv_ranked_breakouts_with_rounds_for_reentry b
    JOIN scan_start_time s
      ON b.trade_date = s.trade_date
     AND b.breakout_time >= s.scan_start_time
)

/* =====================================================
   STEP 4: PICK FIRST BREAKOUT FOR EACH RE-ENTRY ROUND
   ===================================================== */
SELECT 
    trade_date,
    breakout_time,
    breakout_time + INTERVAL '5 minutes' AS entry_time,
    breakout_type,
    entry_option_type,
    next_entry_round AS entry_round
FROM ranked_next_breakouts
WHERE rn = 1;

CREATE INDEX IF NOT EXISTS idx_mv_reentry_triggered_breakouts_date_round ON public.mv_reentry_triggered_breakouts (trade_date, entry_round);

-- File: create_mv_reentry_base_strike_selection.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_base_strike_selection CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_base_strike_selection AS
WITH 
-- 1️⃣ Only required breakout rows
breakout_info AS (
    SELECT
        trade_date,
        entry_time,
        breakout_time,
        breakout_type,
        entry_option_type,
        entry_round
    FROM mv_reentry_triggered_breakouts
   -- WHERE entry_round = 1
),

-- 2️⃣ Spot price at entry time
base AS (
    SELECT 
        b.trade_date,
        b.breakout_time,
        b.entry_time,
        b.breakout_type AS breakout_direction,
        b.entry_option_type,
        b.entry_round,
        n.open AS spot_price
    FROM breakout_info b
    JOIN v_nifty50_filtered n
      ON n.date = b.trade_date
     AND n.time = b.entry_time
),

-- 3️⃣ Get expiry once per date + option type
expiry_map AS (
    SELECT DISTINCT
        o.date,
        o.option_type,
        o.expiry
    FROM v_nifty_options_filtered o
),

-- 4️⃣ Attach expiry + ATM in one go
atm_calc AS (
    SELECT
        b.*,
        e.expiry AS expiry_date,
        CASE
            WHEN (b.spot_price / 50.0 - FLOOR(b.spot_price / 50.0)) > 0.5
            THEN CEIL(b.spot_price / 50.0) * 50
            ELSE FLOOR(b.spot_price / 50.0) * 50
        END AS atm_strike
    FROM base b
    JOIN expiry_map e
      ON e.date = b.trade_date
     AND e.option_type = b.entry_option_type
),

-- 5️⃣ Candidate strikes (time + expiry constrained)
strike_candidates AS (
    SELECT 
        b.*,
        o.strike,
        o.open AS entry_price,
        s.option_entry_price_cap,
        CASE
            WHEN (
                (b.entry_option_type = 'P' AND o.strike > b.atm_strike)
             OR (b.entry_option_type = 'C' AND o.strike < b.atm_strike)
            )
            AND o.open <= s.option_entry_price_cap
            THEN 1
            ELSE 2
        END AS priority,
        ABS(o.open - s.option_entry_price_cap) AS premium_diff
    FROM atm_calc b
    JOIN v_nifty_options_filtered o
      ON o.date   = b.trade_date
     AND o.time   = b.entry_time
     AND o.expiry = b.expiry_date
     AND o.option_type = b.entry_option_type
    JOIN v_strategy_config s ON TRUE
),

-- 6️⃣ Rank once
ranked_strikes AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY trade_date, expiry_date
               ORDER BY priority, premium_diff
           ) AS rn
    FROM strike_candidates
)

SELECT *
FROM ranked_strikes
WHERE rn = 1;

-- File: create_mv_reentry_legs_and_hedge_legs.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_legs_and_hedge_legs CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_legs_and_hedge_legs AS
WITH strategy AS (
    SELECT
        num_entry_legs,
        num_hedge_legs,
        hedge_entry_price_cap
    FROM v_strategy_config
),
current_strategy AS (
    SELECT strategy_name FROM current_reentry_strategy
),

/* =========================
   ENTRY LEGS
   ========================= */
entry_strike_cte AS (
    SELECT 
        c.strategy_name,
        o.date AS trade_date,
        o.expiry AS expiry_date,
        s.breakout_time,
        s.entry_time,
        s.breakout_direction,
        s.entry_option_type AS option_type,
        s.spot_price,
        o.strike,
        o.open AS entry_price,
        s.entry_round,
        'RE-ENTRY'::TEXT AS leg_type,
        'SELL'::TEXT AS transaction_type
    FROM mv_reentry_base_strike_selection s
    JOIN strategy st ON TRUE
    JOIN current_strategy c ON TRUE
    JOIN v_nifty_options_filtered o 
      ON o.date   = s.trade_date
     AND o.expiry = s.expiry_date
     AND o.time   = s.entry_time
     AND o.option_type = s.entry_option_type
     AND (
          (s.entry_option_type = 'P' AND o.strike >= s.strike)
       OR (s.entry_option_type = 'C' AND o.strike <= s.strike)
     )
     AND o.strike BETWEEN
         s.strike - (50 * (st.num_entry_legs - 1))
     AND s.strike + (50 * (st.num_entry_legs - 1))
),

/* =========================
   HEDGE BASE STRIKE (RANKED)
   ========================= */
hedge_ranked AS (
    SELECT
        b.trade_date,
        b.breakout_time,
        b.entry_time,
        b.breakout_direction,
        b.expiry_date,
        b.entry_round,
        CASE 
            WHEN b.entry_option_type = 'C' THEN 'P'
            WHEN b.entry_option_type = 'P' THEN 'C'
        END AS hedge_option_type,
        b.spot_price,
        o.strike,
        o.open AS hedge_price,

        CASE
            WHEN o.strike = b.atm_strike
             AND o.open <= s.hedge_entry_price_cap
            THEN 0
            ELSE 1
        END AS atm_valid_priority,

        ABS(o.open - s.hedge_entry_price_cap) AS premium_diff,

        ROW_NUMBER() OVER (
            PARTITION BY b.trade_date, b.expiry_date, b.entry_round
            ORDER BY
                CASE
                    WHEN o.strike = b.atm_strike
                     AND o.open <= s.hedge_entry_price_cap
                    THEN 0
                    ELSE 1
                END,
                ABS(o.open - s.hedge_entry_price_cap)
        ) AS rn

    FROM mv_reentry_base_strike_selection b
    JOIN strategy s ON TRUE
    JOIN v_nifty_options_filtered o 
      ON o.date   = b.trade_date
     AND o.time   = b.entry_time
     AND o.expiry = b.expiry_date
     AND (
          (b.entry_option_type = 'C' AND o.option_type = 'P')
       OR (b.entry_option_type = 'P' AND o.option_type = 'C')
     )
)
,

selected_hedge_base_strike AS (
    SELECT *
    FROM hedge_ranked
    WHERE rn = 1
),

/* =========================
   HEDGE LEGS
   ========================= */
hedge_strike_cte AS (
    SELECT 
        c.strategy_name,
        o.date AS trade_date,
        o.expiry AS expiry_date,
        s.breakout_time,
        s.entry_time,
        s.breakout_direction,
        s.hedge_option_type AS option_type,
        s.spot_price,
        o.strike,
        o.open AS entry_price,
        s.entry_round,
        'HEDGE-RE-ENTRY'::TEXT AS leg_type,
        'SELL'::TEXT AS transaction_type
    FROM selected_hedge_base_strike s
    JOIN strategy st ON TRUE
    JOIN current_strategy c ON TRUE
    JOIN v_nifty_options_filtered o 
      ON o.date   = s.trade_date
     AND o.expiry = s.expiry_date
     AND o.time   = s.entry_time
     AND o.option_type = s.hedge_option_type
     AND (
          (s.hedge_option_type = 'P' AND o.strike <= s.strike)
       OR (s.hedge_option_type = 'C' AND o.strike >= s.strike)
     )
     AND o.strike BETWEEN
         s.strike - (50 * (st.num_hedge_legs - 1))
     AND s.strike + (50 * (st.num_hedge_legs - 1))
)

/* =========================
   FINAL OUTPUT
   ========================= */
SELECT * FROM entry_strike_cte
UNION ALL
SELECT * FROM hedge_strike_cte;

-- File: create_mv_reentry_live_prices.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_live_prices CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_live_prices AS
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

-- File: create_mv_reentry_breakout_context.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_breakout_context CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_breakout_context AS
WITH strategy AS (
    SELECT entry_candle FROM v_strategy_config
)
SELECT
    trade_date,
    ha_high AS breakout_high,
    ha_low  AS breakout_low
FROM (
    SELECT 
        trade_date,
        candle_time,
        ha_high,
        ha_low,
        ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY candle_time) AS rn
    FROM v_ha_big_filtered
) x
JOIN strategy s ON TRUE
WHERE rn = s.entry_candle;

-- File: create_mv_reentry_sl_hits.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_sl_hits CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_sl_hits AS
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
    FROM mv_reentry_live_prices
    WHERE leg_type = 'RE-ENTRY'
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
    JOIN mv_reentry_breakout_context nr
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
    JOIN mv_reentry_breakout_context nr
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

-- File: create_mv_reentry_sl_executions.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_sl_executions CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_sl_executions AS
WITH strategy AS (
    SELECT
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   ENTRY LIVE PRICES
   ===================================================== */
entry_live_prices AS (
    SELECT *
    FROM mv_reentry_live_prices
    WHERE leg_type = 'RE-ENTRY'
),

/* =====================================================
   SL HITS (ENTRY ONLY)
   ===================================================== */
sl_hits AS (
    SELECT *
    FROM mv_reentry_sl_hits
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
        'RE-ENTRY'::TEXT AS leg_type,
        lp.transaction_type,
        lp.ltp_time AS exit_time,
        lp.option_close AS exit_price,
        sh.exit_reason,
        ROUND(
            (lp.entry_price - lp.option_close)
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

-- File: create_mv_reentry_open_legs.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_open_legs CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_open_legs AS
WITH

/* =====================================================
   ALL ENTRY LEGS (RE-ENTRY)
   ===================================================== */
entry_legs AS (
    SELECT *
    FROM mv_reentry_legs_and_hedge_legs
    WHERE leg_type = 'RE-ENTRY'
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
    FROM mv_reentry_sl_hits
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

-- File: create_mv_reentry_profit_booking.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_profit_booking CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_profit_booking AS
WITH strategy AS (
    SELECT
        leg_profit_pct,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   OPEN ENTRY LEGS
   ===================================================== */
open_entry_legs AS (
    SELECT *
    FROM mv_reentry_open_legs
),

/* =====================================================
   LIVE PRICES AFTER ENTRY
   ===================================================== */
live_prices AS (
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
        o.open  AS option_open
    FROM open_entry_legs l
    JOIN v_nifty_options_filtered o
      ON o.date = l.trade_date
     AND o.expiry = l.expiry_date
     AND o.option_type = l.option_type
     AND o.strike = l.strike
     AND o.time > l.entry_time
),

/* =====================================================
   PROFIT HIT DETECTION
   ===================================================== */
profit_hit AS (
    SELECT
        lp.trade_date,
        lp.expiry_date,
        lp.option_type,
        lp.strike,
        lp.entry_round,
        MIN(lp.ltp_time) AS exit_time
    FROM live_prices lp
    JOIN strategy s ON TRUE
    WHERE lp.option_open
          <= ROUND(lp.entry_price * (1 - s.leg_profit_pct), 2)
    GROUP BY
        lp.trade_date,
        lp.expiry_date,
        lp.option_type,
        lp.strike,
        lp.entry_round
)

/* =====================================================
   FINAL PROFIT BOOKED LEGS
   ===================================================== */
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
    lp.leg_type,
    lp.transaction_type,
    p.exit_time,
    lp.option_open AS exit_price,
    'PROFIT_BOOKED' AS exit_reason,
    ROUND(
        (lp.entry_price - lp.option_open)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount
FROM profit_hit p
JOIN live_prices lp
  ON lp.trade_date  = p.trade_date
 AND lp.expiry_date = p.expiry_date
 AND lp.option_type = p.option_type
 AND lp.strike      = p.strike
 AND lp.entry_round = p.entry_round
 AND lp.ltp_time    = p.exit_time
JOIN strategy s ON TRUE
ORDER BY trade_date, expiry_date, exit_time, strike;

-- File: create_mv_reentry_eod_close.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_eod_close CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_eod_close AS
WITH strategy AS (
    SELECT
        sl_type,
        sl_percentage,
        box_sl_hard_pct,
        eod_time,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   OPEN ENTRY LEGS (NO SL, NO PROFIT)
   ===================================================== */
open_entry_legs AS (
    SELECT *
    FROM mv_reentry_open_legs
    WHERE NOT EXISTS (
        SELECT 1
        FROM mv_reentry_profit_booking p
        WHERE p.trade_date  = mv_reentry_open_legs.trade_date
          AND p.expiry_date = mv_reentry_open_legs.expiry_date
          AND p.option_type = mv_reentry_open_legs.option_type
          AND p.strike      = mv_reentry_open_legs.strike
          AND p.entry_round = mv_reentry_open_legs.entry_round
    )
),

/* =====================================================
   EOD PRICE
   ===================================================== */
eod_prices AS (
    SELECT
        o.date   AS trade_date,
        o.expiry AS expiry_date,
        o.option_type,
        o.strike,
        o.time   AS exit_time,
        o.open   AS exit_price
    FROM v_nifty_options_filtered o
    JOIN strategy s ON TRUE
    WHERE o.time::TIME = s.eod_time::TIME
)

/* =====================================================
   FINAL EOD EXIT
   ===================================================== */
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
    e.exit_time,
    e.exit_price,
    'EOD_CLOSE' AS exit_reason,
    ROUND(
        (l.entry_price - e.exit_price)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount
FROM open_entry_legs l
JOIN eod_prices e
  ON e.trade_date  = l.trade_date
 AND e.expiry_date = l.expiry_date
 AND e.option_type = l.option_type
 And e.strike      = l.strike
JOIN strategy s ON TRUE
ORDER BY trade_date, expiry_date, strike;

-- File: create_mv_reentry_final_exit.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_final_exit CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_final_exit AS
WITH

/* =====================================================
   SL EXECUTED ENTRY LEGS
   ===================================================== */
sl_exits AS (
    SELECT
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
        exit_time,
        exit_price,
        exit_reason,
        pnl_amount
    FROM mv_reentry_sl_executions
),

/* =====================================================
   PROFIT BOOKED ENTRY LEGS
   ===================================================== */
profit_exits AS (
    SELECT
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
        exit_time,
        exit_price,
        exit_reason,
        pnl_amount
    FROM mv_reentry_profit_booking
),

/* =====================================================
   EOD CLOSED ENTRY LEGS
   ===================================================== */
eod_exits AS (
    SELECT
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
        exit_time,
        exit_price,
        exit_reason,
        pnl_amount
    FROM mv_reentry_eod_close
),

/* =====================================================
   UNION ALL ENTRY EXITS
   ===================================================== */
all_entry_exits AS (
    SELECT * FROM sl_exits
    UNION ALL
    SELECT * FROM profit_exits
    UNION ALL
    SELECT * FROM eod_exits
),

/* =====================================================
   SAFETY: EARLIEST EXIT WINS
   ===================================================== */
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY
                   trade_date,
                   expiry_date,
                   option_type,
                   strike,
                   entry_round
               ORDER BY exit_time
           ) AS rn
    FROM all_entry_exits
)

SELECT
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
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM ranked
WHERE rn = 1
ORDER BY trade_date, expiry_date, entry_time, strike;


-- =====================================================
-- 16. MISCELLANEOUS
-- =====================================================

-- File: create_mv_double_buy_legs_reentry.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_double_buy_legs_reentry CASCADE;
CREATE MATERIALIZED VIEW mv_double_buy_legs_reentry AS
WITH strategy AS (
    SELECT
        eod_time,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   1. ENTRY SL-EXITED LEGS
   ===================================================== */
sl_exited_entries AS (
    SELECT *
    FROM mv_reentry_final_exit where exit_reason like 'SL_HIT%'
),

/* =====================================================
   2. ENTRY LEG DETAILS
   ===================================================== */
entry_legs AS (
    SELECT *
    FROM mv_reentry_legs_and_hedge_legs
    WHERE leg_type = 'RE-ENTRY'
),

/* =====================================================
   3. EOD PRICES
   ===================================================== */
eod_prices AS (
    SELECT *
    FROM mv_reentry_live_prices
    WHERE leg_type = 'RE-ENTRY'
)

/* =====================================================
   4. DOUBLE BUY LEG
   ===================================================== */
SELECT
    e.trade_date,
    e.expiry_date,
    e.breakout_time,
    s.exit_time AS entry_time,   -- double buy entry = SL exit time
    e.spot_price,
    e.option_type,
    e.strike,

    s.exit_price AS entry_price, -- buy at SL price

    0 AS sl_level,
    e.entry_round,
    'DOUBLE_BUY_REENTRY' AS leg_type,
    'BUY' AS transaction_type,

    c.eod_time AS exit_time,
    p.option_close AS exit_price,

    'DOUBLE_BUY_REENTRY_EOD_EXIT' AS exit_reason,

    ROUND(
        (p.option_close-s.exit_price)
        * c.lot_size
        * c.no_of_lots,
        2
    ) AS pnl_amount

FROM sl_exited_entries s
JOIN entry_legs e
  ON e.trade_date  = s.trade_date
 AND e.expiry_date = s.expiry_date
 AND e.option_type = s.option_type
 AND e.strike      = s.strike
 AND e.entry_round = s.entry_round

JOIN strategy c ON TRUE
JOIN eod_prices p
  ON p.trade_date  = e.trade_date
 AND p.expiry_date = e.expiry_date
 AND p.option_type = e.option_type
 AND p.strike      = e.strike
 AND p.entry_round = e.entry_round
 AND p.ltp_time::TIME = c.eod_time::TIME;


-- =====================================================
-- 8. CREATE REENTRY ROUND VIEWS
-- =====================================================

-- File: create_mv_reentry_legs_stats.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_legs_stats CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_legs_stats AS
WITH

/* =====================================================
   ALL LEGS (RE-ENTRY)
   ===================================================== */
legs AS (
    SELECT *
    FROM mv_reentry_legs_and_hedge_legs
    WHERE entry_round > 1
),

/* =====================================================
   ENTRY SL-HIT LEGS
   ===================================================== */
entry_sl_hits AS (
    SELECT
        trade_date,
        expiry_date,
        option_type,
        strike,
        entry_round
    FROM mv_reentry_sl_hits
),

/* =====================================================
   LIVE PRICES (TIME-ALIGNED)
   ===================================================== */
live_prices AS (
    SELECT *
    FROM mv_reentry_live_prices
)

/* =====================================================
   FINAL AGGREGATION (TIME-SAFE)
   ===================================================== */
SELECT
    lp.trade_date,
    lp.expiry_date,
    lp.entry_round,
    lp.ltp_time,

    /* ---------- ENTRY COUNTS ---------- */
    COUNT(*) FILTER (WHERE l.leg_type = 'RE-ENTRY') AS total_entry_legs,

    COUNT(*) FILTER (
        WHERE l.leg_type = 'RE-ENTRY'
          AND EXISTS (
              SELECT 1
              FROM entry_sl_hits s
              WHERE s.trade_date  = l.trade_date
                AND s.expiry_date = l.expiry_date
                AND s.option_type = l.option_type
                AND s.strike      = l.strike
                AND s.entry_round = l.entry_round
          )
    ) AS sl_hit_legs,

    /* ---------- TIME-ALIGNED PREMIUMS ---------- */
    SUM(lp.option_open) FILTER (WHERE l.leg_type = 'RE-ENTRY')
        AS total_entry_ltp,

    MAX(lp.option_open) FILTER (WHERE l.leg_type = 'HEDGE-RE-ENTRY')
        AS hedge_ltp

FROM live_prices lp
JOIN legs l
  ON l.trade_date  = lp.trade_date
 AND l.expiry_date = lp.expiry_date
 AND l.option_type = lp.option_type
 AND l.strike      = lp.strike
 AND l.entry_round = lp.entry_round

GROUP BY
    lp.trade_date,
    lp.expiry_date,
    lp.entry_round,
    lp.ltp_time;


-- =====================================================
-- 9. CREATE REENTRY HEDGE VIEWS
-- =====================================================

-- File: create_mv_hedge_reentry_exit_on_all_entry_sl.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_hedge_reentry_exit_on_all_entry_sl CASCADE;
CREATE MATERIALIZED VIEW mv_hedge_reentry_exit_on_all_entry_sl AS
WITH strategy AS (
    SELECT
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   1. LAST ENTRY SL EXIT TIME (ACTUAL EXECUTION)
   ===================================================== */
entry_last_sl_time AS (
    SELECT
        trade_date,
        expiry_date,
        entry_round,
        MAX(exit_time) AS exit_time
    FROM mv_reentry_sl_executions
    WHERE leg_type = 'RE-ENTRY'
      AND exit_reason LIKE 'SL_%'
    GROUP BY
        trade_date,
        expiry_date,
        entry_round
),

/* =====================================================
   2. CONFIRM ALL ENTRY LEGS HIT SL (USING STATS MV)
   ===================================================== */
all_entry_sl_completed AS (
    SELECT
        s.trade_date,
        s.expiry_date,
        s.entry_round,
        t.exit_time
    FROM mv_reentry_legs_stats s
    JOIN entry_last_sl_time t
      ON s.trade_date  = t.trade_date
     AND s.expiry_date = t.expiry_date
     AND s.entry_round = t.entry_round
	 AND s.ltp_time=t.exit_time
    WHERE s.sl_hit_legs = s.total_entry_legs
),

/* =====================================================
   3. ACTUAL HEDGE LEGS (TRUE ENTRY PRICE)
   ===================================================== */
hedge_legs AS (
    SELECT *
    FROM mv_reentry_legs_and_hedge_legs
    WHERE leg_type = 'HEDGE-RE-ENTRY'
),

/* =====================================================
   4. HEDGE LIVE PRICES (EXIT PRICE)
   ===================================================== */
hedge_prices AS (
    SELECT *
    FROM mv_reentry_live_prices
    WHERE leg_type = 'HEDGE-RE-ENTRY'
)

/* =====================================================
   5. FINAL HEDGE EXIT
   ===================================================== */
SELECT
    h.trade_date,
    h.expiry_date,
    h.breakout_time,
    h.entry_time,
    h.spot_price,
    h.option_type,
    h.strike,

    /* ✅ true hedge entry price */
    h.entry_price,

    0 AS sl_level,
    h.entry_round,
    'HEDGE-RE-ENTRY'::TEXT AS leg_type,
    h.transaction_type,

    a.exit_time,

    /* ✅ hedge exit price at correct minute */
    p.option_open AS exit_price,

    'ALL_ENTRY_SL' AS exit_reason,

    ROUND(
        (h.entry_price - p.option_open)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount

FROM all_entry_sl_completed a
JOIN hedge_legs h
  ON h.trade_date  = a.trade_date
 AND h.expiry_date = a.expiry_date
 AND h.entry_round = a.entry_round

JOIN hedge_prices p
  ON p.trade_date  = h.trade_date
 AND p.expiry_date = h.expiry_date
 AND p.option_type = h.option_type
 AND p.strike      = h.strike
 AND p.entry_round = h.entry_round
 AND p.ltp_time    = a.exit_time

JOIN strategy s ON TRUE

ORDER BY
    h.trade_date,
    h.expiry_date,
    a.exit_time;

-- File: create_mv_hedge_reentry_exit_on_partial_conditions.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_hedge_reentry_exit_on_partial_conditions CASCADE;
CREATE MATERIALIZED VIEW mv_hedge_reentry_exit_on_partial_conditions AS
WITH strategy AS (
    SELECT
        hedge_exit_entry_ratio,
        hedge_exit_multiplier,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   1. EXCLUDE ALL-ENTRY-SL ROUNDS
   ===================================================== */
excluded_rounds AS (
    SELECT DISTINCT
        trade_date,
        expiry_date,
        entry_round
    FROM mv_hedge_reentry_exit_on_all_entry_sl
),

/* =====================================================
   2. CANDIDATE EXIT MINUTES (STATS-DRIVEN)
   ===================================================== */
exit_candidates AS (
    SELECT
        s.trade_date,
        s.expiry_date,
        s.entry_round,
        s.ltp_time AS exit_time,
        CASE
            WHEN s.sl_hit_legs = 0
             AND s.hedge_ltp * c.hedge_exit_entry_ratio > s.total_entry_ltp
                THEN 'EXIT_50PCT_ENTRY_LT_HEDGE'

            WHEN s.sl_hit_legs > 0
             AND s.sl_hit_legs < s.total_entry_legs
             AND s.hedge_ltp > c.hedge_exit_multiplier * s.total_entry_ltp
                THEN 'EXIT_3X_HEDGE'
        END AS exit_reason
    FROM mv_reentry_legs_stats s
    JOIN strategy c ON TRUE
    LEFT JOIN excluded_rounds e
      ON e.trade_date  = s.trade_date
     AND e.expiry_date = s.expiry_date
     AND e.entry_round = s.entry_round
    WHERE e.trade_date IS NULL
      AND (
            (s.sl_hit_legs = 0
             AND s.hedge_ltp * c.hedge_exit_entry_ratio > s.total_entry_ltp)
         OR (s.sl_hit_legs > 0
             AND s.sl_hit_legs < s.total_entry_legs
             AND s.hedge_ltp > c.hedge_exit_multiplier * s.total_entry_ltp)
      )
),

/* =====================================================
   3. EARLIEST VALID EXIT PER ROUND
   ===================================================== */
earliest_exit AS (
    SELECT DISTINCT ON (trade_date, expiry_date, entry_round)
        trade_date,
        expiry_date,
        entry_round,
        exit_time,
        exit_reason
    FROM exit_candidates
    ORDER BY trade_date, expiry_date, entry_round, exit_time
),

/* =====================================================
   4. ACTUAL HEDGE LEGS (TRUE ENTRY PRICE)
   ===================================================== */
hedge_legs AS (
    SELECT *
    FROM mv_reentry_legs_and_hedge_legs
    WHERE leg_type = 'HEDGE-RE-ENTRY'
),

/* =====================================================
   5. HEDGE LIVE PRICES (EXIT PRICE)
   ===================================================== */
hedge_prices AS (
    SELECT *
    FROM mv_reentry_live_prices
    WHERE leg_type = 'HEDGE-RE-ENTRY'
)

/* =====================================================
   6. FINAL HEDGE EXIT (PARTIAL CONDITIONS)
   ===================================================== */
SELECT
    h.trade_date,
    h.expiry_date,
    h.breakout_time,
    h.entry_time,
    h.spot_price,
    h.option_type,
    h.strike,

    /* true hedge entry price */
    h.entry_price,

    0 AS sl_level,
    h.entry_round,
    'HEDGE-RE-ENTRY'::TEXT AS leg_type,
    h.transaction_type,

    e.exit_time,

    /* hedge price at exit minute */
    p.option_open AS exit_price,

    e.exit_reason,

    ROUND(
        (h.entry_price - p.option_open)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount

FROM earliest_exit e
JOIN hedge_legs h
  ON h.trade_date  = e.trade_date
 AND h.expiry_date = e.expiry_date
 AND h.entry_round = e.entry_round

JOIN hedge_prices p
  ON p.trade_date  = h.trade_date
 AND p.expiry_date = h.expiry_date
 AND p.option_type = h.option_type
 AND p.strike      = h.strike
 AND p.entry_round = h.entry_round
 AND p.ltp_time    = e.exit_time

JOIN strategy s ON TRUE

ORDER BY
    h.trade_date,
    h.expiry_date,
    e.exit_time;

-- File: create_mv_hedge_reentry_closed_legs.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_hedge_reentry_closed_legs CASCADE;
CREATE MATERIALIZED VIEW mv_hedge_reentry_closed_legs AS
SELECT *
FROM mv_hedge_reentry_exit_on_all_entry_sl

UNION ALL

SELECT *
FROM mv_hedge_reentry_exit_on_partial_conditions

ORDER BY
    trade_date,
    expiry_date,
    entry_round,
    exit_time;

-- File: create_mv_hedge_reentry_eod_exit.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_hedge_reentry_eod_exit CASCADE;
CREATE MATERIALIZED VIEW mv_hedge_reentry_eod_exit AS
WITH strategy AS (
    SELECT
        eod_time,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   1. ALL HEDGE LEGS
   ===================================================== */
hedge_legs AS (
    SELECT *
    FROM mv_reentry_legs_and_hedge_legs
    WHERE leg_type = 'HEDGE-RE-ENTRY'
),

/* =====================================================
   2. ALREADY CLOSED HEDGE LEGS
   ===================================================== */
closed_hedges AS (
    SELECT DISTINCT
        trade_date,
        expiry_date,
        entry_round
    FROM mv_hedge_reentry_closed_legs
),

/* =====================================================
   3. OPEN HEDGE LEGS (NO EXIT YET)
   ===================================================== */
open_hedges AS (
    SELECT h.*
    FROM hedge_legs h
    LEFT JOIN closed_hedges c
      ON c.trade_date  = h.trade_date
     AND c.expiry_date = h.expiry_date
     AND c.entry_round = h.entry_round
    WHERE c.trade_date IS NULL
),

/* =====================================================
   4. HEDGE PRICE AT EOD
   ===================================================== */
hedge_eod_price AS (
    SELECT *
    FROM mv_reentry_live_prices
    WHERE leg_type = 'HEDGE-RE-ENTRY'
)

/* =====================================================
   5. FINAL HEDGE EOD EXIT
   ===================================================== */
SELECT
    h.trade_date,
    h.expiry_date,
    h.breakout_time,
    h.entry_time,
    h.spot_price,
    h.option_type,
    h.strike,

    /* true hedge entry price */
    h.entry_price,

    0 AS sl_level,
    h.entry_round,
    'HEDGE-RE-ENTRY'::TEXT AS leg_type,
    h.transaction_type,

    s.eod_time AS exit_time,

    p.option_open AS exit_price,

    'EOD_CLOSE' AS exit_reason,

    ROUND(
        (h.entry_price - p.option_open)
        * s.no_of_lots
        * s.lot_size,
        2
    ) AS pnl_amount

FROM open_hedges h
JOIN strategy s ON TRUE
JOIN hedge_eod_price p
  ON p.trade_date  = h.trade_date
 AND p.expiry_date = h.expiry_date
 AND p.option_type = h.option_type
 AND p.strike      = h.strike
 AND p.entry_round = h.entry_round
 AND p.ltp_time::TIME = s.eod_time::TIME



ORDER BY
    h.trade_date,
    h.expiry_date,
    s.eod_time;


-- =====================================================
-- 8. CREATE REENTRY ROUND VIEWS
-- =====================================================

-- File: create_mv_reentry_exit_on_partial_hedge.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_exit_on_partial_hedge CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_exit_on_partial_hedge AS
WITH strategy AS (
    SELECT
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

/* =====================================================
   1. PARTIAL HEDGE EXIT TIMES
   ===================================================== */
partial_hedge_exit AS (
    SELECT
        trade_date,
        expiry_date,
        entry_round,
        exit_time
    FROM mv_hedge_reentry_exit_on_partial_conditions
),

/* =====================================================
   2. ENTRY LEGS
   ===================================================== */
entry_legs AS (
    SELECT *
    FROM mv_reentry_legs_and_hedge_legs
    WHERE leg_type = 'RE-ENTRY'
),

/* =====================================================
   3. ENTRY LIVE PRICES
   ===================================================== */
entry_prices AS (
    SELECT *
    FROM mv_reentry_live_prices
    WHERE leg_type = 'RE-ENTRY'
)

/* =====================================================
   4. FORCE ENTRY EXIT
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
    0 AS sl_level,
    e.entry_round,
    'RE-ENTRY'::TEXT AS leg_type,
    e.transaction_type,

    p.exit_time,

    p_price.option_open AS exit_price,

    'EXIT_ON_PARTIAL_HEDGE' AS exit_reason,

    ROUND(
        (e.entry_price - p_price.option_open)
        * s.lot_size
        * s.no_of_lots,
        2
    ) AS pnl_amount

FROM partial_hedge_exit p
JOIN entry_legs e
  ON e.trade_date  = p.trade_date
 AND e.expiry_date = p.expiry_date
 AND e.entry_round = p.entry_round

JOIN entry_prices p_price
  ON p_price.trade_date  = e.trade_date
 AND p_price.expiry_date = e.expiry_date
 AND p_price.option_type = e.option_type
 AND p_price.strike      = e.strike
 AND p_price.entry_round = e.entry_round
 AND p_price.ltp_time    = p.exit_time

JOIN strategy s ON TRUE

ORDER BY
    e.trade_date,
    e.expiry_date,
    p.exit_time;


-- =====================================================
-- 9. CREATE REENTRY HEDGE VIEWS
-- =====================================================

-- File: create_mv_rehedge_trigger_reentry.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_trigger_reentry CASCADE;
CREATE MATERIALIZED VIEW mv_rehedge_trigger_reentry AS
SELECT
    s.trade_date,
    s.expiry_date,
    s.entry_round,
    MAX(sl.exit_time) AS rehedge_trigger_time
FROM mv_reentry_legs_stats s
JOIN mv_reentry_sl_hits sl
  ON s.trade_date  = sl.trade_date
 AND s.expiry_date = sl.expiry_date
 AND s.entry_round = sl.entry_round
WHERE s.sl_hit_legs = s.total_entry_legs   -- 🔑 ALL ENTRY SL
GROUP BY
    s.trade_date,
    s.expiry_date,
    s.entry_round;

-- File: create_mv_rehedge_candidate_reentry.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_candidate_reentry CASCADE;
CREATE MATERIALIZED VIEW mv_rehedge_candidate_reentry AS
SELECT
    h.trade_date,
    h.expiry_date,
    h.entry_round,
    t.rehedge_trigger_time + INTERVAL '1 minute' AS entry_time,
    h.spot_price,
    CASE
        WHEN h.option_type = 'C' THEN 'P'
        ELSE 'C'
    END AS option_type,
    o.strike,
    o.open AS entry_price,
    ABS(o.open - h.exit_price) AS premium_diff,
    h.exit_price AS prev_hedge_exit_price,
    o.time AS option_time
FROM mv_hedge_reentry_exit_on_all_entry_sl h
JOIN mv_rehedge_trigger_reentry t
  ON h.trade_date  = t.trade_date
 AND h.expiry_date = t.expiry_date
 AND h.entry_round = t.entry_round
 --AND h.exit_time=t.rehedge_trigger_time
JOIN v_nifty_options_filtered o
  ON o.date   = h.trade_date
 AND o.expiry = h.expiry_date
 AND o.time = (h.exit_time+ INTERVAL '1 minute')
-- AND o.time=t.rehedge_trigger_time
 AND o.option_type =
        CASE WHEN h.option_type = 'C' THEN 'P' ELSE 'C' END
AND o.time > t.rehedge_trigger_time;   -- 🔑 safe time filter

-- File: create_mv_rehedge_selected_reentry.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_selected_reentry CASCADE;
CREATE MATERIALIZED VIEW mv_rehedge_selected_reentry AS
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trade_date, expiry_date, entry_round
            ORDER BY option_time, premium_diff
        ) AS rn
    FROM mv_rehedge_candidate_reentry
) x
WHERE rn = 1;

-- File: create_mv_rehedge_leg_reentry.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_leg_reentry CASCADE;
CREATE MATERIALIZED VIEW mv_rehedge_leg_reentry AS
SELECT
    trade_date,
    expiry_date,
    NULL::TIME AS breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    0 AS sl_level,
    entry_round,
    'REHEDGE_RENTRY' AS leg_type,
    'SELL' AS transaction_type,
    NULL::TIME AS exit_time,
    NULL::NUMERIC AS exit_price,
    'REHEDGE ON ALL ENTRY SL' AS exit_reason,
    0 AS pnl_amount
FROM mv_rehedge_selected_reentry;

-- File: create_mv_rehedge_eod_exit_reentry.sql
-- Materialized view: rehedge EOD exits (reentry)
DROP MATERIALIZED VIEW IF EXISTS public.mv_rehedge_eod_exit_reentry CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_rehedge_eod_exit_reentry AS
WITH strategy AS (
    SELECT
        eod_time,
        no_of_lots,
        lot_size
    FROM v_strategy_config
)

SELECT
    h.trade_date,
    h.expiry_date,
    h.breakout_time,
    h.entry_time,
    h.spot_price,
    h.option_type,
    h.strike,
    h.entry_price,
    0 AS sl_level,
    h.entry_round,
    h.leg_type,
    h.transaction_type,

    s.eod_time::TIME AS exit_time,
    o.open AS exit_price,

    'EOD CLOSE' AS exit_reason,

    ROUND(
        (h.entry_price - o.open)
        * s.no_of_lots
        * s.lot_size,
        2
    ) AS pnl_amount

FROM mv_rehedge_leg_reentry h
JOIN strategy s ON TRUE
JOIN v_nifty_options_filtered o
  ON o.date = h.trade_date
 AND o.expiry = h.expiry_date
 AND o.option_type = h.option_type
 AND o.strike = h.strike
 AND o.time::TIME = s.eod_time::TIME;

CREATE INDEX IF NOT EXISTS idx_mv_rehedge_eod_exit_reentry_date ON public.mv_rehedge_eod_exit_reentry (trade_date, expiry_date, entry_round);


-- =====================================================
-- 21. MISCELLANEOUS
-- =====================================================

-- File: create_mv_all_legs_reentry.sql
DROP MATERIALIZED VIEW IF EXISTS public.mv_all_legs_reentry CASCADE;
CREATE MATERIALIZED VIEW mv_all_legs_reentry AS

/* =====================================================
   ENTRY – FINAL EXIT (risk + soft exits)
   ===================================================== */
SELECT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    '0' as sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM mv_reentry_final_exit
UNION ALL
SELECT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    '0' AS sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM mv_double_buy_legs_reentry
UNION ALL
SELECT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    '0' AS sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM mv_hedge_reentry_closed_legs

UNION ALL
SELECT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
   '0' AS sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM mv_hedge_reentry_eod_exit

/* =====================================================
   RE-HEDGE – EOD EXIT
   ===================================================== */
UNION ALL
SELECT
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
    spot_price,
    option_type,
    strike,
    entry_price,
    '0' AS sl_level,
    entry_round,
    leg_type,
    transaction_type,
    exit_time,
    exit_price,
    exit_reason,
    pnl_amount
FROM mv_rehedge_eod_exit_reentry

ORDER BY
    trade_date,
    expiry_date,
    entry_round,
    entry_time,
    exit_time,
    strike,
    leg_type;


-- =====================================================
-- 10. CREATE PORTFOLIO AND FINAL VIEWS
-- =====================================================

-- File: create_mv_entry_leg_live_prices.sql
CREATE MATERIALIZED VIEW mv_entry_leg_live_prices AS
WITH legs AS (
    SELECT * FROM mv_all_legs_reentry
    UNION ALL
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

-- File: create_mv_all_entries_sl_tracking_adjusted.sql
CREATE MATERIALIZED VIEW mv_all_entries_sl_tracking_adjusted AS
WITH config AS (
    SELECT 
        sl_type,
        sl_percentage,
        box_sl_trigger_pct,
        box_sl_hard_pct,
        eod_time,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

next_round_reentry_times AS (
    SELECT
        trade_date,
        entry_round - 1 AS prior_round,
        entry_time AS next_round_start_time
    FROM mv_reentry_triggered_breakouts
),

all_legs AS (
       SELECT * FROM mv_all_legs_REENTRY
    UNION ALL
    SELECT * FROM mv_all_legs_round1
),

adjusted_exit_time_data AS (
    SELECT
        l.*,
        CASE
            WHEN r.next_round_start_time IS NOT NULL
             AND l.entry_round = r.prior_round
             AND l.exit_time > r.next_round_start_time
            THEN r.next_round_start_time
            ELSE l.exit_time
        END AS adjusted_exit_time,
        CASE
            WHEN r.next_round_start_time IS NOT NULL
             AND l.entry_round = r.prior_round
             AND l.exit_time > r.next_round_start_time
            THEN 'Closed due to re-entry'
            ELSE l.exit_reason
        END AS adjusted_exit_reason
    FROM all_legs l
    LEFT JOIN next_round_reentry_times r
      ON r.trade_date = l.trade_date
     AND r.prior_round = l.entry_round
),

adjusted_exit_price_data AS (
    SELECT
        l.*,
        COALESCE(p.option_open, l.exit_price) AS adjusted_exit_price
    FROM adjusted_exit_time_data l
    LEFT JOIN mv_entry_leg_live_prices p
      ON p.trade_date  = l.trade_date
     AND p.expiry_date = l.expiry_date
     AND p.option_type = l.option_type
     AND p.strike      = l.strike
     AND p.ltp_time    = l.adjusted_exit_time
)

SELECT DISTINCT ON (
    trade_date, expiry_date,entry_time, option_type, strike, leg_type, entry_round
)
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
    adjusted_exit_time AS exit_time,
    adjusted_exit_price AS exit_price,
    adjusted_exit_reason AS exit_reason,
    ROUND(
        CASE 
            WHEN transaction_type = 'BUY'
                THEN (adjusted_exit_price - entry_price)
            ELSE (entry_price - adjusted_exit_price)
        END * lot_size * no_of_lots,
        2
    ) AS pnl_amount
FROM adjusted_exit_price_data
JOIN config ON TRUE
--where trade_date='2025-04-03'
ORDER BY
    trade_date, expiry_date,entry_time,entry_round,leg_type, option_type, strike;

-- File: create_mv_portfolio_mtm_pnl.sql
CREATE MATERIALIZED VIEW mv_portfolio_mtm_pnl AS
WITH config AS (
    SELECT 
        portfolio_capital,
        portfolio_profit_target_pct,
        portfolio_stop_loss_pct,
        no_of_lots,
        lot_size
    FROM v_strategy_config
),

all_legs AS (
    SELECT * FROM mv_all_entries_sl_tracking_adjusted
),

all_times AS (
    SELECT DISTINCT
        date,
        expiry,
        time
    FROM v_nifty_options_filtered
    WHERE time >= '09:36:00'
),

closed_pnl_at_time AS (
    SELECT
        l.trade_date,
        l.expiry_date,
        t.time,
        SUM(
            CASE
                WHEN l.transaction_type = 'SELL'
                    THEN (l.entry_price - l.exit_price)
                ELSE (l.exit_price - l.entry_price)
            END * c.lot_size * c.no_of_lots
        ) AS realized_pnl
    FROM all_times t
    JOIN all_legs l
      ON l.trade_date = t.date
     AND l.expiry_date = t.expiry
    JOIN config c ON TRUE
    WHERE l.exit_time IS NOT NULL
      AND l.exit_time < t.time
    GROUP BY l.trade_date, l.expiry_date, t.time
),

open_mtm_at_time AS (
    SELECT
        l.trade_date,
        l.expiry_date,
        t.time,
        SUM(
            CASE
                WHEN l.transaction_type = 'SELL'
                    THEN (l.entry_price - o.open)
                ELSE (o.open - l.entry_price)
            END * c.lot_size * c.no_of_lots
        ) AS unrealized_pnl
    FROM all_times t
    JOIN all_legs l
      ON l.trade_date = t.date
     AND l.expiry_date = t.date
    JOIN v_nifty_options_filtered o
      ON o.date  = l.trade_date
     AND o.expiry = l.expiry_date
     AND o.option_type = l.option_type
     AND o.strike      = l.strike
     AND o.time        = t.time
    JOIN config c ON TRUE
    WHERE l.entry_time <= t.time
      AND (l.exit_time IS NULL OR t.time < l.exit_time)
    GROUP BY l.trade_date, l.expiry_date, t.time
)

SELECT
    t.date,
    t.expiry,
    t.time,
    ROUND(COALESCE(c.realized_pnl, 0) + COALESCE(o.unrealized_pnl, 0), 2) AS total_pnl,
    ROUND(COALESCE(c.realized_pnl, 0), 2) AS realized_pnl,
    ROUND(COALESCE(o.unrealized_pnl, 0), 2) AS unrealized_pnl
FROM all_times t
LEFT JOIN closed_pnl_at_time c
  ON c.trade_date = t.date
 AND c.expiry_date = t.expiry
 AND c.time = t.time
LEFT JOIN open_mtm_at_time o
  ON o.trade_date = t.date
 AND o.expiry_date = t.expiry
 AND o.time = t.time
ORDER BY date, expiry, time;

-- File: create_mv_portfolio_final_pnl.sql
CREATE MATERIALIZED VIEW mv_portfolio_final_pnl AS
WITH config AS (
    SELECT 
        portfolio_capital,
        ROUND(portfolio_profit_target_pct / 100, 3) AS portfolio_profit_target_pct,
        ROUND(portfolio_stop_loss_pct  / 100, 3) AS portfolio_stop_loss_pct,
        no_of_lots,
        lot_size,
        eod_time
    FROM v_strategy_config
),

portfolio_mtm_pnl AS (
    SELECT * FROM mv_portfolio_mtm_pnl --where date='2025-04-01'
),

/* ============================================================
   1. First portfolio-level exit trigger (profit / loss)
   ============================================================ */
portfolio_exit_trigger AS (
    SELECT DISTINCT ON (date, expiry)
        date,
        expiry,
        time AS exit_time,
        total_pnl,
        CASE 
            WHEN total_pnl >= portfolio_capital * portfolio_profit_target_pct
                THEN 'Portfolio Exit - Profit'
            WHEN total_pnl <= -portfolio_capital * portfolio_stop_loss_pct
                THEN 'Portfolio Exit - Loss'
        END AS exit_reason
    FROM portfolio_mtm_pnl
    CROSS JOIN config
    WHERE total_pnl >= portfolio_capital * portfolio_profit_target_pct
       OR total_pnl <= -portfolio_capital * portfolio_stop_loss_pct
    ORDER BY date, expiry, time
),

/* ============================================================
   2. Legs open at portfolio exit time
   ============================================================ */
open_legs_at_exit AS (
    SELECT l.*
    FROM mv_all_entries_sl_tracking_adjusted l
    JOIN portfolio_exit_trigger p
      ON p.date  = l.trade_date
     AND p.expiry = l.expiry_date
    WHERE l.entry_time <= p.exit_time
      AND (l.exit_time IS NULL OR p.exit_time <= l.exit_time)
),

/* ============================================================
   3. Price legs at portfolio exit time
   ============================================================ */
exit_priced_legs AS (
    SELECT 
        l.trade_date,
        l.expiry_date,
        l.breakout_time,
        l.entry_time,
        l.spot_price,
        l.option_type,
        l.strike,
        l.entry_price,
        l.sl_level,
        l.entry_round,
        l.leg_type,
        l.transaction_type,
        p.exit_time,
        o.open AS exit_price,
        p.exit_reason
    FROM open_legs_at_exit l
    JOIN portfolio_exit_trigger p
      ON p.date  = l.trade_date
     AND p.expiry = l.expiry_date
    JOIN v_nifty_options_filtered o
      ON o.date  = l.trade_date
     AND o.expiry = l.expiry_date
     AND o.option_type = l.option_type
     AND o.strike      = l.strike
     AND o.time        = p.exit_time
),

/* ============================================================
   4. Portfolio exit PnL
   ============================================================ */
exit_legs_with_pnl AS (
    SELECT 
        e.*,
        CASE 
            WHEN e.transaction_type = 'SELL'
                THEN ROUND((e.entry_price - e.exit_price) * c.lot_size * c.no_of_lots, 2)
            ELSE ROUND((e.exit_price - e.entry_price) * c.lot_size * c.no_of_lots, 2)
        END AS pnl_amount
    FROM exit_priced_legs e
    CROSS JOIN config c
),

/* ============================================================
   5. Remove invalid legs (entry after exit)
   ============================================================ */
invalid_legs AS (
    SELECT *
    FROM mv_all_entries_sl_tracking_adjusted l
    JOIN portfolio_exit_trigger p
      ON p.date  = l.trade_date
     AND p.expiry = l.expiry_date
    WHERE l.entry_time > p.exit_time
),

valid_legs AS (
    SELECT *
    FROM mv_all_entries_sl_tracking_adjusted l
    WHERE NOT EXISTS (
        SELECT 1
        FROM invalid_legs i
        WHERE i.trade_date  = l.trade_date
          AND i.expiry_date = l.expiry_date
          AND i.option_type = l.option_type
          AND i.strike      = l.strike
          AND i.entry_round = l.entry_round
          AND i.leg_type    = l.leg_type
    )
),

/* ============================================================
   6. Combine normal exits + portfolio exits
   ============================================================ */
all_leg_exits AS (
    SELECT * FROM valid_legs
    UNION ALL
    SELECT * FROM exit_legs_with_pnl
),

/* ============================================================
   7. Hedge exit when all RE-ENTRY legs are done
   ============================================================ */
reentry_exit_summary AS (
    SELECT
        trade_date,
        expiry_date,
        MAX(exit_time) AS max_exit_time,
        COUNT(*) FILTER (WHERE exit_time IS NOT NULL) AS exited_count,
        COUNT(*) AS total_reentry_legs
    FROM all_leg_exits
    WHERE leg_type = 'RE-ENTRY'
    GROUP BY trade_date, expiry_date
),

hedge_exit_on_reentry_completion AS (
    SELECT
        h.trade_date,
        h.expiry_date,
        h.breakout_time,
        h.entry_time,
        h.spot_price,
        h.option_type,
        h.strike,
        h.entry_price,
        '0' AS sl_level,
        h.entry_round,
        h.leg_type,
        h.transaction_type,
        r.max_exit_time AS exit_time,
        o.open AS exit_price,
        'EXIT - ALL REENTRY COMPLETE' AS exit_reason,
        CASE 
            WHEN h.transaction_type = 'SELL'
                THEN ROUND((h.entry_price - o.open) * c.lot_size * c.no_of_lots, 2)
            ELSE ROUND((o.open - h.entry_price) * c.lot_size * c.no_of_lots, 2)
        END AS pnl_amount
    FROM all_leg_exits h
    JOIN reentry_exit_summary r
      ON r.trade_date  = h.trade_date
     AND r.expiry_date = h.expiry_date
    JOIN v_nifty_options_filtered o
      ON o.date  = h.trade_date
     AND o.expiry = h.expiry_date
     AND o.option_type = h.option_type
     AND o.strike      = h.strike
     AND o.time        = r.max_exit_time
    CROSS JOIN config c
    WHERE h.leg_type = 'HEDGE-REENTRY'
      AND r.exited_count = r.total_reentry_legs
      AND r.max_exit_time <> c.eod_time
),

/* ============================================================
   8. Final de-duplication
   ============================================================ */
final_legs AS (
    SELECT * FROM valid_legs
    UNION ALL
    SELECT * FROM exit_legs_with_pnl
    UNION ALL
    SELECT * FROM hedge_exit_on_reentry_completion
),

ranked_legs AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY
                   trade_date,
                   expiry_date,
                   option_type,
                   strike,
                   leg_type,
                   entry_round
               ORDER BY exit_time
           ) AS rn
    FROM final_legs
)

SELECT
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
    ROUND(
        SUM(pnl_amount) OVER (PARTITION BY trade_date, expiry_date),
        2
    ) AS total_pnl_per_day
FROM ranked_legs
WHERE rn = 1
ORDER BY trade_date, expiry_date, entry_time, option_type, strike;


-- =====================================================
-- 11. CREATE STORED PROCEDURES
-- =====================================================

-- File: sp_insert_sl_legs_into_book.sql
DELETE FROM strategy_leg_book WHERE strategy_name = 'default';
------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE insert_sl_legs_into_book(p_strategy_name TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO strategy_leg_book (
        strategy_name,
        trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        exit_time,
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
        trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        exit_time,
        option_type,
        strike,
        entry_price,
        exit_price,
        transaction_type,
        leg_type,
        entry_round,
        exit_reason
    FROM mv_all_legs_round1 sl
    WHERE NOT EXISTS (
        SELECT 1
        FROM strategy_leg_book b
        WHERE b.strategy_name = p_strategy_name
          AND b.trade_date = sl.trade_date
          AND b.expiry_date = sl.expiry_date
          AND b.option_type = sl.option_type
          AND b.strike = sl.strike
          AND b.entry_round = sl.entry_round
          AND b.leg_type = sl.leg_type
    );

    RAISE NOTICE '✅ SL legs inserted into strategy_leg_book for strategy %', p_strategy_name;
END;
$$;


CALL insert_sl_legs_into_book('default');

-- File: sp_run_reentry_loop.sql
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

        -- Refresh triggered breakouts after consolidation
      --  REFRESH MATERIALIZED VIEW mv_reentry_triggered_breakouts;

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

-- File: sp_run_strategy.sql
CREATE OR REPLACE PROCEDURE sp_run_strategy()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_from_date DATE;
    v_to_date DATE;
BEGIN
    FOR rec IN SELECT * FROM strategy_settings LOOP
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
        )
        SELECT
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
        FROM strategy_settings
        WHERE strategy_name = rec.strategy_name;

        -- CRITICAL: Refresh v_strategy_config before dependent views
        REFRESH MATERIALIZED VIEW v_strategy_config;

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
        REFRESH MATERIALIZED VIEW mv_live_prices_entry_round1;
        REFRESH MATERIALIZED VIEW mv_entry_sl_hits_round1;
        REFRESH MATERIALIZED VIEW mv_entry_sl_executions_round1;
        REFRESH MATERIALIZED VIEW mv_entry_open_legs_round1;
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
        REFRESH MATERIALIZED VIEW mv_reentry_triggered_breakouts;
        REFRESH MATERIALIZED VIEW mv_reentry_base_strike_selection;
        REFRESH MATERIALIZED VIEW mv_reentry_legs_and_hedge_legs;
        REFRESH MATERIALIZED VIEW mv_reentry_live_prices;
        REFRESH MATERIALIZED VIEW mv_reentry_breakout_context;
        REFRESH MATERIALIZED VIEW mv_reentry_sl_hits;
        REFRESH MATERIALIZED VIEW mv_reentry_sl_executions;
        REFRESH MATERIALIZED VIEW mv_reentry_open_legs;
        REFRESH MATERIALIZED VIEW mv_reentry_profit_booking;
        REFRESH MATERIALIZED VIEW mv_reentry_eod_close;
        REFRESH MATERIALIZED VIEW mv_reentry_final_exit;
        REFRESH MATERIALIZED VIEW mv_reentry_legs_stats;
        REFRESH MATERIALIZED VIEW mv_hedge_reentry_exit_on_all_entry_sl;
        REFRESH MATERIALIZED VIEW mv_hedge_reentry_exit_on_partial_conditions;
        REFRESH MATERIALIZED VIEW mv_hedge_reentry_closed_legs;
        REFRESH MATERIALIZED VIEW mv_hedge_reentry_eod_exit;
        REFRESH MATERIALIZED VIEW mv_reentry_exit_on_partial_hedge;
        REFRESH MATERIALIZED VIEW mv_double_buy_legs_reentry;
        REFRESH MATERIALIZED VIEW mv_rehedge_trigger_reentry;
        REFRESH MATERIALIZED VIEW mv_rehedge_candidate_reentry;
        REFRESH MATERIALIZED VIEW mv_rehedge_selected_reentry;
        REFRESH MATERIALIZED VIEW mv_rehedge_leg_reentry;
        REFRESH MATERIALIZED VIEW mv_rehedge_eod_exit_reentry;
        REFRESH MATERIALIZED VIEW mv_all_legs_reentry;
        CALL sp_run_reentry_loop(rec.strategy_name);
        REFRESH MATERIALIZED VIEW mv_entry_leg_live_prices;
        REFRESH MATERIALIZED VIEW mv_all_entries_sl_tracking_adjusted;
        REFRESH MATERIALIZED VIEW mv_portfolio_mtm_pnl;
        REFRESH MATERIALIZED VIEW mv_portfolio_final_pnl;

        -- Store final results
        INSERT INTO strategy_run_results (
            strategy_name,
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
        FROM mv_portfolio_final_pnl;

        RAISE NOTICE 'Completed run for strategy %', rec.strategy_name;
    END LOOP;

    RAISE NOTICE 'All strategies processed.';
END;
$$;


-- =====================================================
-- 12. CREATE INDEXES
-- =====================================================

-- File: create_indexes_matviews.sql
-- Create helpful indexes for materialized views
-- Non-concurrent indexes (matviews depend on each other; create without CONCURRENTLY)

CREATE INDEX IF NOT EXISTS idx_mv_base_strike_selection_trade_expiry ON public.mv_base_strike_selection (trade_date, expiry_date);

CREATE INDEX IF NOT EXISTS idx_mv_entry_and_hedge_legs_trade_expiry ON public.mv_entry_and_hedge_legs (trade_date, expiry_date);
CREATE INDEX IF NOT EXISTS idx_mv_entry_and_hedge_legs_trade_expiry_leg ON public.mv_entry_and_hedge_legs (trade_date, expiry_date, leg_type);

CREATE INDEX IF NOT EXISTS idx_mv_ranked_breakouts_trade_entry_round ON public.mv_ranked_breakouts_with_rounds (trade_date, entry_round);

-- Add index for reentry view too
CREATE INDEX IF NOT EXISTS idx_mv_ranked_breakouts_reentry_trade_entry_round ON public.mv_ranked_breakouts_with_rounds_for_reentry (trade_date, entry_round);

-- Optional: index by expiry on base strike selection for faster joins
CREATE INDEX IF NOT EXISTS idx_mv_base_strike_selection_expiry ON public.mv_base_strike_selection (expiry_date);


-- =====================================================
-- 13. SET UP DEFAULT DATA
-- =====================================================

-- File: update_strategy_settings_defaults.sql
-- Alter column defaults and update existing default strategy row
ALTER TABLE public.strategy_settings
    ALTER COLUMN box_sl_trigger_pct SET DEFAULT 25,
    ALTER COLUMN box_sl_hard_pct SET DEFAULT 35,
    ALTER COLUMN width_sl_pct SET DEFAULT 40;

-- Update existing row for default strategy
UPDATE public.strategy_settings
SET box_sl_trigger_pct = 25,
    box_sl_hard_pct = 35,
    width_sl_pct = 40,
    sl_percentage = 20,
    switch_pct = 20
WHERE strategy_name = 'default';

-- Show updated row
SELECT strategy_name, sl_percentage, box_sl_trigger_pct, box_sl_hard_pct, width_sl_pct, switch_pct
FROM public.strategy_settings
WHERE strategy_name = 'default';

-- File: upsert_runtime_strategy_config_default.sql
-- Upsert values into runtime_strategy_config for the default strategy
INSERT INTO public.runtime_strategy_config (
    strategy_name,
    sl_percentage,
    box_sl_trigger_pct,
    box_sl_hard_pct,
    width_sl_pct,
    switch_pct
)
VALUES (
    'default',
    20,
    25,
    35,
    40,
    20
)
ON CONFLICT (strategy_name) DO UPDATE SET
    sl_percentage = EXCLUDED.sl_percentage,
    box_sl_trigger_pct = EXCLUDED.box_sl_trigger_pct,
    box_sl_hard_pct = EXCLUDED.box_sl_hard_pct,
    width_sl_pct = EXCLUDED.width_sl_pct,
    switch_pct = EXCLUDED.switch_pct;

-- Show runtime row
SELECT strategy_name, sl_percentage, box_sl_trigger_pct, box_sl_hard_pct, width_sl_pct, switch_pct
FROM public.runtime_strategy_config
WHERE strategy_name = 'default';

-- File: set_strategy_settings_parent_values.sql
-- Update parent strategy_settings with whole-number percentage values
UPDATE public.strategy_settings
SET sl_percentage = 20,
    box_sl_trigger_pct = 25,
    box_sl_hard_pct = 35,
    width_sl_pct = 40,
    switch_pct = 20
WHERE strategy_name = 'default';

-- Show updated row
SELECT strategy_name, sl_percentage, box_sl_trigger_pct, box_sl_hard_pct, width_sl_pct, switch_pct
FROM public.strategy_settings
WHERE strategy_name = 'default';


-- =====================================================
-- 14. CREATE FUNCTIONS
-- =====================================================

-- File: get_heikin_ashi.sql
-- Heikin-Ashi generator function
CREATE OR REPLACE FUNCTION public.get_heikin_ashi(interval_minutes INT)
RETURNS TABLE (
  trade_date DATE,
  candle_time TIME,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  ha_open NUMERIC,
  ha_high NUMERIC,
  ha_low NUMERIC,
  ha_close NUMERIC
)
LANGUAGE SQL
AS
$$
WITH RECURSIVE base1 AS (
  SELECT 
    date + time::time AS trade_time,
    open, high, low, close
  FROM public."Nifty50"
  WHERE time >= '09:15:00'
),
with_bucket AS (
  SELECT
    date_trunc('day', trade_time) AS trade_date,
    (DATE_TRUNC('day', trade_time) + INTERVAL '9 hours 15 minutes') +
      FLOOR(
        EXTRACT(EPOCH FROM (trade_time - DATE_TRUNC('day', trade_time) - INTERVAL '9 hours 15 minutes'))
        / (interval_minutes * 60)
      ) * (interval_minutes || ' minutes')::interval AS candle_time,
    trade_time,
    open, high, low, close
  FROM base1
),
with_windowed AS (
  SELECT * ,
    FIRST_VALUE(open) OVER w AS first_open,
    LAST_VALUE(close) OVER w AS last_close
  FROM with_bucket
  WINDOW w AS (
    PARTITION BY trade_date, candle_time ORDER BY trade_time 
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  )
),
mv_nifty_candles AS (
  SELECT
    trade_date,
    candle_time,
    MIN(first_open) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    MAX(last_close) AS close
  FROM with_windowed
  GROUP BY trade_date, candle_time
),
base AS (
  SELECT * FROM mv_nifty_candles
),
first_candle AS (
  SELECT * FROM base ORDER BY trade_date, candle_time LIMIT 1
),
recursive_ha AS (
  SELECT 
    b.trade_date,
    b.candle_time,
    b.open, b.high, b.low, b.close,
    (b.open + b.high + b.low + b.close) / 4.0 AS ha_close,
    b.open AS ha_open,
    GREATEST(b.high, b.open, (b.open + b.high + b.low + b.close)/4.0) AS ha_high,
    LEAST(b.low, b.open, (b.open + b.high + b.low + b.close)/4.0) AS ha_low
  FROM first_candle b
  UNION ALL
  SELECT 
    b.trade_date,
    b.candle_time,
    b.open, b.high, b.low, b.close,
    (b.open + b.high + b.low + b.close) / 4.0 AS ha_close,
    (r.ha_open + r.ha_close)/2.0 AS ha_open,
    GREATEST(b.high, (r.ha_open + r.ha_close)/2.0, (b.open + b.high + b.low + b.close)/4.0) AS ha_high,
    LEAST(b.low, (r.ha_open + r.ha_close)/2.0, (b.open + b.high + b.low + b.close)/4.0) AS ha_low
  FROM base b
  JOIN recursive_ha r
    ON b.candle_time = (
      SELECT MIN(c2.candle_time)
      FROM base c2
      WHERE c2.candle_time > r.candle_time
    )
)
SELECT 
  trade_date,
  candle_time::time,
  open, high, low, close,
  ROUND(ha_open, 2) AS ha_open,
  ROUND(ha_high, 2) AS ha_high,
  ROUND(ha_low, 2) AS ha_low,
  ROUND(ha_close, 2) AS ha_close
FROM recursive_ha
ORDER BY trade_date, candle_time;
$$;

-- =====================================================
-- Database initialization completed!
-- =====================================================
