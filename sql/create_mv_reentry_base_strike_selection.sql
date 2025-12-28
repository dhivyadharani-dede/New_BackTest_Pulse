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