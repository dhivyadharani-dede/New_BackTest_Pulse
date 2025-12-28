DROP MATERIALIZED VIEW IF EXISTS public.mv_reentry_legs_and_hedge_legs CASCADE;
CREATE MATERIALIZED VIEW mv_reentry_legs_and_hedge_legs AS
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
        'RE-ENTRY'::TEXT AS leg_type,
        'SELL'::TEXT AS transaction_type
    FROM mv_reentry_base_strike_selection s
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