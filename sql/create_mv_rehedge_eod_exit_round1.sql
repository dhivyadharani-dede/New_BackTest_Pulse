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
