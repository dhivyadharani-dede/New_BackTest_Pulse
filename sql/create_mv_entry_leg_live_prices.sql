CREATE MATERIALIZED VIEW mv_entry_leg_live_prices AS
WITH legs AS (
    SELECT * FROM mv_all_legs_REENTRY
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