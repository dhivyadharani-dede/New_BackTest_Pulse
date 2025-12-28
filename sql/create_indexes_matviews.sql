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
