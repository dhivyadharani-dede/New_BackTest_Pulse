CREATE MATERIALIZED VIEW IF NOT EXISTS mv_missing_strategy_results AS
WITH actuals AS (
    SELECT
        strategy_name,
        trade_date,
        entry_round,

        COUNT(*) FILTER (WHERE leg_type = 'ENTRY') AS entry_legs,
        COUNT(*) FILTER (WHERE leg_type = 'HEDGE') AS hedge_legs,

        COUNT(*) FILTER (
            WHERE leg_type = 'ENTRY'
              AND exit_reason = 'SL_HIT'
        ) AS entry_sl_hits,

        COUNT(*) FILTER (WHERE leg_type = 'DOUBLE_BUY')        AS double_buy_legs,
        COUNT(*) FILTER (WHERE leg_type = 'REHEDGE')           AS rehedge_legs,

        COUNT(*) FILTER (WHERE leg_type = 'RE-ENTRY')          AS reentry_legs,
        COUNT(*) FILTER (WHERE leg_type = 'HEDGE-RE-ENTRY')   AS hedge_reentry_legs,
        COUNT(*) FILTER (WHERE leg_type = 'DOUBLE_BUY_REENTRY') AS double_buy_reentry_legs,
        COUNT(*) FILTER (WHERE leg_type = 'REHEDGE_RENTRY')     AS rehedge_reentry_legs

    FROM mv_portfolio_final_pnl
    GROUP BY strategy_name, trade_date, entry_round
),
expected AS (
    SELECT
        strategy_name,
        num_entry_legs,
        num_hedge_legs
    FROM strategy_settings
),
violations AS (
    SELECT
        a.strategy_name,
        a.trade_date,
        a.entry_round,

        -- ENTRY legs missing
        CASE
            WHEN a.entry_legs < e.num_entry_legs
            THEN 'MISSING_ENTRY_LEGS'
        END AS issue_entry,

        -- HEDGE legs missing
        CASE
            WHEN a.hedge_legs < e.num_hedge_legs
            THEN 'MISSING_HEDGE_LEGS'
        END AS issue_hedge,

        -- DOUBLE BUY required if any ENTRY SL hit
        CASE
            WHEN a.entry_sl_hits > 0
             AND a.double_buy_legs = 0
            THEN 'MISSING_DOUBLE_BUY'
        END AS issue_double_buy,

        -- REHEDGE required if ALL ENTRY legs hit SL
        CASE
            WHEN a.entry_sl_hits = e.num_entry_legs
             AND a.rehedge_legs = 0
            THEN 'MISSING_REHEDGE'
        END AS issue_rehedge,

        -- RE-ENTRY required for entry_round > 1
        CASE
            WHEN a.entry_round > 1
             AND a.reentry_legs = 0
            THEN 'MISSING_REENTRY'
        END AS issue_reentry,

        -- HEDGE-RE-ENTRY required if RE-ENTRY exists
        CASE
            WHEN a.reentry_legs > 0
             AND a.hedge_reentry_legs < e.num_hedge_legs
            THEN 'MISSING_HEDGE_REENTRY'
        END AS issue_hedge_reentry,

        -- DOUBLE BUY REENTRY required if REENTRY SL hit
        CASE
            WHEN a.entry_sl_hits > 0
             AND a.double_buy_reentry_legs = 0
             AND a.entry_round > 1
            THEN 'MISSING_DOUBLE_BUY_REENTRY'
        END AS issue_double_buy_reentry,

        -- REHEDGE REENTRY required if ALL ENTRY SL hit in reentry round
        CASE
            WHEN a.entry_sl_hits = e.num_entry_legs
             AND a.rehedge_reentry_legs = 0
             AND a.entry_round > 1
            THEN 'MISSING_REHEDGE_REENTRY'
        END AS issue_rehedge_reentry

    FROM actuals a
    JOIN expected e
      ON e.strategy_name = a.strategy_name
)

SELECT
    strategy_name,
    trade_date,
    entry_round,
    issue
FROM (
    SELECT
        strategy_name,
        trade_date,
        entry_round,
        UNNEST(ARRAY[
            issue_entry,
            issue_hedge,
            issue_double_buy,
            issue_rehedge,
            issue_reentry,
            issue_hedge_reentry,
            issue_double_buy_reentry,
            issue_rehedge_reentry
        ]) AS issue
    FROM violations
) x
WHERE issue IS NOT NULL;
