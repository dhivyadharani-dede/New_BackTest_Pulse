-- create_runtime_strategy_dates.sql
-- This script creates the runtime_strategy_dates table if it does not exist.

CREATE TABLE IF NOT EXISTS runtime_strategy_dates (
    strategy_name TEXT PRIMARY KEY,
    from_date     DATE NOT NULL,
    to_date       DATE NOT NULL,
    created_at    TIMESTAMP DEFAULT now()
);
