-- Call the batched strategy runner procedure
-- Default batch type is 'month', can be changed to 'week'

-- For monthly batches (default):
CALL sp_run_strategy_batched();

-- For weekly batches:
-- CALL sp_run_strategy_batched('week');