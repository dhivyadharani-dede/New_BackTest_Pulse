# Copilot Instructions for New_BackTest_Pulse

## Project Overview
New_BackTest_Pulse is a Postgres-backed backtesting scaffold that favors executing heavy data work in SQL and using Python for simulation and reporting. The design is optimized to handle millions of rows by pushing aggregations and indicator calculations into Postgres.

## Architecture (what matters to an AI agent)
- Data lives in Postgres: normalized `market_data` tables (OHLCV) and any precomputed indicators. Queries should be written to leverage indexes and server-side filtering.
- Strategy logic: provided as SQL files (or strings). SQL should return ordered signal rows: `ts`, `symbol`, `signal`, `price`, optional `size`.
- Simulation engine: Python consumes the SQL results (DataFrame) and simulates fills, PnL, slippage, and commissions.

## Key files to inspect and edit
- `src/db.py`: connection helpers and a server-side cursor helper for streaming large result sets.
- `src/strategy_executor.py`: loads and executes SQL, returns `pandas.DataFrame`.
- `src/backtest.py`: CLI orchestrator that runs the SQL and invokes the simulator.
- `src/sim.py`: simple portfolio and position logic used by the simulator.
- `examples/sample_strategy.sql`: template showing expected output schema and parameter usage.

## Developer workflows (concrete commands)
- Create virtualenv and install deps:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

- Set Postgres environment variables and run backtest with an SQL file:

```powershell
$env:PGHOST='localhost'
$env:PGPORT='5432'
$env:PGDATABASE='backtest_db'
$env:PGUSER='user'
$env:PGPASSWORD='pass'
python -m src.backtest --sql ./examples/sample_strategy.sql --start 2020-01-01 --end 2020-12-31 --initial-capital 100000
```

## Project-specific conventions and patterns
- Always express heavy computations in SQL. The Python runtime is intentionally lightweight: it expects the SQL to return final signal rows.
- SQL parameter placeholders: the sample uses `:start` / `:end` style. The executor passes parameters as a dictionary — adapt to your DB client placeholder style if needed.
- Result columns expected by the simulator: lowercase `ts` (timestamp), `symbol`, `signal` (`buy`/`sell`/`close`), `price`, optional numeric `size`.

## Integration points & performance notes
- Use Postgres server-side cursors (see `src/db.py`) or `LIMIT/OFFSET` patterns for very large result sets.
- Prefer returning aggregated signals per timestamp rather than raw tick-by-tick rows when possible to reduce Python-side load.
- If strategy SQL writes intermediate results to temp tables, ensure `CREATE TEMP TABLE` usage is compatible with the connection/session lifetime used by `src/db.get_conn()`.

## If you (human) provide SQL
- Share the SQL file path and any required parameters (start/end, symbol list). If SQL references temp tables or expects session-local state, note that the executor opens a single connection per run.

## What the AI agent should do first
1. Inspect `examples/sample_strategy.sql` to understand expected columns and parameter usage.
2. If SQL is large or writes temp tables, adapt `src/db.get_conn()` usage to keep the same session for the whole run.
3. Optimize SQL for Postgres (indexes, avoid unnecessary sorts) before moving logic into Python.

Please provide your SQL file and any details about expected params or temp table usage so I can adapt the executor and optimize streaming for your dataset.