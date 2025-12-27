# New_BackTest_Pulse — Postgres-backed backtester

This repository contains a minimal, scalable backtesting scaffold that executes strategy logic in Postgres (SQL) and simulates trades in Python. The goal is to let Postgres handle heavy data joins/aggregations and keep Python for simulation and reporting.

Getting started

1. Create a Python venv and install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. Set Postgres connection environment variables:

```powershell
$env:PGHOST='localhost'
$env:PGPORT='5432'
$env:PGDATABASE='backtest_db'
$env:PGUSER='user'
$env:PGPASSWORD='pass'
```

3. Run a backtest using your SQL file that produces signal rows (timestamp, symbol, signal, price, size):

```powershell
python -m src.backtest --sql ./examples/sample_strategy.sql --start 2020-01-01 --end 2020-12-31 --initial-capital 100000
```

Files of interest

- `src/db.py` — Postgres connection helpers and helpers to run SQL files.
- `src/strategy_executor.py` — Loads and runs user SQL, returns `pandas.DataFrame` signals.
- `src/sim.py` — Lightweight event-driven simulator that consumes signals and updates portfolio.
- `src/backtest.py` — CLI orchestration.

Notes

- This scaffold expects the heavy lifting (joins, aggregations, indicator calculations) to be expressed in SQL so Postgres processes millions of rows efficiently.
- Provide your SQL that returns ordered signals with at least: `ts` (timestamp), `symbol`, `signal` (buy/sell/close), `price`, and optional `size`.

Please provide your SQL file or point to where it's stored so I can adapt the executor for any expected parameters or temp tables.

Bulk load and partitioning

To create the parent table and yearly partitions, then bulk-load CSVs while auto-creating missing partitions:

```powershell
python .\scripts\create_nifty50_partitions.py --start 2015 --end 2026
python .\scripts\bulk_load_nifty50.py --csv .\data\nifty50_2015_2020.csv --partitioning yearly

Create monthly partitions for options and (optionally) pre-create them:

```powershell
python .\scripts\create_nifty_options_partitions.py --start 2021-01 --end 2025-12
```

The options parent table definition is in `sql/create_nifty_options.sql`.

Bulk-load options CSVs (filtering `option_type` to P and C):

```powershell
python .\scripts\bulk_load_nifty_options.py --csv "C:\path\to\Nifty_options_2021_2025.csv" --chunksize 100000
```

The loader auto-creates monthly partitions and only imports rows where `option_type` is `P` or `C` (case-insensitive).
```
