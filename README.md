# New_BackTest_Pulse — Postgres-backed backtester

This repository contains a comprehensive backtesting platform for options trading strategies. It executes strategy logic in Postgres (SQL) for optimal performance and provides a web interface for easy analysis with **day-wise performance breakdowns**.

## 🚀 Quick Start

### Web Application (Recommended)
```powershell
# Install dependencies
pip install -r requirements.txt

# Set database environment variables
$env:PGHOST='localhost'
$env:PGPORT='5432'
$env:PGDATABASE='Backtest_Pulse'
$env:PGUSER='postgres'
$env:PGPASSWORD='Alliswell@28'

# Run the web application
python app.py
```

Access the web interface at: http://localhost:5000

### CLI Backtesting
```powershell
python -m src.backtest --sql ./examples/sample_strategy.sql --start 2020-01-01 --end 2020-12-31 --initial-capital 100000
```

## 🎯 Key Features

- **Day-wise Performance Analysis**: Detailed breakdown showing Total Trades, Total PnL, Best Trade, and Worst Trade for each trading date
- **Session-Isolated Results**: Each backtest run shows only its own results (automatic cleanup)
- **Web Interface**: User-friendly upload and analysis interface
- **Excel Report Generation**: Comprehensive downloads with daily analysis and strategy rankings
- **PostgreSQL Optimization**: Heavy computations handled in SQL for maximum performance

## 📁 Files of interest

- `app.py` — Main Flask web application with day-wise analysis
- `src/db.py` — Postgres connection helpers and helpers to run SQL files
- `src/strategy_executor.py` — Loads and runs user SQL, returns `pandas.DataFrame` signals
- `src/sim.py` — Lightweight event-driven simulator that consumes signals and updates portfolio
- `src/backtest.py` — CLI orchestration for direct SQL-based backtesting

## 📊 Web Application Usage

1. **Upload Strategy CSV**: Configure multiple strategies in a CSV file
2. **Run Backtest**: Automated execution with real-time progress tracking
3. **View Results**: Day-by-day performance breakdown for each strategy
4. **Download Reports**: Excel files with detailed analysis and rankings

## 🔧 Technical Notes

- This platform expects heavy data processing (joins, aggregations, indicator calculations) to be expressed in SQL for optimal Postgres performance
- Each backtest session is isolated - results are cleared between runs for focused analysis
- The web interface provides day-wise breakdowns instead of aggregated date ranges

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
