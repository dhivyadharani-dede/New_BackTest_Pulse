import argparse
from pathlib import Path
from .strategy_executor import load_sql, run_strategy_sql
from .sim import Portfolio
import pandas as pd


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--sql", required=True, help="Path to SQL file implementing strategy logic")
    p.add_argument("--start", required=False)
    p.add_argument("--end", required=False)
    p.add_argument("--initial-capital", type=float, default=100000)
    p.add_argument("--commission", type=float, default=1.0)
    p.add_argument("--slippage", type=float, default=0.0)
    p.add_argument("--output", default="backtest_results.csv")
    return p.parse_args()


def run_backtest(sql_path: str, initial_capital: float = 100000.0, commission: float = 1.0, slippage: float = 0.0, params: dict | None = None, output: str = "backtest_results.csv"):
    sql_text = load_sql(sql_path)
    df = run_strategy_sql(sql_text, params=params)
    if df.empty:
        print("No signals returned by SQL.")
        return

    # Normalize column names
    df.columns = [c.lower() for c in df.columns]
    # Expect ts, symbol, signal, price, size(optional)
    df = df.sort_values("ts")

    port = Portfolio(cash=initial_capital)

    for _, row in df.iterrows():
        ts = row["ts"]
        symbol = row["symbol"]
        signal = str(row.get("signal", "")).lower()
        price = float(row["price"])
        size = float(row.get("size", 1.0))

        if signal in ("buy", "long"):
            port.apply_trade(ts, symbol, "buy", price, size, commission=commission, slippage=slippage)
        elif signal in ("sell", "short"):
            port.apply_trade(ts, symbol, "sell", price, size, commission=commission, slippage=slippage)
        elif signal in ("close", "exit"):
            port.apply_trade(ts, symbol, "close", price, size, commission=commission, slippage=slippage)

    out_df = port.to_frame()
    out_df.to_csv(output, index=False)
    print(f"Backtest finished. Results written to {output}")


if __name__ == "__main__":
    args = parse_args()
    run_backtest(args.sql, initial_capital=args.initial_capital, commission=args.commission, slippage=args.slippage, params={"start": args.start, "end": args.end}, output=args.output)
