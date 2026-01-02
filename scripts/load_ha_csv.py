import sys
from pathlib import Path
import pandas as pd
import io

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.db import get_conn

def copy_df_to_table(conn, df: pd.DataFrame, table: str):
    if df is None or df.empty:
        return 0
    buf = io.StringIO()
    # prepare CSV rows: trade_date, candle_time, open, high, low, close, ha_open, ha_high, ha_low, ha_close
    df_to_write = df[['trade_date','candle_time','open','high','low','close','ha_open','ha_high','ha_low','ha_close']]
    df_to_write.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur = conn.cursor()
    # use COPY with CSV
    cur.copy_expert(f"COPY public.{table} (trade_date, candle_time, open, high, low, close, ha_open, ha_high, ha_low, ha_close) FROM STDIN WITH CSV", buf)
    conn.commit()
    return len(df_to_write)

def load_ha_csv(file_path: str, table: str):
    # Read CSV
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} rows from {file_path}")
    
    # Rename columns
    df = df.rename(columns={
        'open(RAW)': 'open',
        'high(RAW)': 'high',
        'low(RAW)': 'low',
        'close(RAW)': 'close',
        'Date': 'trade_date',
        'time': 'candle_time',
        'OPEN(HA)': 'ha_open',
        'HIGH(HA)': 'ha_high',
        'LOW(HA)': 'ha_low',
        'CLOSE(HA)': 'ha_close'
    })
    
    # Select only the needed columns
    df = df[['trade_date', 'candle_time', 'open', 'high', 'low', 'close', 'ha_open', 'ha_high', 'ha_low', 'ha_close']]
    
    # Convert trade_date from int to date
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
    
    # Convert candle_time to time object
    df['candle_time'] = pd.to_datetime(df['candle_time'], format='%H:%M:%S').dt.time
    
    # Insert into DB
    with get_conn() as conn:
        inserted = copy_df_to_table(conn, df, table)
    
    print(f"Inserted {inserted} rows into {table}")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', required=True)
    parser.add_argument('--table', default='ha_big')
    args = parser.parse_args()
    
    load_ha_csv(args.file, args.table)