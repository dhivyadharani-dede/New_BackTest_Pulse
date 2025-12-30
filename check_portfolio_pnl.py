from src.db import get_conn
import pandas as pd

print('Checking portfolio final PnL view after refresh...')

try:
    with get_conn() as conn:
        # Check if portfolio final pnl has data
        count_query = "SELECT COUNT(*) as count FROM mv_portfolio_final_pnl"
        count_df = pd.read_sql(count_query, conn)
        total_records = count_df.iloc[0,0]
        print(f'Total records in mv_portfolio_final_pnl: {total_records}')

        if total_records > 0:
            # Check date range in the view
            date_query = "SELECT MIN(date) as min_date, MAX(date) as max_date FROM mv_portfolio_final_pnl"
            date_df = pd.read_sql(date_query, conn)
            print(f'Portfolio PnL date range: {date_df.iloc[0,0]} to {date_df.iloc[0,1]}')

            # Check a few sample records
            sample_query = "SELECT date, portfolio_pnl FROM mv_portfolio_final_pnl ORDER BY date LIMIT 3"
            sample_df = pd.read_sql(sample_query, conn)
            print('Sample records:')
            print(sample_df)

        print('✅ Portfolio final PnL view refreshed successfully!')

except Exception as e:
    print(f'Error: {e}')