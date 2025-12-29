import psycopg2
import os

conn = psycopg2.connect(
    host=os.environ.get('PGHOST'),
    port=os.environ.get('PGPORT'),
    dbname=os.environ.get('PGDATABASE'),
    user=os.environ.get('PGUSER'),
    password=os.environ.get('PGPASSWORD')
)

with conn.cursor() as cur:
    # Check current leg counts
    cur.execute('SELECT entry_round, COUNT(*) FROM strategy_leg_book GROUP BY entry_round ORDER BY entry_round')
    rounds = cur.fetchall()
    print('Legs by round BEFORE reentry:')
    for r in rounds:
        print(f'  Round {r[0]}: {r[1]} legs')

    # Run the reentry procedure
    print('')
    print('Running reentry procedure...')
    cur.execute("CALL sp_run_reentry_loop('default')")
    conn.commit()
    print('Reentry procedure completed')

    # Check leg counts after
    cur.execute('SELECT entry_round, COUNT(*) FROM strategy_leg_book GROUP BY entry_round ORDER BY entry_round')
    rounds = cur.fetchall()
    print('')
    print('Legs by round AFTER reentry:')
    for r in rounds:
        print(f'  Round {r[0]}: {r[1]} legs')

conn.close()