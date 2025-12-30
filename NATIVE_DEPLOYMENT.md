# New_BackTest_Pulse - Native Deployment Guide (Without Docker)

This guide explains how to deploy the New_BackTest_Pulse application directly on your system without using Docker.

## Prerequisites

### System Requirements
- **Operating System**: Linux, macOS, or Windows
- **Python**: Version 3.8 or higher
- **PostgreSQL**: Version 12 or higher
- **Memory**: At least 4GB RAM recommended
- **Disk Space**: At least 2GB free space

### Required Software
1. **PostgreSQL Database Server**
2. **Python 3.8+**
3. **Git** (for cloning the repository)

## Step 1: Install PostgreSQL

### On Ubuntu/Debian:
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

### On CentOS/RHEL:
```bash
sudo yum install postgresql-server postgresql-contrib
sudo postgresql-setup initdb
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

### On macOS (using Homebrew):
```bash
brew install postgresql
brew services start postgresql
```

### On Windows:
Download and install PostgreSQL from: https://www.postgresql.org/download/windows/

## Step 2: Set Up PostgreSQL Database

### Create Database and User
```bash
# Switch to postgres user
sudo -u postgres psql

# In PostgreSQL shell:
CREATE DATABASE "Backtest_Pulse";
CREATE USER postgres WITH PASSWORD 'Alliswell@28';
GRANT ALL PRIVILEGES ON DATABASE "Backtest_Pulse" TO postgres;
ALTER USER postgres CREATEDB;
\q
```

### Configure PostgreSQL (Optional - for better performance)
Edit `/etc/postgresql/12/main/postgresql.conf` (adjust version number as needed):
```
# Increase shared buffers
shared_buffers = 256MB

# Increase work memory
work_mem = 4MB

# Increase maintenance work memory
maintenance_work_mem = 64MB

# Enable parallel processing
max_parallel_workers_per_gather = 2
max_parallel_workers = 4
```

Restart PostgreSQL after configuration changes:
```bash
sudo systemctl restart postgresql
```

## Step 3: Clone and Set Up the Application

### Clone the Repository
```bash
git clone <your-repository-url> New_BackTest_Pulse
cd New_BackTest_Pulse
```

### Set Up Python Virtual Environment
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Linux/macOS
# OR
venv\Scripts\activate     # On Windows
```

### Install Python Dependencies
```bash
pip install -r requirements.txt
```

## Step 4: Initialize the Database

### Set Environment Variables
```bash
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=Backtest_Pulse
export PGUSER=postgres
export PGPASSWORD=Alliswell@28
```

### Run Database Initialization Scripts

The database needs to be initialized in the correct order. Run these SQL files in sequence:

```bash
# 1. Create base tables
psql -f sql/create_nifty50.sql
psql -f sql/create_nifty_options.sql
psql -f sql/create_strategy_settings.sql
psql -f sql/create_runtime_strategy_config.sql
psql -f sql/create_strategy_run_results.sql
psql -f sql/create_strategy_leg_book.sql

# 2. Create Heikin-Ashi tables
psql -f sql/create_heikin_ashi_tables.sql

# 3. Create views and materialized views
psql -f sql/create_v_strategy_config.sql
psql -f sql/create_filtered_views.sql
psql -f sql/create_mv_ha_candles.sql
psql -f sql/create_mv_nifty_options_filtered.sql

# 4. Create breakout and trading logic materialized views
psql -f sql/create_mv_all_5min_breakouts.sql
psql -f sql/create_mv_ranked_breakouts_with_rounds.sql
psql -f sql/create_mv_ranked_breakouts_with_rounds_for_reentry.sql
psql -f sql/create_mv_base_strike_selection.sql
psql -f sql/create_mv_breakout_context_round1.sql

# 5. Create entry/exit logic views
psql -f sql/create_mv_entry_and_hedge_legs.sql
psql -f sql/create_mv_live_prices_entry_round1.sql
psql -f sql/create_mv_entry_sl_hits_round1.sql
psql -f sql/create_mv_entry_sl_executions_round1.sql
psql -f sql/create_mv_entry_open_legs_round1.sql
psql -f sql/create_mv_entry_profit_booking_round1.sql
psql -f sql/create_mv_entry_eod_close_round1.sql
psql -f sql/create_mv_entry_closed_legs_round1.sql
psql -f sql/create_mv_entry_round1_stats.sql

# 6. Create hedge logic views
psql -f sql/create_mv_hedge_exit_on_all_entry_sl.sql
psql -f sql/create_mv_hedge_exit_partial_conditions.sql
psql -f sql/create_mv_hedge_closed_legs_round1.sql
psql -f sql/create_mv_hedge_eod_exit_round1.sql
psql -f sql/create_mv_entry_exit_on_partial_hedge_round1.sql
psql -f sql/create_mv_double_buy_legs_round1.sql
psql -f sql/create_mv_entry_final_exit_round1.sql

# 7. Create reentry logic views
psql -f sql/create_mv_rehedge_trigger_round1.sql
psql -f sql/create_mv_rehedge_candidate_round1.sql
psql -f sql/create_mv_rehedge_selected_round1.sql
psql -f sql/create_mv_rehedge_leg_round1.sql
psql -f sql/create_mv_rehedge_eod_exit_round1.sql
psql -f sql/create_mv_all_legs_round1.sql

# 8. Create reentry round views
psql -f sql/create_mv_reentry_triggered_breakouts.sql
psql -f sql/create_mv_reentry_base_strike_selection.sql
psql -f sql/create_mv_reentry_legs_and_hedge_legs.sql
psql -f sql/create_mv_reentry_live_prices.sql
psql -f sql/create_mv_reentry_breakout_context.sql
psql -f sql/create_mv_reentry_sl_hits.sql
psql -f sql/create_mv_reentry_sl_executions.sql
psql -f sql/create_mv_reentry_open_legs.sql
psql -f sql/create_mv_reentry_profit_booking.sql
psql -f sql/create_mv_reentry_eod_close.sql
psql -f sql/create_mv_reentry_final_exit.sql
psql -f sql/create_mv_double_buy_legs_reentry.sql
psql -f sql/create_mv_reentry_legs_stats.sql

# 9. Create reentry hedge views
psql -f sql/create_mv_hedge_reentry_exit_on_all_entry_sl.sql
psql -f sql/create_mv_hedge_reentry_exit_on_partial_conditions.sql
psql -f sql/create_mv_hedge_reentry_closed_legs.sql
psql -f sql/create_mv_hedge_reentry_eod_exit.sql
psql -f sql/create_mv_reentry_exit_on_partial_hedge.sql
psql -f sql/create_mv_rehedge_trigger_reentry.sql
psql -f sql/create_mv_rehedge_candidate_reentry.sql
psql -f sql/create_mv_rehedge_selected_reentry.sql
psql -f sql/create_mv_rehedge_leg_reentry.sql
psql -f sql/create_mv_rehedge_eod_exit_reentry.sql
psql -f sql/create_mv_all_legs_reentry.sql

# 10. Create portfolio and final views
psql -f sql/create_mv_entry_leg_live_prices.sql
psql -f sql/create_mv_all_entries_sl_tracking_adjusted.sql
psql -f sql/create_mv_portfolio_mtm_pnl.sql
psql -f sql/create_mv_portfolio_final_pnl.sql

# 11. Create stored procedures
psql -f sql/sp_insert_sl_legs_into_book.sql
psql -f sql/sp_run_reentry_loop.sql
psql -f sql/sp_run_strategy.sql

# 12. Create indexes
psql -f sql/create_indexes_matviews.sql

# 13. Set up default data
psql -f sql/update_strategy_settings_defaults.sql
psql -f sql/upsert_runtime_strategy_config_default.sql
psql -f sql/set_strategy_settings_parent_values.sql

# 14. Create functions
psql -f sql/get_heikin_ashi.sql
```

### Alternative: Use the Initialization Script
If you have bash available, you can use the provided initialization script:

```bash
# Make script executable
chmod +x init-db.sh

# Run initialization (adjust database connection details if needed)
PGHOST=localhost PGPORT=5432 PGDATABASE=Backtest_Pulse PGUSER=postgres PGPASSWORD=Alliswell@28 ./init-db.sh
```

## Step 5: Load Market Data (Optional)

If you have market data to load, you can use the provided scripts:

```bash
# Load NIFTY50 data
python scripts/bulk_load_nifty50.py

# Load NIFTY options data
python scripts/bulk_load_nifty_options.py

# Compute Heikin-Ashi candles
python scripts/populate_heikin_ashi.py
```

## Step 6: Run the Application

### Start the Flask Web Application
```bash
# Ensure virtual environment is activated
source venv/bin/activate  # On Linux/macOS
# OR
venv\Scripts\activate     # On Windows

# Set environment variables (same as database setup)
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=Backtest_Pulse
export PGUSER=postgres
export PGPASSWORD=Alliswell@28

# Run the application
python app.py
```

The application will start on `http://localhost:5000`

### Alternative: Run with Gunicorn (Production)
For production deployment, use Gunicorn:

```bash
# Install Gunicorn
pip install gunicorn

# Run with Gunicorn
gunicorn --bind 0.0.0.0:5000 --workers 4 app:app
```

## Step 7: Access the Application

1. **Web Interface**: Open `http://localhost:5000` in your browser
2. **Health Check**: Visit `http://localhost:5000/health` to verify the application is running
3. **Database Connection**: The app will connect to PostgreSQL using the environment variables

## Configuration

### Environment Variables
Create a `.env` file in the project root:

```bash
PGHOST=localhost
PGPORT=5432
PGDATABASE=Backtest_Pulse
PGUSER=postgres
PGPASSWORD=Alliswell@28
FLASK_ENV=production
SECRET_KEY=your-secret-key-here
```

### Application Configuration
Edit `app.py` to modify:
- Host and port settings
- Upload folder location
- Secret key for sessions

## Troubleshooting

### Database Connection Issues
1. Verify PostgreSQL is running: `sudo systemctl status postgresql`
2. Check connection: `psql -U postgres -d Backtest_Pulse -c "SELECT 1;"`
3. Verify environment variables are set correctly

### Permission Issues
1. Ensure the database user has proper permissions
2. Check file permissions on the application directory
3. Verify the virtual environment is activated

### Port Conflicts
1. Check if port 5000 is available: `netstat -tlnp | grep :5000`
2. Change the port in `app.py` if needed

### Performance Issues
1. Increase PostgreSQL shared_buffers
2. Add more workers to Gunicorn
3. Monitor system resources with `top` or `htop`

## Maintenance

### Database Backup
```bash
pg_dump -U postgres -d Backtest_Pulse > backup_$(date +%Y%m%d_%H%M%S).sql
```

### Database Restore
```bash
psql -U postgres -d Backtest_Pulse < backup_file.sql
```

### Update Application
```bash
# Pull latest changes
git pull origin main

# Update dependencies
pip install -r requirements.txt

# Restart application
# (Kill existing process and restart)
```

### Monitor Logs
```bash
# View PostgreSQL logs
sudo tail -f /var/log/postgresql/postgresql-12-main.log

# View application logs (if using systemd)
sudo journalctl -u your-service-name -f
```

## Security Considerations

1. **Change Default Passwords**: Update PostgreSQL password in production
2. **Firewall**: Configure firewall to restrict access to necessary ports only
3. **SSL/TLS**: Enable SSL for PostgreSQL and consider HTTPS for the web interface
4. **User Permissions**: Run the application as a non-root user
5. **Regular Updates**: Keep PostgreSQL, Python, and dependencies updated

## Support

If you encounter issues:
1. Check the troubleshooting section above
2. Verify all prerequisites are met
3. Check PostgreSQL and application logs
4. Ensure all environment variables are set correctly