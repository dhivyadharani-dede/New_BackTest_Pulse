# New_BackTest_Pulse - Windows Deployment Guide
## Complete Step-by-Step Instructions for Client Machine

**Date:** December 30, 2025  
**Platform:** Windows  
**Prerequisites:** Python 3.8+ and PostgreSQL installed

---

## 📋 PRE-DEPLOYMENT CHECKLIST

### ✅ Verify Your Environment
Before starting, confirm these are installed:

1. **Python 3.8 or higher**
   ```cmd
   python --version
   ```
   Expected: `Python 3.8.x` or higher

2. **PostgreSQL 12 or higher**
   ```cmd
   psql --version
   ```
   Expected: `psql (PostgreSQL) 12.x` or higher

3. **Pip (Python package manager)**
   ```cmd
   pip --version
   ```
   Expected: `pip 20.x` or higher

---

## 🚀 STEP-BY-STEP DEPLOYMENT PROCESS

### STEP 1: Prepare PostgreSQL Database

**Action:** Create the required database and user

**Instructions:**
1. Open **Command Prompt** as Administrator (Win + R → `cmd` → Ctrl+Shift+Enter)
2. Connect to PostgreSQL:
   ```cmd
   psql -U postgres
   ```
3. If prompted for password, enter your PostgreSQL password
4. Run these SQL commands in the psql shell:
   ```sql
   CREATE DATABASE "Backtest_Pulse";
   CREATE USER postgres WITH PASSWORD 'Alliswell@28';
   GRANT ALL PRIVILEGES ON DATABASE "Backtest_Pulse" TO postgres;
   ALTER USER postgres CREATEDB;
   \q
   ```

**Expected Result:** Database created successfully

**Troubleshooting:**
- If "user already exists" error: Skip CREATE USER line, just run GRANT
- If connection fails: Check PostgreSQL service is running (services.msc)

---

### STEP 2: Set Up Project Directory

**Action:** Navigate to your project folder

**Instructions:**
```cmd
cd C:\path\to\your\New_BackTest_Pulse\folder
```

**Note:** Replace `C:\path\to\your\New_BackTest_Pulse\folder` with your actual folder path

**Expected Result:** Command prompt shows your project directory path

---

### STEP 3: Create Python Virtual Environment

**Action:** Set up isolated Python environment

**Instructions:**
```cmd
# Create virtual environment
python -m venv venv

# Activate virtual environment
venv\Scripts\activate
```

**Expected Result:**
- `(venv)` appears at the start of your command prompt
- Command prompt shows: `(venv) C:\path\to\project>`

**Important:** Keep this command prompt window open for all subsequent steps

---

### STEP 4: Install Python Dependencies

**Action:** Install required Python packages

**Instructions:**
```cmd
pip install -r requirements.txt
```

**Expected Result:**
- Multiple packages install (takes 1-2 minutes)
- No error messages
- Final output shows "Successfully installed..."

**Troubleshooting:**
- If "pip not recognized": Run `python -m pip install -r requirements.txt`
- If network errors: Check internet connection

---

### STEP 5: Configure Environment Variables

**Action:** Set database connection parameters

**Instructions:** In the SAME command prompt, run:
```cmd
set PGHOST=localhost
set PGPORT=5432
set PGDATABASE=Backtest_Pulse
set PGUSER=postgres
set PGPASSWORD=Alliswell@28
```

**Expected Result:** No output (variables are set silently)

**Verification:** Run `echo %PGDATABASE%` - should show `Backtest_Pulse`

---

### STEP 6: Initialize Database Schema

**Action:** Create all tables, views, and procedures

**Option A: Automated (Recommended)**
```cmd
deploy-native.bat
```

**Option B: Manual SQL Execution**
If automated script fails, run these commands individually:
```cmd
psql -f sql\create_nifty50.sql
psql -f sql\create_nifty_options.sql
psql -f sql\create_strategy_settings.sql
psql -f sql\create_runtime_strategy_config.sql
psql -f sql\create_strategy_run_results.sql
psql -f sql\create_strategy_leg_book.sql
psql -f sql\create_heikin_ashi_tables.sql
psql -f sql\create_v_strategy_config.sql
psql -f sql\create_filtered_views.sql
psql -f sql\create_mv_ha_candles.sql
psql -f sql\create_mv_nifty_options_filtered.sql
psql -f sql\create_mv_all_5min_breakouts.sql
psql -f sql\create_mv_ranked_breakouts_with_rounds.sql
psql -f sql\create_mv_ranked_breakouts_with_rounds_for_reentry.sql
psql -f sql\create_mv_base_strike_selection.sql
psql -f sql\create_mv_breakout_context_round1.sql
psql -f sql\create_mv_entry_and_hedge_legs.sql
psql -f sql\create_mv_live_prices_entry_round1.sql
psql -f sql\create_mv_entry_sl_hits_round1.sql
psql -f sql\create_mv_entry_sl_executions_round1.sql
psql -f sql\create_mv_entry_open_legs_round1.sql
psql -f sql\create_mv_entry_profit_booking_round1.sql
psql -f sql\create_mv_entry_eod_close_round1.sql
psql -f sql\create_mv_entry_closed_legs_round1.sql
psql -f sql\create_mv_entry_round1_stats.sql
psql -f sql\create_mv_hedge_exit_on_all_entry_sl.sql
psql -f sql\create_mv_hedge_exit_partial_conditions.sql
psql -f sql\create_mv_hedge_closed_legs_round1.sql
psql -f sql\create_mv_hedge_eod_exit_round1.sql
psql -f sql\create_mv_entry_exit_on_partial_hedge_round1.sql
psql -f sql\create_mv_double_buy_legs_round1.sql
psql -f sql\create_mv_entry_final_exit_round1.sql
psql -f sql\create_mv_rehedge_trigger_round1.sql
psql -f sql\create_mv_rehedge_candidate_round1.sql
psql -f sql\create_mv_rehedge_selected_round1.sql
psql -f sql\create_mv_rehedge_leg_round1.sql
psql -f sql\create_mv_rehedge_eod_exit_round1.sql
psql -f sql\create_mv_all_legs_round1.sql
psql -f sql\create_mv_reentry_triggered_breakouts.sql
psql -f sql\create_mv_reentry_base_strike_selection.sql
psql -f sql\create_mv_reentry_legs_and_hedge_legs.sql
psql -f sql\create_mv_reentry_live_prices.sql
psql -f sql\create_mv_reentry_breakout_context.sql
psql -f sql\create_mv_reentry_sl_hits.sql
psql -f sql\create_mv_reentry_sl_executions.sql
psql -f sql\create_mv_reentry_open_legs.sql
psql -f sql\create_mv_reentry_profit_booking.sql
psql -f sql\create_mv_reentry_eod_close.sql
psql -f sql\create_mv_reentry_final_exit.sql
psql -f sql\create_mv_double_buy_legs_reentry.sql
psql -f sql\create_mv_reentry_legs_stats.sql
psql -f sql\create_mv_hedge_reentry_exit_on_all_entry_sl.sql
psql -f sql\create_mv_hedge_reentry_exit_on_partial_conditions.sql
psql -f sql\create_mv_hedge_reentry_closed_legs.sql
psql -f sql\create_mv_hedge_reentry_eod_exit.sql
psql -f sql\create_mv_reentry_exit_on_partial_hedge.sql
psql -f sql\create_mv_rehedge_trigger_reentry.sql
psql -f sql\create_mv_rehedge_candidate_reentry.sql
psql -f sql\create_mv_rehedge_selected_reentry.sql
psql -f sql\create_mv_rehedge_leg_reentry.sql
psql -f sql\create_mv_rehedge_eod_exit_reentry.sql
psql -f sql\create_mv_all_legs_reentry.sql
psql -f sql\create_mv_entry_leg_live_prices.sql
psql -f sql\create_mv_all_entries_sl_tracking_adjusted.sql
psql -f sql\create_mv_portfolio_mtm_pnl.sql
psql -f sql\create_mv_portfolio_final_pnl.sql
psql -f sql\sp_insert_sl_legs_into_book.sql
psql -f sql\sp_run_reentry_loop.sql
psql -f sql\sp_run_strategy.sql
psql -f sql\create_indexes_matviews.sql
psql -f sql\update_strategy_settings_defaults.sql
psql -f sql\upsert_runtime_strategy_config_default.sql
psql -f sql\set_strategy_settings_parent_values.sql
psql -f sql\get_heikin_ashi.sql
```

**Expected Result:**
- Each command runs without errors
- Progress through ~50 SQL files
- Final message: "Database initialization completed successfully!"

---

### STEP 7: Start the Application

**Action:** Launch the Flask web application

**Instructions:**
```cmd
# Ensure virtual environment is active (should show (venv))
# If not, run: venv\Scripts\activate

# Start the application
python app.py
```

**Expected Result:**
```
* Serving Flask app 'app'
* Debug mode: on
* Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
* Restarting with stat
* Debugger is active!
* Debugger PIN: xxx-xxx-xxx
```

---

### STEP 8: Verify Deployment

**Action:** Test that everything works

**Instructions:**
1. Open web browser
2. Navigate to: `http://localhost:5000`
3. Check health endpoint: `http://localhost:5000/health`

**Expected Results:**
- Web interface loads successfully
- Health check returns: `{"status": "healthy", "database": "connected"}`

---

## 📁 REQUIRED FILES TO COPY

Copy these files/folders to your client machine:

### Core Application Files:
- [ ] `app.py` - Main Flask application
- [ ] `requirements.txt` - Python dependencies
- [ ] `src/db.py` - Database connection module
- [ ] `src/strategy_executor.py` - Strategy execution logic
- [ ] `src/backtest.py` - CLI backtesting tool
- [ ] `src/sim.py` - Portfolio simulation logic

### Web Interface Files:
- [ ] `templates/` - HTML templates folder
- [ ] `static/` - CSS/JS assets folder

### Database Files:
- [ ] `sql/` - All SQL schema files (50+ files)
- [ ] `init-db.sh` - Database initialization script

### Deployment Scripts:
- [ ] `deploy-native.bat` - Windows deployment automation
- [ ] `deploy-native.sh` - Linux/macOS deployment automation

### Documentation:
- [ ] `README.md` - General documentation
- [ ] `NATIVE_DEPLOYMENT.md` - Native deployment guide
- [ ] `DEPLOYMENT.md` - Complete deployment guide

### Optional Files:
- [ ] `examples/` - Sample strategy files
- [ ] `scripts/` - Utility scripts
- [ ] `.env.example` - Environment template

---

## 🔧 TROUBLESHOOTING GUIDE

### Issue: "psql: command not found"
**Solution:**
1. Add PostgreSQL bin folder to PATH
2. Or use full path: `"C:\Program Files\PostgreSQL\14\bin\psql.exe"`

### Issue: "python: command not found"
**Solution:**
1. Use `py` instead of `python`
2. Or use full path: `"C:\Python38\python.exe"`

### Issue: Database connection fails
**Solution:**
```cmd
# Test connection
psql -U postgres -d Backtest_Pulse -c "SELECT 1;"

# Check PostgreSQL service
services.msc
# Find "postgresql-x64-14" and ensure it's running
```

### Issue: Port 5000 already in use
**Solution:**
```cmd
# Find process using port
netstat -ano | findstr :5000

# Kill the process
taskkill /PID <PID_NUMBER> /F
```

### Issue: Virtual environment not activating
**Solution:**
```cmd
# Use full path
C:\path\to\project\venv\Scripts\activate.bat

# Or run as administrator
```

### Issue: Pip install fails
**Solution:**
```cmd
# Upgrade pip first
python -m pip install --upgrade pip

# Then install requirements
pip install -r requirements.txt
```

---

## ✅ SUCCESS VERIFICATION CHECKLIST

- [ ] Python 3.8+ installed and working
- [ ] PostgreSQL installed and service running
- [ ] Database "Backtest_Pulse" created successfully
- [ ] User "postgres" has proper permissions
- [ ] Virtual environment created and activated
- [ ] All Python packages installed without errors
- [ ] Environment variables set correctly
- [ ] All SQL files executed successfully
- [ ] Application starts without errors
- [ ] Web interface accessible at http://localhost:5000
- [ ] Health check returns healthy status

---

## 🚀 POST-DEPLOYMENT TASKS

### Optional: Load Market Data
```cmd
# Load NIFTY50 data
python scripts\bulk_load_nifty50.py

# Load NIFTY options data
python scripts\bulk_load_nifty_options.py

# Compute Heikin-Ashi candles
python scripts\populate_heikin_ashi.py
```

### Optional: Run Backtest
```cmd
# Run backtest with sample strategy
python -m src.backtest --sql examples/sample_strategy.sql --start 2020-01-01 --end 2020-12-31 --initial-capital 100000
```

---

## 📞 SUPPORT INFORMATION

If you encounter issues:

1. **Check the troubleshooting section above**
2. **Verify all prerequisites are met**
3. **Share error messages and which step failed**
4. **Include your Windows version and PostgreSQL/Python versions**

**Success Path:**
Follow steps 1→2→3→4→5→6→7→8 in order
Each step builds on the previous one
Don't skip steps or close command prompt windows

---

**Document Version:** 1.0  
**Last Updated:** December 30, 2025  
**Platform:** Windows 10/11  
**Tested With:** Python 3.8+, PostgreSQL 14