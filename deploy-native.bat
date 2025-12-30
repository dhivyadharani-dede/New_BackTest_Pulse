@echo off
REM New_BackTest_Pulse Native Deployment Script for Windows
REM This script helps deploy the application on Windows systems

echo 🚀 Starting New_BackTest_Pulse Native Deployment...

REM Check if Python is available
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python is not installed. Please install Python 3.8 or higher from https://python.org
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo [SUCCESS] Python %PYTHON_VERSION% is available

REM Check if PostgreSQL is available
psql --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] PostgreSQL client (psql) is not installed. Please install PostgreSQL.
    exit /b 1
)
echo [SUCCESS] PostgreSQL client is available

REM Set up Python virtual environment
echo [INFO] Setting up Python virtual environment...
if not exist venv (
    python -m venv venv
    echo [SUCCESS] Virtual environment created
) else (
    echo [WARNING] Virtual environment already exists
)

REM Activate virtual environment and install dependencies
echo [INFO] Installing Python dependencies...
call venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt
echo [SUCCESS] Dependencies installed

REM Set default environment variables if not set
if "%PGHOST%"=="" set PGHOST=localhost
if "%PGPORT%"=="" set PGPORT=5432
if "%PGDATABASE%"=="" set PGDATABASE=Backtest_Pulse
if "%PGUSER%"=="" set PGUSER=postgres
if "%PGPASSWORD%"=="" set PGPASSWORD=Alliswell@28

REM Check database connection
echo [INFO] Checking database connection...
psql -c "SELECT 1;" >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Cannot connect to database. Please ensure:
    echo   1. PostgreSQL is running
    echo   2. Database 'Backtest_Pulse' exists
    echo   3. User 'postgres' has access
    echo   4. Environment variables are set correctly:
    echo     PGHOST=%PGHOST%
    echo     PGPORT=%PGPORT%
    echo     PGDATABASE=%PGDATABASE%
    echo     PGUSER=%PGUSER%
    exit /b 1
)
echo [SUCCESS] Database connection successful

REM Initialize database
echo [INFO] Initializing database...
if not exist init-db.sh (
    echo [ERROR] init-db.sh script not found. Please ensure you're in the correct directory.
    exit /b 1
)

REM Run database initialization (using bash if available, otherwise manual)
bash init-db.sh 2>nul
if %errorlevel% neq 0 (
    echo [WARNING] bash not available, you'll need to run SQL files manually.
    echo [INFO] Please run the SQL files in sql/ directory in the order shown in init-db.sh
    echo [INFO] You can use: psql -f sql/filename.sql for each file
)

echo [SUCCESS] Database initialized

REM Test application
echo [INFO] Testing application startup...
python -c "import app; print('Application imports successfully')" >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Application import failed
    exit /b 1
)
echo [SUCCESS] Application imports successfully

REM Test database connection from Python
echo [INFO] Testing database connection from application...
python -c "
import os
from src.db import get_conn
with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM strategy_settings')
        count = cur.fetchone()[0]
        print(f'Database connection successful. Found {count} strategies.')
" >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Database connection from application failed
    exit /b 1
)
echo [SUCCESS] Database connection from application successful

REM Show deployment information
echo.
echo 🎉 Native deployment completed successfully!
echo.
echo To start the application:
echo   venv\Scripts\activate.bat
echo   python app.py
echo.
echo Application will be available at:
echo   Web Interface: http://localhost:5000
echo   Health Check: http://localhost:5000/health
echo.
echo Database connection details:
echo   Host: %PGHOST%
echo   Port: %PGPORT%
echo   Database: %PGDATABASE%
echo   User: %PGUSER%
echo.
echo Useful commands:
echo   Activate environment: venv\Scripts\activate.bat
echo   Run application: python app.py
echo   Run in background: start /B python app.py
echo   Check processes: tasklist /FI "IMAGENAME eq python.exe"
echo   Kill process: taskkill /F /IM python.exe

pause