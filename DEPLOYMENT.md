# Backtest Pulse - Deployment Guide

A comprehensive backtesting platform for options trading strategies with PostgreSQL and Python.

## 🚀 Quick Start

### Option 1: Docker Deployment (Recommended)
```bash
# Clone the repository
git clone <repository-url>
cd New_BackTest_Pulse

# Deploy the application
./deploy.sh deploy
```

### Option 2: Native Deployment (Without Docker)
For systems without Docker, see [NATIVE_DEPLOYMENT.md](NATIVE_DEPLOYMENT.md) for detailed instructions.

**Quick Native Setup:**
```bash
# Linux/macOS
./deploy-native.sh deploy

# Windows
deploy-native.bat
```

The application will be available at:
- **Web Interface**: http://localhost:5000
- **Database**: localhost:5432 (postgres/Alliswell@28)

## 📋 Detailed Deployment

### Docker Deployment

#### 1. Environment Setup
```bash
# Copy environment template
cp .env.example .env

# Edit environment variables if needed
nano .env
```

#### 2. Database Initialization
The application includes an automated database initialization script that:
- Creates all required tables, views, and materialized views
- Sets up stored procedures and functions
- Configures default strategy settings
- Creates necessary indexes

#### 3. First Run
```bash
# Build and start services
docker-compose up --build -d

# Check logs
docker-compose logs -f

# Verify health
curl http://localhost:5000
```

### Native Deployment

#### Prerequisites
- Python 3.8+ installed
- PostgreSQL 12+ installed and running
- At least 4GB RAM available
- 10GB free disk space

#### 1. Database Setup
```bash
# Create database and user (Linux/macOS)
sudo -u postgres psql
CREATE DATABASE "Backtest_Pulse";
CREATE USER postgres WITH PASSWORD 'Alliswell@28';
GRANT ALL PRIVILEGES ON DATABASE "Backtest_Pulse" TO postgres;
\q

# Windows (run in psql shell)
CREATE DATABASE "Backtest_Pulse";
CREATE USER postgres WITH PASSWORD 'Alliswell@28';
GRANT ALL PRIVILEGES ON DATABASE "Backtest_Pulse" TO postgres;
```

#### 2. Python Environment
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # Linux/macOS
# OR
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

#### 3. Database Initialization
```bash
# Set environment variables
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=Backtest_Pulse
export PGUSER=postgres
export PGPASSWORD=Alliswell@28

# Run initialization script
./init-db.sh
```

#### 4. Start Application
```bash
# Activate environment and run
source venv/bin/activate
python app.py
```
```

## 🏗️ Architecture

### Services
- **PostgreSQL 13**: Database with optimized configuration
- **Python Flask App**: Web interface and backtesting engine

### Database Schema
- **Tables**: Market data, strategy configurations, results
- **Materialized Views**: Pre-computed trading logic and analytics
- **Stored Procedures**: Automated backtesting workflows

### Key Features
- ✅ Date-range filtered backtesting
- ✅ Real-time portfolio calculations
- ✅ Multiple strategy support
- ✅ Re-entry logic
- ✅ Risk management (SL, hedging)

## 🔧 Configuration

### Environment Variables
```bash
# Database
PGHOST=localhost
PGPORT=5432
PGDATABASE=Backtest_Pulse
PGUSER=postgres
PGPASSWORD=Alliswell@28

# Application
FLASK_ENV=production
SECRET_KEY=your-secret-key-here
```

### Database Tuning
The PostgreSQL container is configured with:
- `max_connections=200` for concurrent users
- Persistent data volumes
- Optimized for analytical workloads

## 📊 Usage

### Web Interface
1. Access http://localhost:5000
2. Upload market data (Nifty50, Nifty Options)
3. Configure strategy parameters
4. Run backtests with custom date ranges
5. Analyze results and performance metrics

### API Endpoints
- `GET /` - Main dashboard
- `POST /run-backtest` - Execute backtest
- `GET /results` - View results
- `GET /health` - Health check

## 🛠️ Management Commands

### Deployment Management
```bash
# Start services
./deploy.sh deploy

# Stop services
./deploy.sh stop

# View logs
./deploy.sh logs

# Restart services
./deploy.sh restart

# Clean deployment (removes all data)
./deploy.sh clean
```

### Database Management
```bash
# Access database
docker-compose exec postgres psql -U postgres -d Backtest_Pulse

# Backup database
docker-compose exec postgres pg_dump -U postgres Backtest_Pulse > backup.sql

# Restore database
docker-compose exec -T postgres psql -U postgres -d Backtest_Pulse < backup.sql
```

### Application Management
```bash
# Access application container
docker-compose exec app bash

# View application logs
docker-compose logs -f app

# Restart only the app
docker-compose restart app
```

## 📈 Performance Optimization

### Database Indexes
The system includes optimized indexes on:
- Date columns for time-range filtering
- Strategy identifiers
- Trade execution timestamps

### Materialized Views
Pre-computed views for:
- Portfolio calculations
- Risk metrics
- Performance analytics

### Memory Configuration
- PostgreSQL: 200 max connections
- Application: Health checks and monitoring

## 🔒 Security

### Container Security
- Non-root user execution
- Minimal base images
- No sensitive data in images

### Database Security
- Strong password requirements
- Connection pooling
- Query parameterization

### Network Security
- Internal network between services
- Exposed ports only as needed

## 🐛 Troubleshooting

### Common Issues

**Application won't start:**
```bash
# Check logs
docker-compose logs app

# Verify database connectivity
docker-compose exec app python -c "from src.db import get_conn; print('DB OK')"
```

**Database connection errors:**
```bash
# Check PostgreSQL status
docker-compose ps

# Verify database is ready
docker-compose exec postgres pg_isready -U postgres
```

**Out of memory:**
```bash
# Increase Docker memory allocation
# Or reduce PostgreSQL max_connections in docker-compose.yml
```

**Slow performance:**
```bash
# Check materialized view refresh status
docker-compose exec postgres psql -U postgres -d Backtest_Pulse -c "SELECT * FROM pg_stat_activity;"

# Refresh specific views if needed
docker-compose exec postgres psql -U postgres -d Backtest_Pulse -c "REFRESH MATERIALIZED VIEW mv_portfolio_final_pnl;"
```

## 📚 Advanced Configuration

### Custom Strategy Development
1. Add new strategy parameters to `strategy_settings` table
2. Create corresponding materialized views
3. Update `sp_run_strategy` stored procedure
4. Add UI components in Flask templates

### Data Loading
- Use the bulk loading scripts in `/scripts`
- Ensure data format matches expected schema
- Verify date ranges and data quality

### Monitoring
- Application health checks every 30 seconds
- Database connection monitoring
- Automatic service restarts on failure

## 🤝 Support

For issues and questions:
1. Check the logs: `docker-compose logs -f`
2. Verify configuration in `.env`
3. Ensure sufficient system resources
4. Review the troubleshooting section above

## 📄 License

This project is proprietary software. See LICENSE file for details.