#!/bin/bash

# New_BackTest_Pulse Native Deployment Script
# This script helps deploy the application on systems without Docker

set -e

echo "🚀 Starting New_BackTest_Pulse Native Deployment..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Python 3.8+ is available
check_python() {
    if ! command -v python3 > /dev/null 2>&1; then
        print_error "Python 3 is not installed. Please install Python 3.8 or higher."
        exit 1
    fi

    PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
    if python3 -c 'import sys; exit(0 if sys.version_info >= (3, 8) else 1)'; then
        print_success "Python $PYTHON_VERSION is available"
    else
        print_error "Python $PYTHON_VERSION is too old. Please upgrade to Python 3.8 or higher."
        exit 1
    fi
}

# Check if PostgreSQL is available
check_postgresql() {
    if ! command -v psql > /dev/null 2>&1; then
        print_error "PostgreSQL client (psql) is not installed. Please install PostgreSQL."
        exit 1
    fi
    print_success "PostgreSQL client is available"
}

# Check if virtualenv is available
check_virtualenv() {
    if ! python3 -m venv --help > /dev/null 2>&1; then
        print_error "Python venv module is not available. Please ensure Python 3.8+ is properly installed."
        exit 1
    fi
    print_success "Python venv is available"
}

# Set up Python virtual environment
setup_virtualenv() {
    print_status "Setting up Python virtual environment..."

    if [ ! -d "venv" ]; then
        python3 -m venv venv
        print_success "Virtual environment created"
    else
        print_warning "Virtual environment already exists"
    fi

    # Activate virtual environment
    source venv/bin/activate

    print_success "Virtual environment activated"
}

# Install Python dependencies
install_dependencies() {
    print_status "Installing Python dependencies..."

    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt

    print_success "Dependencies installed"
}

# Check database connection
check_database_connection() {
    print_status "Checking database connection..."

    # Set default environment variables if not set
    export PGHOST=${PGHOST:-localhost}
    export PGPORT=${PGPORT:-5432}
    export PGDATABASE=${PGDATABASE:-Backtest_Pulse}
    export PGUSER=${PGUSER:-postgres}
    export PGPASSWORD=${PGPASSWORD:-Alliswell@28}

    if psql -c "SELECT 1;" > /dev/null 2>&1; then
        print_success "Database connection successful"
    else
        print_error "Cannot connect to database. Please ensure:"
        echo "  1. PostgreSQL is running"
        echo "  2. Database 'Backtest_Pulse' exists"
        echo "  3. User 'postgres' has access"
        echo "  4. Environment variables are set correctly:"
        echo "     PGHOST=$PGHOST"
        echo "     PGPORT=$PGPORT"
        echo "     PGDATABASE=$PGDATABASE"
        echo "     PGUSER=$PGUSER"
        exit 1
    fi
}

# Initialize database
initialize_database() {
    print_status "Initializing database..."

    if [ ! -f "init-db.sh" ]; then
        print_error "init-db.sh script not found. Please ensure you're in the correct directory."
        exit 1
    fi

    chmod +x init-db.sh
    ./init-db.sh

    print_success "Database initialized"
}

# Test application startup
test_application() {
    print_status "Testing application startup..."

    source venv/bin/activate

    # Set environment variables
    export PGHOST=${PGHOST:-localhost}
    export PGPORT=${PGPORT:-5432}
    export PGDATABASE=${PGDATABASE:-Backtest_Pulse}
    export PGUSER=${PGUSER:-postgres}
    export PGPASSWORD=${PGPASSWORD:-Alliswell@28}
    export FLASK_ENV=production

    # Test import
    if python3 -c "import app; print('Application imports successfully')"; then
        print_success "Application imports successfully"
    else
        print_error "Application import failed"
        exit 1
    fi

    # Test database connection from Python
    if python3 -c "
import os
os.environ['PGHOST'] = '${PGHOST:-localhost}'
os.environ['PGPORT'] = '${PGPORT:-5432}'
os.environ['PGDATABASE'] = '${PGDATABASE:-Backtest_Pulse}'
os.environ['PGUSER'] = '${PGUSER:-postgres}'
os.environ['PGPASSWORD'] = '${PGPASSWORD:-Alliswell@28}'

from src.db import get_conn
with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM strategy_settings')
        count = cur.fetchone()[0]
        print(f'Database connection successful. Found {count} strategies.')
"; then
        print_success "Database connection from application successful"
    else
        print_error "Database connection from application failed"
        exit 1
    fi
}

# Show deployment information
show_info() {
    echo ""
    print_success "🎉 Native deployment completed successfully!"
    echo ""
    echo "To start the application:"
    echo "  source venv/bin/activate"
    echo "  python app.py"
    echo ""
    echo "Application will be available at:"
    echo "  Web Interface: http://localhost:5000"
    echo "  Health Check: http://localhost:5000/health"
    echo ""
    echo "Database connection details:"
    echo "  Host: ${PGHOST:-localhost}"
    echo "  Port: ${PGPORT:-5432}"
    echo "  Database: ${PGDATABASE:-Backtest_Pulse}"
    echo "  User: ${PGUSER:-postgres}"
    echo ""
    echo "Useful commands:"
    echo "  Activate environment: source venv/bin/activate"
    echo "  Run application: python app.py"
    echo "  Run in background: nohup python app.py &"
    echo "  Check processes: ps aux | grep python"
    echo "  Kill process: pkill -f 'python app.py'"
}

# Main deployment function
deploy() {
    check_python
    check_postgresql
    check_virtualenv
    setup_virtualenv
    install_dependencies
    check_database_connection
    initialize_database
    test_application
    show_info
}

# Show usage
usage() {
    echo "New_BackTest_Pulse Native Deployment Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  deploy    Deploy the application (default)"
    echo "  setup     Set up Python environment only"
    echo "  db-init   Initialize database only"
    echo "  test      Test application only"
    echo "  help      Show this help message"
    echo ""
    echo "Environment Variables (optional):"
    echo "  PGHOST      PostgreSQL host (default: localhost)"
    echo "  PGPORT      PostgreSQL port (default: 5432)"
    echo "  PGDATABASE  Database name (default: Backtest_Pulse)"
    echo "  PGUSER      Database user (default: postgres)"
    echo "  PGPASSWORD  Database password (default: Alliswell@28)"
}

# Main script logic
case "${1:-deploy}" in
    deploy)
        deploy
        ;;
    setup)
        check_python
        check_virtualenv
        setup_virtualenv
        install_dependencies
        print_success "Python environment setup complete"
        ;;
    db-init)
        check_database_connection
        initialize_database
        print_success "Database initialization complete"
        ;;
    test)
        test_application
        print_success "Application test complete"
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        print_error "Unknown command: $1"
        usage
        exit 1
        ;;
esac