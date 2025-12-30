# Run the automated script
deploy-native.bat#!/bin/bash

# Backtest Pulse Deployment Script
# This script handles the complete deployment of the Backtest Pulse application

set -e

echo "🚀 Starting Backtest Pulse Deployment..."

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

# Check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    print_success "Docker is running"
}

# Check if docker-compose is available
check_docker_compose() {
    if ! command -v docker-compose > /dev/null 2>&1; then
        print_error "docker-compose is not installed. Please install docker-compose and try again."
        exit 1
    fi
    print_success "docker-compose is available"
}

# Build the application
build_app() {
    print_status "Building the application..."
    docker-compose build --no-cache
    print_success "Application built successfully"
}

# Start the services
start_services() {
    print_status "Starting services..."
    docker-compose up -d
    print_success "Services started"
}

# Wait for services to be healthy
wait_for_services() {
    print_status "Waiting for services to be ready..."

    # Wait for PostgreSQL
    print_status "Waiting for PostgreSQL..."
    for i in {1..30}; do
        if docker-compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; then
            print_success "PostgreSQL is ready"
            break
        fi
        sleep 2
    done

    # Wait for application
    print_status "Waiting for application..."
    for i in {1..30}; do
        if curl -f http://localhost:5000 > /dev/null 2>&1; then
            print_success "Application is ready"
            break
        fi
        sleep 2
    done
}

# Check application health
check_health() {
    print_status "Checking application health..."

    # Test database connection
    if docker-compose exec -T app python -c "
from src.db import get_conn
try:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM strategy_settings')
            count = cur.fetchone()[0]
            print(f'✓ Database connection successful. Found {count} strategies.')
except Exception as e:
    print(f'✗ Database connection failed: {e}')
    exit(1)
"; then
        print_success "Database connection is working"
    else
        print_error "Database connection failed"
        exit 1
    fi

    # Test web interface
    if curl -s http://localhost:5000 > /dev/null; then
        print_success "Web interface is accessible"
    else
        print_error "Web interface is not accessible"
        exit 1
    fi
}

# Show deployment info
show_info() {
    echo ""
    print_success "🎉 Deployment completed successfully!"
    echo ""
    echo "Application URLs:"
    echo "  Web Interface: http://localhost:5000"
    echo "  PostgreSQL: localhost:5432"
    echo ""
    echo "Database Credentials:"
    echo "  Database: Backtest_Pulse"
    echo "  User: postgres"
    echo "  Password: Alliswell@28"
    echo ""
    echo "Useful commands:"
    echo "  View logs: docker-compose logs -f"
    echo "  Stop services: docker-compose down"
    echo "  Restart: docker-compose restart"
    echo "  Rebuild: docker-compose up --build"
}

# Main deployment function
deploy() {
    check_docker
    check_docker_compose
    build_app
    start_services
    wait_for_services
    check_health
    show_info
}

# Stop deployment
stop() {
    print_status "Stopping services..."
    docker-compose down
    print_success "Services stopped"
}

# Clean deployment (remove volumes)
clean() {
    print_warning "This will remove all data volumes. Are you sure? (y/N)"
    read -r response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        print_status "Cleaning deployment..."
        docker-compose down -v
        docker system prune -f
        print_success "Deployment cleaned"
    else
        print_status "Clean cancelled"
    fi
}

# Show usage
usage() {
    echo "Backtest Pulse Deployment Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  deploy    Deploy the application (default)"
    echo "  stop      Stop the application"
    echo "  clean     Clean deployment (removes all data)"
    echo "  logs      Show application logs"
    echo "  restart   Restart the application"
    echo "  help      Show this help message"
}

# Main script logic
case "${1:-deploy}" in
    deploy)
        deploy
        ;;
    stop)
        stop
        ;;
    clean)
        clean
        ;;
    logs)
        docker-compose logs -f
        ;;
    restart)
        docker-compose restart
        show_info
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