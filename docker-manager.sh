#!/bin/bash

# Weather Data Pipeline Docker Management Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}================================${NC}"
    echo -e "${BLUE} Weather Data Pipeline Manager ${NC}"
    echo -e "${BLUE}================================${NC}"
}

# Check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
}

# Check if .env file exists
check_env() {
    if [ ! -f .env ]; then
        print_warning ".env file not found. Creating from template..."
        if [ -f .env.template ]; then
            cp .env.template .env
            print_warning "Please edit .env file with your OpenWeatherMap API key"
            print_warning "You can get a free API key from: https://openweathermap.org/api"
        else
            print_error ".env.template not found"
            exit 1
        fi
    fi
}

# Build the Docker image
build_image() {
    print_status "Building Weather Data Pipeline Docker image..."
    docker build -t weather-pipeline:latest .
    print_status "Docker image built successfully!"
}

# Start the services
start_services() {
    print_status "Starting Weather Data Pipeline services..."
    docker-compose up -d
    
    print_status "Services started! Available endpoints:"
    echo "  - PostgreSQL: localhost:5432"
    echo "  - pgAdmin: http://localhost:8080 (admin@weather.com / admin)"
    echo "  - Spark UI: http://localhost:4040"
    echo ""
    print_status "Waiting for services to be ready..."
    sleep 10
    
    # Check service health
    if docker-compose ps | grep -q "healthy\|Up"; then
        print_status "Services are running successfully!"
    else
        print_warning "Some services may still be starting. Check with 'docker-compose ps'"
    fi
}

# Stop the services
stop_services() {
    print_status "Stopping Weather Data Pipeline services..."
    docker-compose down
    print_status "Services stopped!"
}

# View logs
view_logs() {
    service=${1:-weather-pipeline}
    print_status "Viewing logs for $service..."
    docker-compose logs -f $service
}

# Run the pipeline once
run_pipeline() {
    print_status "Running Weather Data Pipeline (single execution)..."
    docker-compose run --rm weather-pipeline spark
}

# Show service status
show_status() {
    print_status "Service Status:"
    docker-compose ps
    
    echo ""
    print_status "Resource Usage:"
    docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"
}

# Clean up everything
cleanup() {
    print_status "Cleaning up Docker resources..."
    docker-compose down -v
    docker image rm weather-pipeline:latest 2>/dev/null || true
    print_status "Cleanup completed!"
}

# Main script logic
case "${1:-help}" in
    build)
        print_header
        check_docker
        build_image
        ;;
    start)
        print_header
        check_docker
        check_env
        start_services
        ;;
    stop)
        print_header
        check_docker
        stop_services
        ;;
    restart)
        print_header
        check_docker
        stop_services
        start_services
        ;;
    logs)
        check_docker
        view_logs $2
        ;;
    run)
        print_header
        check_docker
        check_env
        run_pipeline
        ;;
    status)
        print_header
        check_docker
        show_status
        ;;
    cleanup)
        print_header
        check_docker
        cleanup
        ;;
    help|*)
        print_header
        echo "Usage: $0 {build|start|stop|restart|logs|run|status|cleanup|help}"
        echo ""
        echo "Commands:"
        echo "  build     - Build the Docker image"
        echo "  start     - Start all services (PostgreSQL, pgAdmin, Weather Pipeline)"
        echo "  stop      - Stop all services"
        echo "  restart   - Restart all services"
        echo "  logs      - View logs (optional: specify service name)"
        echo "  run       - Run the pipeline once"
        echo "  status    - Show service status and resource usage"
        echo "  cleanup   - Remove all containers, volumes, and images"
        echo "  help      - Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0 build"
        echo "  $0 start"
        echo "  $0 logs weather-pipeline"
        echo "  $0 status"
        ;;
esac