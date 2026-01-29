#!/bin/bash
# Local Setup Script
# Sets up the local development environment with Docker Compose

set -e

echo "Setting up Starlink ETL Pipeline locally..."
echo "This will use Docker Compose with PostgreSQL"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "Error: Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

cd infrastructure

echo ""
echo "=== Starting services ==="
echo "Starting PostgreSQL and Dashboard..."

# Start core services
docker-compose up -d postgres dashboard

# Wait for services to be healthy
echo "Waiting for services to be ready..."
sleep 10

echo ""
echo "=== Setup complete! ==="
echo ""
echo "Services running:"
echo "  - PostgreSQL: localhost:5432 (postgres/postgres)"
echo "  - Dashboard: http://localhost:5000"
echo ""
echo "Next steps:"
echo "1. Generate data: docker-compose --profile generator run --rm data-generator"
echo "2. Run ETL: docker-compose --profile etl run --rm spark-etl"
echo "3. View dashboard: http://localhost:5000"

