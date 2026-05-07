#!/bin/bash
set -e
PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
VENV="$PROJECT_ROOT/venv/bin/activate"
DJANGO="$PROJECT_ROOT/django_app"

echo "==> 1. Starting Docker services (Kafka, MongoDB, PostgreSQL)..."
docker compose up -d postgres mongo kafka
echo "    Waiting 25s for Kafka to be ready..."
sleep 25

echo "==> 2. Activating venv..."
source "$VENV"

echo "==> 3. Applying Django migrations..."
cd "$DJANGO"
python manage.py migrate --run-syncdb

echo "==> 4. Starting Daphne (Django ASGI server)..."
echo "    Open http://localhost:8000 in your browser."
echo "    Then go to Pipeline Control and click 'Start Pipeline'."
echo ""
daphne -b 0.0.0.0 -p 8000 reviews_project.asgi:application
