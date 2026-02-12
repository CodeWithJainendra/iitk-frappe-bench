#!/bin/bash

# Build the containers
echo "Building Docker images..."
docker-compose build

# Start services in background
echo "Starting services..."
docker-compose up -d

# Wait for DB to be ready
echo "Waiting for MariaDB..."
sleep 20

# Create a new site if it doesn't exist
echo "Setting up site iit.test..."
docker-compose exec frappe bench new-site iit.test --mariadb-root-password root --admin-password admin --no-mariadb-socket

# Install the app
echo "Installing iitdata app..."
docker-compose exec frappe bench --site iit.test install-app iitdata

echo "------------------------------------------------"
echo "Setup Complete!"
echo "You can access the site at http://localhost:8000"
echo "Login: Administrator / admin"
echo "------------------------------------------------"
