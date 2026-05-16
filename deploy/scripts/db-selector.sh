#!/bin/bash

# Utility script for managing the active database service on the Databases Node.
# This script allows the operator to toggle between different storage engines
# without keeping all services active simultaneously, optimizing resource usage.

# Usage: ./db-selector.sh {postgres|mongodb|redis|hbase|all|stop}

# Retrieve the selection target from the first command-line argument.
TARGET=$1

case $TARGET in
    postgres)
        echo "[DB-SELECTOR] Activating PostgreSQL service only..."
        docker-compose stop
        docker-compose up -d postgres
        ;;
    mongodb)
        echo "[DB-SELECTOR] Activating MongoDB service only..."
        docker-compose stop
        docker-compose up -d mongodb
        ;;
    redis)
        echo "[DB-SELECTOR] Activating Redis Output service only..."
        docker-compose stop
        docker-compose up -d redis-output
        ;;
    hbase)
        echo "[DB-SELECTOR] Activating HBase service only..."
        docker-compose stop
        docker-compose up -d hbase
        ;;
    all)
        echo "[DB-SELECTOR] Activating all database services..."
        docker-compose up -d
        ;;
    stop)
        echo "[DB-SELECTOR] Terminating all database services..."
        docker-compose stop
        ;;
    *)
        # Display usage information for invalid or missing inputs.
        echo "Usage: $0 {postgres|mongodb|redis|hbase|all|stop}"
        echo "Example: ./db-selector.sh postgres"
        exit 1
        ;;
esac

echo "[DB-SELECTOR] Service state updated successfully."
