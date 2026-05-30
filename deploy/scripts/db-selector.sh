#!/bin/bash

# Utility script for managing the active database service on the Databases Node.
# This script allows to toggle between different storage engines
# without keeping all services active simultaneously, optimizing resource usage.

# Usage: ./db-selector.sh {postgres|redis|hbase|cockroach|all|stop}

# Retrieve the selection target from the first command-line argument.
TARGET=$1

case $TARGET in
    postgres)
        echo "[DB-SELECTOR] Activating PostgreSQL service only..."
        docker-compose stop
        docker-compose up -d postgres
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
    cockroach)
        echo "[DB-SELECTOR] Activating CockroachDB service only..."
        docker-compose stop
        docker-compose up -d cockroachdb
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
        echo "Usage: $0 {postgres|redis|hbase|cockroach|all|stop}"
        echo "Example: ./db-selector.sh cockroach"
        exit 1
        ;;
esac

echo "[DB-SELECTOR] Service state updated successfully."
