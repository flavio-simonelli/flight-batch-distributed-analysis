#!/bin/bash

# Script to select which database should be active
# Usage: ./db-selector.sh {postgres|mongodb|redis|hbase|all|stop}

TARGET=$1

case $TARGET in
    postgres)
        echo "[DB] Starting PostgreSQL only..."
        docker-compose stop
        docker-compose up -d postgres
        ;;
    mongodb)
        echo "[DB] Starting MongoDB only..."
        docker-compose stop
        docker-compose up -d mongodb
        ;;
    redis)
        echo "[DB] Starting Redis only..."
        docker-compose stop
        docker-compose up -d redis-output
        ;;
    hbase)
        echo "[DB] Starting HBase only..."
        docker-compose stop
        docker-compose up -d hbase
        ;;
    all)
        echo "[DB] Starting all databases..."
        docker-compose up -d
        ;;
    stop)
        echo "[DB] Stopping all databases..."
        docker-compose stop
        ;;
    *)
        echo "Usage: $0 {postgres|mongodb|redis|hbase|all|stop}"
        exit 1
        ;;
esac
