#!/bin/bash

# Helper script to manage HDFS files on EC2
# Transfers files between Host, Container staging, and HDFS.
# Usage: 
#   ./hdfs-helper.sh upload <host_file> <hdfs_dest>
#   ./hdfs-helper.sh download <hdfs_file> <host_dest>
#   ./hdfs-helper.sh ls <hdfs_path>

ACTION=$1

case $ACTION in
    upload)
        HOST_FILE=$2
        HDFS_DEST=$3
        FILENAME=$(basename "$HOST_FILE")
        STAGING_PATH="/home/$FILENAME"
        
        echo "[HDFS] Copying $HOST_FILE to container staging..."
        docker cp "$HOST_FILE" hdfs-master:"$STAGING_PATH"
        
        echo "[HDFS] Moving from staging to HDFS: $HDFS_DEST..."
        docker exec hdfs-master /usr/local/hadoop/bin/hdfs dfs -put -f "$STAGING_PATH" "$HDFS_DEST"
        
        echo "[HDFS] Cleaning up staging..."
        docker exec hdfs-master rm "$STAGING_PATH"
        ;;
    download)
        HDFS_FILE=$2
        HOST_DEST=$3
        FILENAME=$(basename "$HDFS_FILE")
        STAGING_PATH="/home/$FILENAME"
        
        echo "[HDFS] Extracting $HDFS_FILE to container staging..."
        docker exec hdfs-master /usr/local/hadoop/bin/hdfs dfs -get -f "$HDFS_FILE" "$STAGING_PATH"
        
        echo "[HDFS] Copying from staging to host: $HOST_DEST..."
        docker cp hdfs-master:"$STAGING_PATH" "$HOST_DEST"
        
        echo "[HDFS] Cleaning up staging..."
        docker exec hdfs-master rm "$STAGING_PATH"
        ;;
    ls)
        HDFS_PATH=${2:-/}
        docker exec -i hdfs-master /usr/local/hadoop/bin/hdfs dfs -ls "$HDFS_PATH"
        ;;
    *)
        echo "Usage: $0 {upload|download|ls} [args...]"
        exit 1
        ;;
esac
