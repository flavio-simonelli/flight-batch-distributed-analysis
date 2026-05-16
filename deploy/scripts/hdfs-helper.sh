#!/bin/bash

# Helper script for managing HDFS files on the EC2 cluster.
# This script facilitates the transfer of files between the EC2 host and HDFS
# using the hdfs-master container as a staging area.

# Usage: 
#   ./hdfs-helper.sh upload <local_source> <hdfs_destination>
#   ./hdfs-helper.sh download <hdfs_source> <local_destination>
#   ./hdfs-helper.sh ls <hdfs_path>

# Retrieve the intended action from the first command-line argument.
ACTION=$1

case $ACTION in
    upload)
        # Uploads a file from the EC2 host to HDFS.
        HOST_FILE=$2
        HDFS_DEST=$3
        FILENAME=$(basename "$HOST_FILE")
        STAGING_PATH="/home/$FILENAME"
        
        echo "[HDFS] Copying file $HOST_FILE to container staging area..."
        docker cp "$HOST_FILE" hdfs-master:"$STAGING_PATH"
        
        echo "[HDFS] Moving file from staging to HDFS destination: $HDFS_DEST..."
        docker exec hdfs-master /usr/local/hadoop/bin/hdfs dfs -put -f "$STAGING_PATH" "$HDFS_DEST"
        
        echo "[HDFS] Cleaning up container staging area..."
        docker exec hdfs-master rm "$STAGING_PATH"
        ;;
    download)
        # Downloads a file from HDFS to the EC2 host.
        HDFS_FILE=$2
        HOST_DEST=$3
        FILENAME=$(basename "$HDFS_FILE")
        STAGING_PATH="/home/$FILENAME"
        
        echo "[HDFS] Extracting file $HDFS_FILE to container staging area..."
        docker exec hdfs-master /usr/local/hadoop/bin/hdfs dfs -get -f "$HDFS_FILE" "$STAGING_PATH"
        
        echo "[HDFS] Copying file from staging area to host destination: $HOST_DEST..."
        docker cp hdfs-master:"$STAGING_PATH" "$HOST_DEST"
        
        echo "[HDFS] Cleaning up container staging area..."
        docker exec hdfs-master rm "$STAGING_PATH"
        ;;
    ls)
        # Lists the contents of an HDFS directory.
        HDFS_PATH=${2:-/}
        echo "[HDFS] Listing contents of HDFS path: $HDFS_PATH"
        docker exec -i hdfs-master /usr/local/hadoop/bin/hdfs dfs -ls "$HDFS_PATH"
        ;;
    *)
        # Display usage information for invalid inputs.
        echo "Usage: $0 {upload|download|ls} [arguments...]"
        echo "Example: ./hdfs-helper.sh ls /data"
        exit 1
        ;;
esac
