#!/bin/sh

# ------------------------------
# start-compose.sh
# ------------------------------
echo "[ Removing Mongo Running Server '$MONGO_HOST_NAME'... ]"
# On supprime (-f force) le conteneur. 
docker rm -f "$MONGO_HOST_NAME" > /dev/null 2>&1 || true

echo "---------- < Transient Server Removed > ----------"