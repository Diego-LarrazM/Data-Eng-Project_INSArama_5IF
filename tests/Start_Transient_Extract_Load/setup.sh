#!/bin/sh

# ------------------------------
# start-compose.sh
# ------------------------------

echo "[ Reading ENVs... ]"

if [ -f .env ]; then
  while IFS= read -r line; do
    # Trim leading/trailing whitespace
    line=$(echo "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
    # Skip empty lines or lines starting with #
    [ -z "$line" ] && continue
    echo "$line" | grep -q '^#' && continue
    # Remove inline comments (anything after #)
    line=$(echo "$line" | sed 's/[[:space:]]*#.*$//')
    echo $line
    # Export the variable
    export "$line"
  done < .env
fi
echo "\n"

echo "[ Starting postgres docker compose... ]"

docker compose -f compose.datawarehouse.yaml up -d 
if [ $? -ne 0 ]; then
  echo "\n<@@ Docker compose failed for $DW_POSGRES_HOST... @@>"
  exit 1
fi

echo "\n"


echo "[ Starting mongo docker container... ]"

docker run -d --name "$MONGO_HOST_NAME" --network "$INSARAMA_NET" -p "$MONGO_PORT:$MONGO_PORT" mongo:7.0 --bind_ip_all --port $MONGO_PORT #--replSet "$MONGO_RSET_NAME"
if [ $? -ne 0 ] ; then
  echo "\n<@@ Docker run failed for $MONGO_HOST_NAME... @@>"
  exit 1
fi

echo "\n"

echo "[ Waiting for MongoDB container '$MONGO_HOST_NAME' to become healthy... ]"

# Loop until container can receive requests/conexions before initiating replica set
# Also serves as healthcheck.
i=1
while [ "$i" -le "$MONGO_HEALTHCHECK_RETRIES" ]; do
  STATUS=$(docker exec -i $MONGO_HOST_NAME mongosh --authenticationDatabase admin --quiet --eval "JSON.stringify(db.adminCommand({ ping: 1 }))")
  OK=$(echo "$STATUS" | jq -r '.ok')
  if [ "$OK" = "1" ]; then
    echo "\n< MongoDB is healthy! >"
    break
  fi

  echo "Attempt $i/$MONGO_HEALTHCHECK_RETRIES failed. Current status: ['$STATUS']. Retrying..."
  sleep 2

  i=$((i + 1))

done

if [ "$OK" != "1" ]; then
    echo "\n<@@ MongoDB failed to become healthy after $MONGO_HEALTHCHECK_RETRIES attempts. @@>"
    exit 1
fi

echo "\n"

# echo "[ Setuing Up Replica Set: '$MONGO_HOST_NAME' ]\n"

# docker exec -i $MONGO_HOST_NAME mongosh --authenticationDatabase admin --quiet --eval \
#  "try { printjson(rs.status()) }\
#   catch (err) {\
#     print('Initializing $MONGO_RSET_NAME');\
#     rs.initiate({_id:'$MONGO_RSET_NAME', members:[{_id:0, host:'localhost:$MONGO_PORT'}]});\
#     printjson(rs.status());\
#   }"
# echo "\n"

echo "---------- < Transient Server Running > ----------"