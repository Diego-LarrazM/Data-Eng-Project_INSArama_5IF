#!/bin/sh

# ------------------------------
# start-compose.sh
# ------------------------------

TEMPLATE="./docker-compose.template.yml"
OUTPUT="./docker-compose.yml"

if [ ! -f "$TEMPLATE" ]; then
  echo "\n<% Template file not found: $TEMPLATE %>"
  exit 1
fi

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
echo "[ Generating docker-compose.yml from template... ]"

# Substitute environment variables
envsubst < "$TEMPLATE" > "$OUTPUT"

echo "\n< Generated: $OUTPUT >" 
echo "[ Starting docker compose... ]"

docker compose up -d
if [ $? -ne 0 ]; then
  echo "\n<% Docker compose failed. %>"
  exit 1
fi

echo "\n"

echo "[ Waiting for MongoDB container '$MONGO_HOST_NAME' to become healthy... ]"

# Loop until health is "healthy"
while true; do
  STATUS=$(docker exec -i $MONGO_HOST_NAME mongosh --authenticationDatabase admin --quiet --eval "JSON.stringify(db.adminCommand({ ping: 1 }))")
  OK=$(echo "$STATUS" | jq -r '.ok')
  if [ "$OK" = "1" ]; then
    echo "\n< MongoDB is healthy! >"
    break
  fi

  echo "Current status: $STATUS. Retrying..."
  sleep 2
done

echo "\n"

echo "[ Setuing Up Replica Set: '$MONGO_HOST_NAME' ]\n"
# Setup Replica Set
docker exec -i $MONGO_HOST_NAME mongosh --authenticationDatabase admin --quiet --eval \
 "try { printjson(rs.status()) }\
  catch (err) {\
    print('Initializing $MONGO_RSET_NAME');\
    rs.initiate({_id:'$MONGO_RSET_NAME', members:[{_id:0, host:'localhost:27017'}]});\
    printjson(rs.status());\
  }"
echo "\n"
echo "---------- < Startup Completed > ----------"