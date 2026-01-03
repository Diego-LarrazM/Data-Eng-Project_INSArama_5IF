#!/bin/sh

# ------------------------------
# start-compose.sh
# ------------------------------
echo "[ Checking for existing container '$MONGO_HOST_NAME'... ]"
# On supprime (-f force) le conteneur. 
# "|| true" permet de ne pas planter le script si le conteneur n'existe pas.
docker rm -f "$MONGO_HOST_NAME" > /dev/null 2>&1 || true

echo "[ Starting mongo docker container... ]"

docker run -d --name "$MONGO_HOST_NAME" --network "$INSARAMA_NET" -e MONGO_INITDB_ROOT_USERNAME="$MONGO_USERNAME" -e MONGO_INITDB_ROOT_PASSWORD="$MONGO_PASSWORD" -e MONGO_INITDB_DATABASE=admin mongo:7.0 --bind_ip_all --port $MONGO_PORT --auth
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
  if echo "$STATUS" | grep -q '"ok":1'; then
    echo "\n< MongoDB is healthy! >"
    # On marque le succès explicitement
    SUCCESS=1
    break
  fi

  echo "Attempt $i/$MONGO_HEALTHCHECK_RETRIES failed. Current status: ['$STATUS']. Retrying..."
  sleep 2

  i=$((i + 1))
  
done

if [ "$SUCCESS" != "1" ]; then
    echo "\n<@@ MongoDB failed to become healthy after $MONGO_HEALTHCHECK_RETRIES attempts. @@>"
    exit 1
fi

echo "\n"

# echo "[ Setuing Up Replica Set: '$MONGO_HOST_NAME' ]\n"
# # Setup Replica Set
# docker exec -i $MONGO_HOST_NAME mongosh --authenticationDatabase admin --quiet --eval \
#  "try { printjson(rs.status()) }\
#   catch (err) {\
#     print('Initializing $MONGO_RSET_NAME');\
#     rs.initiate({_id:'$MONGO_RSET_NAME', members:[{_id:0, host:'localhost:$MONGO_PORT'}]});\
#     printjson(rs.status());\
#   }"
# echo "\n"

echo "---------- < Transient Server Running > ----------"