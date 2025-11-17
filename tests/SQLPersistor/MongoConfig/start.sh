#!/bin/bash
set -e

# Start mongod temporarily in fork mode for replica set init
mongod --config "$CONFIG_FILE" --fork --logpath /tmp/mongod.log

# Wait for mongod to be ready
until mongosh --port $MONGO_PORT --eval "db.adminCommand('ping')" &> /dev/null; do
  echo -n "."
  sleep 2
done
echo ""
echo "MongoDB is ready!"

# Initialize replica set (ignore errors if already initiated)
mongosh --port $MONGO_PORT --quiet <<EOF
try {
  rs.initiate({
    _id: "$REPLICA_SET",
    members: [{ _id: 0, host: "localhost:$MONGO_PORT" }]
  })
} catch (e) { print(e) }
EOF

echo "Replica set initialized."

# Stop the temporary mongod
mongod --shutdown --dbpath /data/db

# Start mongod in foreground (main process for Docker)
exec mongod --config "$CONFIG_FILE"

