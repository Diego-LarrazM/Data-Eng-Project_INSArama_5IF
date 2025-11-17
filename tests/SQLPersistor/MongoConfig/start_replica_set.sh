#!/bin/bash
set -e

# Run rs.initiate and rs.status via mongosh non-interactively
docker exec -i mongo_test_server mongosh -u test -p test --authenticationDatabase admin <<'EOF'
rs.initiate({
  _id: "TestReplicaSet",
  members: [
    { _id: 0, host: "localhost:27017" }
  ]
})
rs.status()
EOF
