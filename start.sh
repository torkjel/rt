#! /bin/bash

echo "Starting worker nodes"
java -jar storage/target/worker-1.0-SNAPSHOT.jar 0 &
java -jar storage/target/worker-1.0-SNAPSHOT.jar 1 &
java -jar storage/target/worker-1.0-SNAPSHOT.jar 2 &
java -jar storage/target/worker-1.0-SNAPSHOT.jar 3 &

echo "Starting api nodes"
java -jar api/target/api-1.0-SNAPSHOT.jar 0 etc/cluster.json &
java -jar api/target/api-1.0-SNAPSHOT.jar 1 etc/cluster.json &

echo "Waiting 10s for startup..."
sleep 15
