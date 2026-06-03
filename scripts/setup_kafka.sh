#!/bin/bash
set -e

podman run -d \
  --name kafka \
  -p 9092:9092 \
  docker.io/apache/kafka:3.7.0

echo "Waiting for Kafka to start..."
sleep 5

podman exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic foo --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092

mkdir -p /tmp/kraft-combined-logs
podman cp kafka:/tmp/kraft-combined-logs/__cluster_metadata-0 /tmp/kraft-combined-logs/

echo "Done. Metadata log at /tmp/kraft-combined-logs/__cluster_metadata-0/"
