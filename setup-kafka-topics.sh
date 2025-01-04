#!/bin/bash

# Create topics for each region
docker exec -it kafka kafka-topics \
    --create \
    --bootstrap-server localhost:9092 \
    --topic europe-transactions \
    --partitions 3 \
    --replication-factor 1

docker exec -it kafka kafka-topics \
    --create \
    --bootstrap-server localhost:9092 \
    --topic americas-transactions \
    --partitions 3 \
    --replication-factor 1

docker exec -it kafka kafka-topics \
    --create \
    --bootstrap-server localhost:9092 \
    --topic asiapacific-transactions \
    --partitions 3 \
    --replication-factor 1

# List created topics
docker exec -it kafka kafka-topics \
    --list \
    --bootstrap-server localhost:9092