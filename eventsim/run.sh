#!/bin/bash
CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
START_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
END_TIME=$(date -u -v+24H +"%Y-%m-%dT%H:%M:%S")
echo $CURRENT_TIME
echo "Running eventsim with start-time=$START_TIME and end-time=$END_TIME at $(date)"

docker run --name eventsim_temp --rm \
  --network streamify_my-network \
  eventsim:latest \
  -c examples/example-config.json \
  --continuous \
  --start-time "$START_TIME" \
  --end-time "$END_TIME" \
  --nusers 200000 \
  --growth-rate 10 \
  --userid 1 \
  --kafkaBrokerList broker-1:29092,broker-2:29092,broker-3:29092

# stop container after finishing
docker rm -f eventsim_temp >/dev/null 2>&1 || true
