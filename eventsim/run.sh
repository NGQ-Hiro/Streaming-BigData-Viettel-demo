#!/bin/bash
CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
START_TIME=$(date -u -d "$CURRENT_TIME UTC - 10 days" +"%Y-%m-%dT%H:%M:%S")
END_TIME=$(date -u -d "$CURRENT_TIME UTC + 10 minutes" +"%Y-%m-%dT%H:%M:%S")
# END_TIME=$(date -u -d "$START_TIME UTC +1 years" +"%Y-%m-%dT%H:%M:%S")
echo $CURRENT_TIME
echo "Running eventsim with start-time=$START_TIME and end-time=$END_TIME at $(date)"

docker run --name eventsim_temp --rm \
  --network streamify_my-network \
  eventsim-eventsim:latest \
  -c examples/example-config.json \
  --continuous \
  --start-time "$START_TIME" \
  --end-time "$END_TIME" \
  --nusers 20000 \
  --growth-rate 10 \
  --userid 1 \
  --kafkaBrokerList broker:29092

# stop container after finishing
docker rm -f eventsim_temp >/dev/null 2>&1 || true
