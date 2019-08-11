#!/usr/bin/env bash

./record_stats.sh &
java -jar \
  -Xmx40g -Xms40g \
  -Dcom.github.codelionx.dodo.output-file=results.txt \
  -Dcom.github.codelionx.dodo.workers=8 \
  -Dakka.loglevel=\"INFO\" \
  -Dlogback.configurationFile=file:logback.xml \
  dodo.jar \
  --input-file=data/flight_1k_stripped10.csv \
  --has-header=true \
  --host=${HOSTNAME} \
  --seed-host=${SEED_HOSTNAME} \
  --port=7877 \
  --seed-port=7877 \
  > metrics.csv

# stop all remaining childs
pkill -P $$ 1>/dev/null 2>&1
