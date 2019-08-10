#!/usr/bin/env bash

./record_stats.sh &
java -jar \
  -Xmx5g -Xms5g \
  -Dcom.github.codelionx.dodo.output-file=results.txt \
  -Dcom.github.codelionx.dodo.workers=1 \
  -Dakka.loglevel=\"INFO\" \
  -Dlogback.configurationFile=file:logback.xml \
  dodo.jar \
  --input-file=../data/test.csv \
  --has-header=false \
  --host=$(hostname) \
  --port=7877 \
  --seed-port=7877 \
  >metrics.csv

#     --seed-host=odin01 \

# wait for first child to end (will be the java-command)
# wait -n

# stop all remaining childs
pkill -P $$ 1>/dev/null 2>&1
