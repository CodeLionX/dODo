#!/usr/bin/env bash

java -jar \
     -Xmx5g -Xms5g \
     -Dcom.github.codelionx.dodo.output-file=results.txt \
     -Dcom.github.codelionx.dodo.workers=1 \
     -Dakka.loglevel=\"INFO\" \
     -Dlogback.configurationFile=file:logback.xml \
     dodo.jar \
     --input-file=../data/test.csv \
     --has-header=false \
     --port=7877 \
     --seed-port=7877 \
     > metrics.csv
