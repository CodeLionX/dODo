#!/usr/bin/env bash

# exit script on first failure
set -e

seed_port=7877

echo "Starting node4"
pushd node4 >/dev/null
java -jar \
     -Dcom.github.codelionx.dodo.output-file=results.txt \
     -Dakka.loglevel=\"DEBUG\" \
     -Dlogback.configurationFile=file:../logback.xml \
     dodo.jar \
     --port=8000 \
     --seed-port=${seed_port} &
pid=$!
popd >/dev/null
echo ${pid} >> .pidfile