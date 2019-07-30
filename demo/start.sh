#!/usr/bin/env bash

# exit script on first failure
set -e

source settings.sh

port=${seed_port}
for node in ${nodes}; do
    echo "Starting ${node}"
    pushd ${node} >/dev/null
    java -jar \
         -Dcom.github.codelionx.dodo.output-file=results.txt \
         ${common_java_ops} \
         dodo.jar \
         --input-file=${input_file} \
         --has-header=${has_header} \
         --port=${port} \
         --seed-port=${seed_port} &
    pid=$!
    popd >/dev/null
    echo ${pid} >> .pidfile
    port=$(( port + 1 ))
done
