#!/usr/bin/env bash

# exit script on first failure
set -e

source settings.sh

echo "Starting extra-node: ${extra_node}"
pushd ${extra_node} >/dev/null
java -jar \
     -Dcom.github.codelionx.dodo.output-file=results.txt \
     ${common_java_ops} \
     dodo.jar \
     --port=8000 \
     --seed-port=${seed_port} &
pid=$!
popd >/dev/null
echo ${pid} >> .pidfile
