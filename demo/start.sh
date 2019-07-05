#!/usr/bin/env bash

# exit script on first failure
set -e

nodes="node1 node2 node3"
seed_port=7877
input_file=../../data/flight_1k.csv
has_header=true

port=${seed_port}
for node in ${nodes}; do
    echo "Starting ${node}"
    pushd ${node} >/dev/null
    java -jar dodo.jar \
         --input-file=${input_file} \
         --has-header=${has_header} \
         --port=${port} \
         --seed-port=${seed_port} &
    pid=$!
    popd >/dev/null
    echo ${pid} >> .pidfile
    port=$(( port + 1 ))
done