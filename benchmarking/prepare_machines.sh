#!/usr/bin/env bash

# create odin node hostnames
nodes=""
odin_end=8
for ((i=1;i<=odin_end;i++)); do
  nodes="${nodes} odin0${i}"
done

files="start.sh clean.sh pack.sh record_stats.sh logback.xml dodo.jar"

for node in ${nodes}; do
  echo "Preparing node ${node}"
  # ensure directory
  ssh student@${node} 'mkdir -p ~/dodo'

  # copy over files using ssh

  # actually use globbing and word splitting here
  # shellcheck disable=SC2086
  scp ${files} student@${node}:~/dodo/
done
