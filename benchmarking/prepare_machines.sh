#!/usr/bin/env bash

# create odin node hostnames
nodes="172.16.64.61 172.16.64.62 172.16.64.63 172.16.64.64 172.16.64.65 172.16.64.66 172.16.64.67 172.16.64.68"
#nodes="172.16.64.62 172.16.64.63 172.16.64.64 172.16.64.65 172.16.64.66 172.16.64.67 172.16.64.68"
seed_node="172.16.64.62"
#odin_end=8
#for ((i=1;i<=odin_end;i++)); do
#  nodes="${nodes} odin0${i}"
#done

files="start.sh clean.sh pack.sh record_stats.sh logback.xml dodo.jar"

for node in ${nodes}; do
  echo "Preparing node ${node}"
  # ensure directory
  ssh student@"${node}" 'mkdir -p ~/dodo/data'

  # copy over files using ssh

  # actually use globbing and word splitting here
  # shellcheck disable=SC2086
  rsync ${files} student@"${node}":~/dodo/
  rsync ../data/flight_1k.csv ../data/flight_1k_stripped10.csv student@"${node}":~/dodo/data/

  # set node ip and seed node ip
  ssh student@"${node}" "sed -i 's/\${HOSTNAME}/${node}/g' ~/dodo/start.sh; sed -i 's/\${SEED_HOSTNAME}/${seed_node}/g' ~/dodo/start.sh"

done
