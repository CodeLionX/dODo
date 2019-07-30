#!/usr/bin/env bash

# names of the nodes to be spawned during cluster startup (separated by whitespace)
nodes="node1 node2 node3"
# name of the extra node (can be started via `./start-single.sh`)
extra_node=node4

# port of the seed node
seed_port=7877

# input configuration
input_file=../../data/flight_1k.csv
has_header=true

# common java ops, used for all nodes
common_java_ops="\
    -Dcom.github.codelionx.dodo.workers=2 \
    -Dakka.loglevel=\"DEBUG\" \
    -Dlogback.configurationFile=file:../logback.xml"
