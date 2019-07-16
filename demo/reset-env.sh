#!/usr/bin/env bash

nodes="node1 node2 node3 node4"

for dirname in ${nodes}; do
    echo "Preparing $dirname ..."
    mkdir -p ${dirname}
    pushd ${dirname} >/dev/null

    echo "  ... erasing logs and results"
    echo "" > dodo.log
    echo "" > all.log
    rm results.txt 2>/dev/null

    echo "  ... replacing application (*.jar)"
    cp ../dodo.jar ./
    popd >/dev/null

    echo "Finished."
    echo ""
done