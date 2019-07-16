#!/usr/bin/env bash

source settings.sh

nodes="${nodes} ${extra_node}"

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

echo "Clearing .pidfile"
rm .pidfile 1>/dev/null 2>&1
