#!/usr/bin/env bash

while read -r pid; do
    echo "Killing job with pid ${pid}"
    kill ${pid}
done < .pidfile

echo "Clearing .pidfile"
rm .pidfile