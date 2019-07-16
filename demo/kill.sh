#!/usr/bin/env bash

if [[ ! -f .pidfile ]]; then
  echo "No processes running (missing .pidfile)"
  exit 0
fi
while read -r pid; do
    echo "Killing job with pid ${pid}"
    kill ${pid}
done < .pidfile

echo "Clearing .pidfile"
rm .pidfile 1>/dev/null 2>&1
