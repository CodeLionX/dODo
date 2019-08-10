#!/usr/bin/env bash

interval=1m
output_file=node_stats.csv
delim=";"

echo "timestamp${delim}timestamp (human)${delim}Memory Util${delim}CPU Util" > ${output_file}

while true; do
  used_mem=$(grep "Active:" /proc/meminfo | sed 's/Active: //g')
  cpu_load=$(cut -d " " -f 1 < /proc/loadavg)

  date_seconds=$(date -u +"%s")
  date_human=$(date -u -Iseconds)

  echo "${date_seconds}${delim}${date_human}${delim}${used_mem}${delim}${cpu_load}" >> ${output_file}
  sleep ${interval}
done