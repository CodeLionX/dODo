#!/usr/bin/env bash

function join {
  local IFS="$1"
  shift
  echo "$*"
}

filename_append=$(join _ "$@")
if [[ -n ${filename_append} ]]; then
  filename_append="${filename_append}_"
fi

filename="run_${filename_append}"
files="start.sh dodo.log metrics.csv results.txt"

# find unused filename
i=0
while [[ -f "${filename}${i}.zip" ]]; do
  i=$(( i+1 ))
done
filename="${filename}${i}.zip"

# check file existence
file_missing=false
for f in ${files}; do
  if [[ ! -f ${f} ]]; then
    echo "${f} is missing!" >&2
    file_missing=true
  fi
done

# zip files
if [[ ${file_missing} != "true" ]]; then
  echo "Zipping into ${filename}"
  # actually use globbing and word splitting here (do not add double quotes!!)
  # shellcheck disable=SC2086
  if zip ${filename} ${files}; then
    echo "Running clean script after successful packing"
    ./clean.sh
  fi
else
  echo "Skipping packing step because files are missing!" >&2
  exit 1;
fi
