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
files="start.sh dodo.log metrics.csv results.txt node_stats.csv"

# check for zip
filename_suffix=.tar.gz
use_zip=false
if command -v zip >/dev/null; then
  use_zip=true
  filename_suffix=.zip
fi

# find unused filename
i=0
while [[ -f "${filename}${i}${filename_suffix}" ]]; do
  i=$(( i+1 ))
done
filename="${filename}${i}${filename_suffix}"

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
  echo "Packing into ${filename}"
  if [[ ${use_zip} == "true" ]]; then
    # actually use globbing and word splitting here (do not add double quotes!!)
    # shellcheck disable=SC2086
    if zip "${filename}" ${files}; then
      echo "Running clean script after successful packing with zip"
      ./clean.sh
    else
      echo "Zipping failed"
      exit 1
    fi
  else
    # shellcheck disable=SC2086
    if tar -czvf "${filename}" ${files}; then
      echo "Running clean script after successful packing with tar"
      ./clean.sh
    else
      echo "Packing with tar failed"
      exit 1
    fi
  fi
else
  echo "Skipping packing step because files are missing!" >&2
  exit 1
fi
