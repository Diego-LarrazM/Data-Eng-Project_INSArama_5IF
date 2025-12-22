#!/bin/sh
set -e

if find "$DATA_FILE_DIRECTORY" -maxdepth 2 -type f -name "*.json" -print -quit | grep -q .; then
  echo "At least one Media JSON file exists..."
else
  echo "No Media JSON files found"
  exec python ./scripts/main.py
fi


