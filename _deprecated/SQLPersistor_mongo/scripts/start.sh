#!/bin/sh
set -e

mkdir -p "$DATA_FILE_DIRECTORY"

exec python ./scripts/main.py
