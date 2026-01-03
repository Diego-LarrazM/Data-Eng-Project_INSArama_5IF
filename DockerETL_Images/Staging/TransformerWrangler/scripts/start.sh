#!/bin/sh
set -e

# Ensure data folders exist
mkdir -p "$IMDB_DATA_FILE_DIRECTORY"
mkdir -p "$METACRITIC_DATA_FILE_DIRECTORY"

# Make output folder
mkdir -p "$OUT_DATA_FILE_DIRECTORY"


exec python ./scripts/main.py
