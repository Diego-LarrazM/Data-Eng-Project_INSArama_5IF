#!/bin/sh
set -e

mkdir -p "$DATA_FILE_DIRECTORY"

echo "\n[Downloading] IMDB datasets from $IMDB_DATA_URL"
echo "[Saving files] to $DATA_FILE_DIRECTORY\n"

if [ -z "$IMDB_FILES_TO_DOWNLOAD" ]; then
    raise "No files specified for download. Please set IMDB_FILES_TO_DOWNLOAD in the .env file."
fi

for file in $IMDB_FILES_TO_DOWNLOAD; do
    echo "[Processing $file]"
    if [ -f "$DATA_FILE_DIRECTORY$file.csv" ]; then
        echo "Skipping $DATA_FILE_DIRECTORY$.csv already exists.\n"
        continue
    fi
    wget -q -nc -O "$DATA_FILE_DIRECTORY$file.tsv.gz" "$IMDB_DATA_URL$file.tsv.gz" || \
      { echo "Download failed for $file"; exit 1; }
    echo "Decompressing $DATA_FILE_DIRECTORY$file.tsv.gz to $DATA_FILE_DIRECTORY$file.csv"
    gzip -d "$DATA_FILE_DIRECTORY$file.tsv.gz"
    tr '\t' ',' < "$DATA_FILE_DIRECTORY$file.tsv" > "$DATA_FILE_DIRECTORY$file.csv"
    rm "$DATA_FILE_DIRECTORY$file.tsv"
    echo "Completed $file downloaded and decompressed to $DATA_FILE_DIRECTORY$file.csv\n"

done

echo "\nAll files downloaded and decompressed successfully.\n"
exec python ./scripts/main.py
