#!/bin/sh
set -e

mkdir -p "$DATA_FILE_DIRECTORY"

echo "\n[Downloading] IMDB datasets from $IMDB_DATA_URL"
echo "[Saving files] to $DATA_FILE_DIRECTORY/\n"

if [ -z "$IMDB_FILES_TO_DOWNLOAD" ]; then
    raise "No files specified for download. Please set IMDB_FILES_TO_DOWNLOAD in the .env file."
fi

for file in $IMDB_FILES_TO_DOWNLOAD; do
    echo "[Processing $file]"
    if [ -f "$DATA_FILE_DIRECTORY/$file.tsv.gz" ]; then
        echo "Skipping $DATA_FILE_DIRECTORY/$file.tsv.gz already exists.\n"
        continue
    fi
    wget -q -nc -O "$DATA_FILE_DIRECTORY/$file.tsv.gz" "$IMDB_DATA_URL$file.tsv.gz" || \
      { echo "Download failed for $file"; exit 1; }
    echo "Completed $file downloaded to $DATA_FILE_DIRECTORY/$file.tsv.gz\n"

done

echo "\nAll files downloaded and decompressed successfully.\n"
