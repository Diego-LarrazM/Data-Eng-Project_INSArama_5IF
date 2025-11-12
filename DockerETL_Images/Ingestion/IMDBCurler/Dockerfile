FROM python:3.14-slim

WORKDIR /app

COPY . .

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget gzip && \
    pip install --no-cache-dir -r ./config/requirements.txt

RUN chmod +x /app/scripts/start.sh

CMD ["sh", "./scripts/start.sh"]

# docker build -t ingestion/imdb_curler -f DockerETL_Images\Ingestion\IMDBCurler\Dockerfile DockerETL_Images\Ingestion\IMDBCurler
# docker run --name Ingestion_Curler --network IngestionNet -v ./DockerETL_Images/Ingestion/IMDBCurler/data:/app/data --rm --env-file .env ingestion/imdb_curler