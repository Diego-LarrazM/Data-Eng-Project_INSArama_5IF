FROM python:3.14-slim

WORKDIR /app

COPY . .

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget gzip && \
    pip install --no-cache-dir -r ./config/requirements.txt

RUN chmod +x /app/scripts/start.sh

CMD ["sh", "./scripts/start.sh"]

# docker build -t ingestion/metacritic_scrapper -f DockerETL_Images/Ingestion/MetacriticScrapper/Dockerfile DockerETL_Images/Ingestion/MetacriticScrapper
# docker run --name Ingestion_Scrapper --network IngestionNet -v ./data:/app/data --rm --env-file .env ingestion/metacritic_scrapper