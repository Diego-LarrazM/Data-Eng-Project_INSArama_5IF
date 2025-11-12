FROM python:3.14-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r ./config/requirements.txt

RUN chmod +x /app/scripts/start.sh

CMD ["sh", "./scripts/start.sh"]

# docker build -t staging/sql_persistor -f DockerETL_Images\Staging\SQLPersistor\Dockerfile DockerETL_Images\Staging\SQLPersistor
# docker run --name Staging_SQLPersistor --network IngestionNet -v ./data:/app/data --rm --env-file .env staging/sql_persistor