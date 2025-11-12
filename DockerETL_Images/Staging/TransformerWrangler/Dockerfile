FROM python:3.14-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r ./config/requirements.txt

RUN chmod +x /app/scripts/start.sh

CMD ["sh", "./scripts/start.sh"]

# docker build -t staging/wrangler -f DockerETL_Images\Staging\TransformerWrangler\Dockerfile DockerETL_Images\Staging\TransformerWrangler
# docker run --name Staging_Wrangler --network IngestionNet -v ./data:/app/data --rm --env-file .env staging/wrangler