FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY agent ./agent
COPY collector ./collector
COPY distributed ./distributed
COPY storage ./storage

RUN adduser --disabled-password --gecos "" appuser \
    && mkdir -p /data \
    && chown -R appuser:appuser /app /data

USER appuser

# Default command; overridden per-service in docker-compose.yml
CMD ["python", "-m", "collector.app"]
