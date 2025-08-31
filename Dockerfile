FROM python:3.11-slim


ENV PYTHONDONTWRITEBYTECODE=1 \
PYTHONUNBUFFERED=1 \
PIP_NO_CACHE_DIR=1


RUN apt-get update && apt-get install -y --no-install-recommends \
build-essential git curl && rm -rf /var/lib/apt/lists/*


WORKDIR /app
COPY requirements.txt /app/
RUN pip install -r requirements.txt


COPY . /app


# Create data dirs
RUN mkdir -p /data/raw_parquet && mkdir -p /data/qdrant


EXPOSE 8000