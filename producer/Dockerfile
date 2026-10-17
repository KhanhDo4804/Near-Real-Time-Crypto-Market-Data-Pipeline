FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY producer/requirements.txt /app/producer/requirements.txt
RUN pip install --no-cache-dir -r /app/producer/requirements.txt \
    && useradd --uid 1000 --create-home pipeline

COPY --chown=pipeline:pipeline producer/binance_producer.py /app/producer/binance_producer.py
COPY --chown=pipeline:pipeline config/market.json /app/config/market.json

USER pipeline
CMD ["python", "-u", "producer/binance_producer.py"]
