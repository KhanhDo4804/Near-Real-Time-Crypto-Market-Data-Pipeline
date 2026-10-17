from python:3.10-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "dbt-core>=1.11,<1.12" \
    "dbt-redshift==1.11.1"

workdir /usr/app/dbt

entrypoint ["dbt"]
cmd ["--version"]