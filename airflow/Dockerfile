FROM apache/airflow:3.1.7-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/pipeline-env \
    && /opt/pipeline-env/bin/pip install --no-cache-dir \
       "dbt-core>=1.11,<1.12" \
       "dbt-redshift==1.11.1" \
       boto3 python-dotenv redshift-connector

USER airflow