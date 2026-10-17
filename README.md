# Real-Time Crypto Market Data Pipeline & Financial Analytics Warehouse

A Docker-based data engineering project that collects Binance Spot trades, aggregates one-minute OHLCV and VWAP candles with Apache Spark, stores Parquet files in Amazon S3, and loads an analytics table in Amazon Redshift Serverless through Apache Airflow and dbt.

The streaming layer runs continuously. The warehouse layer runs hourly. Power BI is the planned reporting layer; dashboard assets are not included in this repository.

## Contents

- [Architecture](#architecture)
- [Technology stack](#technology-stack)
- [Market coverage and data model](#market-coverage-and-data-model)
- [Repository structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Run the streaming pipeline locally](#run-the-streaming-pipeline-locally)
- [Configure AWS and the warehouse](#configure-aws-and-the-warehouse)
- [Run Airflow and dbt](#run-airflow-and-dbt)
- [Deploy to EC2](#deploy-to-ec2)
- [Operations and troubleshooting](#operations-and-troubleshooting)
- [Current limitations and next steps](#current-limitations-and-next-steps)

## Architecture

![Pipeline architecture showing Binance, Kafka, Spark Streaming, S3, Redshift, dbt, Airflow, and Docker](images/architecture.png)

The streaming path collects Binance trades through the Python producer and Kafka, then writes Spark's one-minute candles to S3. Airflow orchestrates the hourly load into Redshift staging and the dbt transformations into the analytics table.

### Streaming layer

1. The producer subscribes to the trade streams defined in `config/market.json`.
2. Each trade is normalized to JSON and published to `market_trades_raw`, keyed by symbol.
3. Kafka buffers trades for Spark. A new topic is initialized with six partitions and replication factor one.
4. Spark aggregates trades by symbol and one-minute event-time window.
5. Finalized candles are appended as Parquet files to local storage or S3, partitioned by UTC trade date.

### Warehouse layer

The `crypto_market_pipeline` DAG runs these tasks in sequence:

```text
create_manifest → copy_staging → build_and_test → mark_loaded_files
```

| Task | Behavior |
| --- | --- |
| `create_manifest` | Reads committed files from `_spark_metadata`, excludes paths in `etl.loaded_files`, and writes an S3 COPY manifest. |
| `copy_staging` | Clears `staging.ohlcv_1m` and loads the manifest's files inside a transaction. |
| `build_and_test` | Runs `dbt build --select +ohlcv_1m`. |
| `mark_loaded_files` | Records the manifest's file paths after the build succeeds. |

The schedule is `0 * * * *`: minute zero of every hour. The DAG uses `Asia/Bangkok`, disables catchup, allows one active run, and currently has no automatic task retries. Stored market timestamps use UTC.

## Technology stack

Versions below reflect the image tags and dependencies configured in the repository.

| Component | Technology | Role |
| --- | --- | --- |
| Market source | Binance Spot WebSocket | Individual trade events |
| Producer | Python 3.12, `websockets`, `aiokafka` | Collection and Kafka publishing |
| Message broker | Apache Kafka 4.1.2, KRaft | Buffered event transport |
| Stream processor | Apache Spark 3.5.9, Scala 2.12, Java 17 | Stateful candle aggregation |
| S3 connector | Hadoop AWS 3.3.4 | S3A access from Spark |
| Object storage | Amazon S3, Parquet | Committed candle files and COPY manifests |
| Orchestrator | Apache Airflow 3.1.7 | Hourly warehouse workflow |
| Transformations | dbt Core 1.11.x, dbt-redshift 1.11.1 | Incremental table updates and SQL tests |
| Warehouse | Amazon Redshift Serverless | Historical OHLCV analytics |
| Deployment | Docker Compose on Linux / EC2 | Container lifecycle and networking |

Spark currently uses `local[2]`, a 2 GiB driver heap, six shuffle partitions, and at most 50,000 Kafka offsets per trigger. It is a single-host Spark application, not a distributed Spark cluster.

## Market coverage and data model

The source of truth is [`config/market.json`](config/market.json).

| Analysis group | Symbols |
| --- | --- |
| Blue-chip | BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT |
| Payments and Layer 1 | XRPUSDT, ADAUSDT, AVAXUSDT |
| DeFi and AI | LINKUSDT, UNIUSDT, FETUSDT |

These groups are project labels, not a live market-cap ranking. The producer constructs subscriptions as `<lowercase-symbol>@trade`; it does not automatically add other symbols.

### Kafka trade events

The JSON value contains `symbol`, `trade_id`, `price`, `quantity`, `trade_time`, `event_time`, `is_buyer_maker`, and `ingested_at`. Prices and quantities arrive as strings and are parsed as decimals in Spark. Trade, event, and ingestion timestamps are Unix milliseconds. `(symbol, trade_id)` identifies a source trade, but trade-level deduplication is not currently implemented.

### One-minute candle output

| Column | Meaning |
| --- | --- |
| `symbol` | Trading pair |
| `window_start`, `window_end` | UTC bounds of the one-minute window |
| `open`, `close` | First and last prices ordered by `(trade_time, trade_id)` |
| `high`, `low` | Maximum and minimum trade prices |
| `volume` | Sum of base-asset quantities |
| `trade_count` | Number of input trades included in the candle |
| `vwap` | Sum of `price × quantity` divided by total quantity |
| `trade_date` | UTC date used as the output partition |

`quote_volume` is computed internally for VWAP but is not included in the saved output. `trade_date` is encoded in the partition path and is not a physical column in the Parquet files loaded by the supplied staging DDL.

Spark checks for work every **five seconds**, but the output windows are **one minute**. Append-mode output is finalized when the event-time watermark passes the window end. The watermark delay is ten seconds and depends on observed event times, not a wall-clock timer. A five-second trigger does not promise a new finalized candle every five seconds.

### Warehouse models and tests

- `staging.ohlcv_1m`: files selected for the current load.
- `stg_ohlcv_1m`: a dbt view using `SELECT DISTINCT` over staging.
- `ohlcv_1m`: an incremental model using `delete+insert`, keyed by `(symbol, window_start)`.
- `etl.loaded_files`: file paths already processed, with load timestamps.

The supplied main-table DDL creates `market.ohlcv_1m`. Set `REDSHIFT_SCHEMA=market` to use that schema consistently. The dbt target schema is configurable.

Three SQL tests check duplicate candle keys in staging and the final model, plus invalid staging rows such as missing prices, inconsistent OHLC ranges, invalid window bounds, negative volume, and nonpositive trade counts. Exact duplicate removal is not a resolution rule for conflicting candles; duplicate keys with different values should be investigated.

## Repository structure

```text
.
├── config/market.json                # Symbol list and stream configuration
├── producer/
│   ├── binance_producer.py           # Binance → Kafka
│   ├── Dockerfile
│   └── requirements.txt
├── spark/
│   ├── Dockerfile
│   └── jobs/
│       ├── read_kafka.py             # Kafka → OHLCV/VWAP → Parquet
│       └── run_kafka.sh              # spark-submit and connector dependencies
├── airflow/
│   ├── Dockerfile
│   └── dags/crypto_market_pipeline.py
├── redshift/
│   ├── create_staging.sql
│   ├── create_main.sql
│   ├── create_etl_tables.sql
│   ├── create_manifest.py
│   └── run_pipeline.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   ├── macros/
│   └── tests/
├── images/
│   ├── architecture.png              # Pipeline architecture diagram
│   └── airflow.png                   # Successful warehouse DAG run
├── docker-compose.yml
├── .dockerignore
├── .env.example
└── requirements.txt
```

Runtime directories such as `data/`, `checkpoints/`, and log directories are excluded from Git. The root requirements support host-side Python scripts; the Docker images install their own dependencies.

## Prerequisites

- Git, Docker Engine, and Docker Compose.
- Outbound connectivity to Binance, container registries, Maven repositories, and the required AWS services.
- For S3/warehouse mode: an existing S3 bucket and Redshift Serverless namespace/workgroup in the same AWS Region, with suitable IAM and database permissions.
- A Linux host with enough RAM and disk space for Spark, Kafka, and Airflow together. Four vCPUs and 8 GiB RAM are a starting point for experimentation, not a measured capacity guarantee. A 16 GiB root disk is tight for these images; plan additional capacity and monitor usage.

The 100–300 messages/second mentioned in project planning is an assumption, not a benchmark. Measure real throughput, lag, memory, and batch duration before sizing a deployment.

Use `sudo docker ...` where the current account cannot access the Docker daemon. Containerized execution does not require Spark or Java installed on the host.

## Configuration

Clone the source and create an environment file without overwriting an existing one:

```bash
git clone https://github.com/KhanhDo4804/Real-Time-Crypto-Market-Data-Pipeline.git
cd Real-Time-Crypto-Market-Data-Pipeline
test -f .env || cp .env.example .env
```

Edit `.env` with your own values. The following example contains placeholders:

```dotenv
AWS_REGION=ap-southeast-1
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=

OHLCV_OUTPUT_PATH=s3a://YOUR_BUCKET/market/ohlcv_1m_ec2
OHLCV_CHECKPOINT_PATH=/opt/spark/checkpoints/ohlcv-ec2

REDSHIFT_HOST=YOUR_WORKGROUP.YOUR_ACCOUNT_ID.ap-southeast-1.redshift-serverless.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=dev
REDSHIFT_SCHEMA=market
REDSHIFT_USER=YOUR_DATABASE_USER
REDSHIFT_WORKGROUP=YOUR_WORKGROUP
REDSHIFT_ACCOUNT_ID=YOUR_ACCOUNT_ID
REDSHIFT_COPY_ROLE_ARN=arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_REDSHIFT_COPY_ROLE

AIRFLOW_UID=1000
```

Important configuration details:

- `REDSHIFT_HOST` is the hostname only, without `jdbc://`, a port, or a database suffix.
- The current dbt profile and metadata reader use **IAM authentication**. The `REDSHIFT_AUTH_METHOD` and password fields in the sample do not switch this implementation to database-password authentication.
- The COPY role is currently **hardcoded in `dbt/macros/load_ohlcv_staging.sql`**. Replace that ARN for your deployment; setting `REDSHIFT_COPY_ROLE_ARN` alone does not change the macro.
- On EC2, prefer an instance IAM role. Leave static credential fields empty when using it, and verify the containers can retrieve credentials through instance metadata. Temporary credentials require `AWS_SESSION_TOKEN`.
- Never commit `.env`, SSH private keys, or `airflow/passwords.json`. Revoke and replace credentials if exposed.

### Host directory permissions

The current Spark and dbt containers use UID 1000. Airflow defaults to UID 1000 and group 0.

```bash
mkdir -p data checkpoints airflow/logs dbt/target dbt/logs
sudo chown -R 1000:1000 data checkpoints
sudo chown -R 1000:0 airflow/logs dbt/target dbt/logs
sudo chmod -R u+rwX,g+rwX data checkpoints airflow/logs dbt/target dbt/logs
```

Verify these IDs match the intended deployment before changing ownership. The dbt project mount must also permit the configured user to write generated files. Avoid blanket `chmod 777` permissions.

## Run the streaming pipeline locally

For a local-output smoke test, override both paths for the container creation:

```bash
docker compose -p crypto-market config --quiet

OHLCV_OUTPUT_PATH=/opt/spark/data/ohlcv_1m \
OHLCV_CHECKPOINT_PATH=/opt/spark/checkpoints/ohlcv-parquet \
  docker compose -p crypto-market up -d --build producer spark

docker compose -p crypto-market ps -a
docker compose -p crypto-market logs -f --tail=50 producer spark
```

The dependency chain is Kafka healthy → `kafka-init` successful → producer and Spark. `kafka-init` exiting with code zero is expected. It creates the six-partition topic only if absent; it does not change an existing topic's partition count.

Spark starts its job automatically. Do not launch another `spark-submit` inside the running Spark container with the same checkpoint. Stop any host-side producer or old Spark job first to avoid duplicate collection or concurrent writers.

Local results appear under `data/ohlcv_1m/trade_date=YYYY-MM-DD/`. Wait for finalized event-time windows before expecting files. Pressing Ctrl+C while following logs only stops log display; detached containers keep running.

To use S3, configure a dedicated output/checkpoint pair in `.env` and recreate the service configuration:

```bash
docker compose -p crypto-market up -d --build producer spark
```

Shell environment variables override `.env` during Compose interpolation. Unset any previously exported path overrides when switching modes. Updating `.env` requires `docker compose up -d`; `docker compose restart` does not reload environment variables.

## Configure AWS and the warehouse

### S3 layout and permissions

A configured output prefix contains:

```text
s3://YOUR_BUCKET/market/ohlcv_1m_ec2/
├── _spark_metadata/                 # Spark's committed output log
├── _redshift_manifests/             # Manifests created by the loader
└── trade_date=YYYY-MM-DD/
    └── part-....snappy.parquet
```

There are two separate AWS permission paths:

| Identity | Required access |
| --- | --- |
| Spark / Airflow application identity | Bucket listing/location; object reads/writes for output metadata and manifests; Spark cleanup and multipart-upload permissions. Also the required Redshift Serverless IAM credential APIs. |
| Redshift COPY role, associated with the namespace | Read access to the manifest and every referenced Parquet object, plus relevant bucket access. |

For the application identity, the S3 actions are typically `ListBucket`, `GetBucketLocation`, `GetObject`, `PutObject`, `DeleteObject`, and `AbortMultipartUpload`. Scope object permissions to the selected prefix. S3A may also need to remove a parent directory-marker object such as `market/`; if denied, grant the required action on that exact marker ARN rather than expanding access to the entire bucket. SSE-KMS buckets require the corresponding KMS permissions.

When changing the output prefix, update **both** the application identity's permissions and the Redshift COPY role's permissions.

### Redshift setup

Run the following scripts in Redshift Query Editor v2 against the configured database:

1. [`redshift/create_staging.sql`](redshift/create_staging.sql)
2. [`redshift/create_etl_tables.sql`](redshift/create_etl_tables.sql)
3. [`redshift/create_main.sql`](redshift/create_main.sql)

Grant the metadata-reader identity permission to read `etl.loaded_files`. Grant the dbt identity the schema/table privileges needed to create its view, update staging and the final table, and insert tracking records. IAM permission to request temporary credentials does not replace database grants.

Allow TCP access to the configured Redshift port, normally 5439. For EC2 in the same VPC, add a Redshift security-group inbound rule with the **EC2 security group as the source**. Verify routing and outbound rules as well; different VPCs require an appropriate network path.

## Run Airflow and dbt

Create `airflow/passwords.json` locally on the deployment host with a strong password:

```json
{"admin": "REPLACE_WITH_A_STRONG_PASSWORD"}
```

This file is required by the bind mount and is intentionally not tracked. After creating it:

```bash
sudo chown 1000:0 airflow/passwords.json
sudo chmod 600 airflow/passwords.json

docker compose -p crypto-market up -d --build airflow
```

Wait until Spark has created valid `_spark_metadata` batches and Parquet files before triggering the warehouse workflow. Open `http://localhost:8081`, sign in as `admin`, find `crypto_market_pipeline`, and unpause it. Use **Trigger DAG** for an immediate test.

The current Airflow image includes dbt and loader dependencies in `/opt/pipeline-env`; the DAG does not need to start the separate dbt service or mount the Docker socket. The dbt Compose service is available for ad hoc commands:

```bash
docker compose -p crypto-market run --rm dbt debug
docker compose -p crypto-market run --rm dbt build --select +ohlcv_1m
```

For a complete manual warehouse load using the running Airflow container:

```bash
docker compose -p crypto-market exec -T airflow \
  /opt/pipeline-env/bin/python -u /opt/project/redshift/run_pipeline.py all
```

Pause the DAG and ensure no run is active before using the manual loader, so two processes do not clear or populate staging concurrently.

### Example successful DAG run

![Airflow DAG with all four warehouse tasks completed successfully](images/airflow.png)

## Deploy to EC2

1. Install Docker Engine and the Compose plugin on the EC2 host.
2. Clone the repository and create `.env` **on EC2**. Changes to a local `.env` do not update the remote host.
3. Configure the IAM identities, Redshift networking, SQL objects, writable directories, and Airflow password file described above.
4. Use a dedicated S3 output/checkpoint pair for the EC2 query. Do not reuse an old output prefix with a fresh checkpoint.
5. Start the streaming services, verify new Parquet files, and then start Airflow:

```bash
sudo systemctl enable --now docker
sudo docker compose -p crypto-market up -d --build producer spark
sudo docker compose -p crypto-market up -d --build airflow
```

Detached containers continue running after SSH disconnects or the client computer shuts down, provided EC2 remains running. Kafka, producer, and Spark already use `restart: unless-stopped`. The checked-in Airflow service does not yet have a restart policy; add `restart: unless-stopped` under that service and apply `docker compose up -d airflow` if it must restart automatically after a host reboot.

### Service access

| Service | Host endpoint | Exposure in the current Compose file |
| --- | --- | --- |
| Kafka | `127.0.0.1:9094` | Localhost only; containers use `kafka:19092` |
| Spark UI | `127.0.0.1:4040` | Localhost only |
| Airflow UI | Port `8081` | Published on all host interfaces; restrict access through host/cloud networking |

An SSH tunnel lets you reach Airflow without opening port 8081 to the Internet. Run this on your client computer:

```bash
ssh -i /path/to/your-key.pem \
  -L 18081:127.0.0.1:8081 ubuntu@YOUR_EC2_PUBLIC_IP
```

Keep that session open and visit `http://localhost:18081`. For Spark UI, use another local port forward to EC2's `127.0.0.1:4040`. If accessing Airflow directly, permit TCP 8081 only from the intended client addresses.

## Operations and troubleshooting

### Status, logs, and storage

Run Compose commands from the repository directory, consistently using project name `crypto-market`:

```bash
docker compose -p crypto-market ps -a
docker compose -p crypto-market logs --tail=100 spark
docker compose -p crypto-market logs --tail=100 airflow
docker compose -p crypto-market logs -f --tail=50 producer spark

df -h /
docker system df
```

Spark's application log level is `WARN`; quiet logs do not establish whether batches are progressing. A container being `Up` also does not prove it is writing output. Check restart counts, checkpoint commits, recent Kafka offsets, and new committed S3 files together.

To inspect the topic:

```bash
docker compose -p crypto-market exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 \
  --describe --topic market_trades_raw

docker compose -p crypto-market exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 \
  --topic market_trades_raw
```

Offset growth shows Kafka is receiving records; it does not establish that Spark or Redshift has processed them.

### Persistence and recovery

- Kafka stores raw trades in the `kafka-data` named volume. Retention is 24 hours or approximately 512 MiB per partition, enforced by segment cleanup. This is not a hard cap on total disk usage.
- Local Spark checkpoints persist through the `checkpoints/` bind mount. With S3 output configured, Parquet is written directly to S3; there is no additional Parquet copy in `data/`.
- A checkpoint contains Kafka offsets, batch progress, watermark information, and aggregation state. It is not a raw-data archive.
- Keep the output and checkpoint paired across restarts, with a single writer. Never delete `_spark_metadata` or edit batch IDs to force progress.
- A fresh checkpoint restarts batch numbering. Pointing it at an old output prefix can make the file sink skip batches already recorded there. Use the original compatible checkpoint or a new empty output prefix with a new checkpoint.
- A fresh query reads retained Kafka data from `earliest`, so replay may overlap warehouse history. Data older than Kafka retention cannot be recovered from Kafka alone.
- Streaming service logs rotate at three 10 MiB files per container. Airflow task logs and Docker build/image storage require separate monitoring and retention.

For a normal pause and restart:

```bash
docker compose -p crypto-market stop producer spark airflow kafka
docker compose -p crypto-market up -d producer spark airflow
```

Do not use `docker compose down -v` when you intend to preserve Kafka and Airflow volumes.

### Common symptoms

| Symptom | What to check |
| --- | --- |
| Airflow tasks are `Skipped` | `create_manifest` returns exit code 99 when all committed files are already tracked. Downstream tasks skip normally; no load occurred in that run. |
| `Chưa có metadata Spark trên S3` | No recognized Spark metadata batch exists at the configured prefix. Confirm Spark output, permissions, and first successful write. |
| Kafka grows but S3 files do not | Check Spark restarts/errors, output/checkpoint pairing, watermark progress, and S3 permissions. |
| S3 `AccessDenied` / HTTP 403 | Confirm the actual application's IAM identity can access the new prefix, including required marker cleanup. |
| Redshift connection timeout | Verify endpoint/port, security groups, VPC routing, and connectivity from the Airflow container. |
| COPY manifest error | Verify a complete `s3://bucket/key.json` URI, object existence, and access by the **Redshift COPY role**. |
| `Permission denied` on bind mounts | Check UID/GID and ownership of checkpoints, Airflow logs/password file, and dbt output directories. |
| No output from `logs --since=10m` | No matching logs were emitted. Remove the time filter and inspect status/restart counts. |
| Airflow UI inaccessible | Test `curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8081/` on EC2, then check forwarding/network rules. |
| Disk nearly full despite S3 output | Images, build cache, container filesystems, Kafka, logs, and checkpoints still use the EC2 disk. |

If task details are missing in the UI, read task logs inside the Airflow container:

```bash
docker compose -p crypto-market exec -T airflow bash -lc \
'find /opt/airflow/logs -type f -path "*dag_id=crypto_market_pipeline/*" -name "*.log" -mmin -60 -exec tail -v -n 80 {} \;'
```

Inspect logs for secrets before sharing them. When changing only `.env`, recreate the affected services with `up -d --force-recreate`; when changing image inputs, rebuild with `up -d --build`. Spark job files and the dbt project are bind-mounted, while producer code is baked into its image.

## Current limitations and next steps

- Single Kafka broker/controller and single-host Spark; there is no high-availability deployment.
- Airflow uses `standalone` and SimpleAuthManager for the current learning/demo setup.
- Raw trades are retained in Kafka but are not archived to S3.
- No source-trade deduplication, dead-letter routing, or exchange-status preflight check is implemented.
- The watermark does not provide a historical correction/backfill workflow for late trades.
- Warehouse loading uses a file ledger and keyed updates, but no end-to-end exactly-once guarantee is claimed across collection, replay, COPY, dbt, and bookkeeping.
- Power BI dashboards and higher-timeframe analytics are future work.

Next improvements include raw-trade archival and replay, validation before aggregation, bounded deduplication, freshness/lag monitoring, file compaction, late-data correction, automatic task retry after idempotency review, and production orchestration/security.
