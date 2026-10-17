import json
import os
import redshift_connector
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit

import boto3
from dotenv import load_dotenv


def find_logs(s3, bucket, prefix):
    """Lấy log compact mới nhất và các batch được ghi sau nó."""
    folder = f"{prefix}/_spark_metadata/"
    logs = {}
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=folder)
    for page in pages:
        for item in page.get("Contents", []):
            name = item["Key"][len(folder):]
            if name.removesuffix(".compact").isdigit():
                logs[name] = item["Key"]

    if not logs:
        raise ValueError("Chưa có metadata Spark trên S3")

    compact_batch = max(
        (int(name.split(".")[0]) for name in logs if name.endswith(".compact")),
        default=-1,
    )
    last_batch = max(int(name.split(".")[0]) for name in logs)
    selected = [logs[f"{compact_batch}.compact"]] if compact_batch >= 0 else []
    for batch in range(compact_batch + 1, last_batch + 1):
        if str(batch) not in logs:
            raise ValueError(f"Thiếu log batch {batch}. Hãy chạy lại để đọc metadata mới.")
        selected.append(logs[str(batch)])
    return selected


def read_files(s3, bucket, prefix, logs):
    """Đọc các file đã commit, bỏ file có action delete."""
    files = {}
    for log in logs:
        with s3.get_object(Bucket=bucket, Key=log)["Body"] as body:
            lines = body.read().decode("utf-8").splitlines()
        if not lines or lines[0] != "v1":
            raise ValueError(f"Không hỗ trợ định dạng metadata: {log}")

        for line in lines[1:]:
            item = json.loads(line)
            path = urlsplit(item["path"])
            key = unquote(path.path).lstrip("/")
            if path.netloc != bucket or not key.startswith(prefix + "/"):
                raise ValueError("Metadata chứa file ngoài thư mục đã chọn")

            if item["action"] == "delete":
                files.pop(key, None)
            elif item["action"] == "add":
                if key.endswith(".parquet") and not item.get("isDir"):
                    files[key] = item["size"]
            else:
                raise ValueError(f"Action không hỗ trợ: {item['action']}")
    return files

def get_loaded_files():
    # Lấy user/password tạm thời bằng AWS IAM.
    redshift = boto3.client(
        "redshift-serverless",
        region_name=os.environ["AWS_REGION"],
    )

    credentials = redshift.get_credentials(
        workgroupName=os.environ["REDSHIFT_WORKGROUP"],
        dbName=os.environ["REDSHIFT_DATABASE"],
    )

    connection = redshift_connector.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
        database=os.environ["REDSHIFT_DATABASE"],
        user=credentials["dbUser"],
        password=credentials["dbPassword"],
        ssl=True,
        sslmode="verify-full",
        timeout=60,
    )

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT file_path FROM etl.loaded_files")
        return {row[0] for row in cursor.fetchall()}
    finally:
        connection.close()
        cursor.close()

def create_manifest():
    # Đọc cấu hình S3 từ .env.
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    path = urlsplit(os.environ["OHLCV_OUTPUT_PATH"])
    if path.scheme not in ("s3", "s3a") or not path.netloc:
        raise ValueError("OHLCV_OUTPUT_PATH phải là đường dẫn S3")
    bucket = path.netloc
    prefix = unquote(path.path).strip("/")
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION") or None)

    # Tìm các file Parquet Spark đã ghi thành công.
    logs = find_logs(s3, bucket, prefix)
    files = read_files(s3, bucket, prefix, logs)
    loaded_files = get_loaded_files()

    new_files = {}
    for key, size in files.items():
        file_uri = f"s3://{bucket}/{key}"

        if file_uri not in loaded_files:
            new_files[key] = size

    files = new_files

    if not files:
        print("Không có file mới cần nạp")
        return None
    
    # Tạo danh sách cho COPY rồi lưu JSON lên S3.
    entries = []
    for key, size in sorted(files.items()):
        entries.append({
            "url": f"s3://{bucket}/{key}",
            "mandatory": True,
            "meta": {"content_length": size},
        })

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    manifest_key = f"{prefix}/_redshift_manifests/all-{timestamp}.json"
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps({"entries": entries}).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"Đã tạo manifest gồm {len(entries)} file Parquet")
    return f"s3://{bucket}/{manifest_key}"


if __name__ == "__main__":
    print(create_manifest())
