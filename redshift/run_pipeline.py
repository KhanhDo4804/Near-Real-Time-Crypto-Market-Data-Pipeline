import argparse
import json
import subprocess
import os
from pathlib import Path
import boto3

from create_manifest import create_manifest


def run_dbt(*args):
    dbt_executable = os.getenv("DBT_EXECUTABLE")

    if dbt_executable:
        project = Path(__file__).resolve().parents[1] / "dbt"

        command = [
            dbt_executable, *args,
            "--project-dir", str(project),
            "--profiles-dir", str(project),
        ]
    else:
        command = [
            "docker", "compose", "-p", "crypto-market",
            "run", "--rm", "dbt", *args,
        ]

    subprocess.run(command, check=True)


def mark_loaded_files(manifest_uri):
    # Lấy đúng danh sách file của manifest đã COPY trong lượt chạy này.
    bucket, key = manifest_uri[5:].split("/", 1)
    s3 = boto3.client("s3")

    response = s3.get_object(Bucket=bucket, Key=key)
    with response["Body"] as body:
        manifest = json.loads(body.read())

    file_paths = [item["url"] for item in manifest["entries"]]

    for start in range(0, len(file_paths), 200):
        batch = file_paths[start:start + 200]

        run_dbt(
            "run-operation", "mark_loaded_files",
            "--args", json.dumps({"file_paths": batch}),
        )

    print(f"Hoàn tất xử lý {len(file_paths)} file")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("step", nargs="?", default="all", choices=[
        "all", "create_manifest", "copy_staging", "build_and_test", "mark_loaded_files",
    ])
    args = parser.parse_args()

    if args.step in ("all", "create_manifest"):
        manifest_uri = create_manifest()
        if not manifest_uri:
            if args.step == "create_manifest":
                # Airflow dùng mã 99 để bỏ qua khi không có file mới.
                raise SystemExit(99)
            return
        if args.step == "create_manifest":
            # Dòng cuối stdout được Airflow chuyển sang task sau bằng XCom.
            print(manifest_uri)
            return
    else:
        manifest_uri = os.getenv("MANIFEST_URI", "")

    if args.step in ("copy_staging", "mark_loaded_files"):
        if not manifest_uri.startswith("s3://"):
            parser.error("Cần MANIFEST_URI từ task create_manifest")

    if args.step in ("all", "copy_staging"):
        run_dbt(
            "run-operation", "load_ohlcv_staging",
            "--args", json.dumps({"manifest_uri": manifest_uri}),
        )
    if args.step in ("all", "build_and_test"):
        run_dbt("build", "--select", "+ohlcv_1m")
    if args.step in ("all", "mark_loaded_files"):
        mark_loaded_files(manifest_uri)


if __name__ == "__main__":
    main()
