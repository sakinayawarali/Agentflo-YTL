import boto3
import os
from dotenv import load_dotenv
import time
import argparse
import sys
import json
from botocore.exceptions import ClientError

load_dotenv()

# -------------------------
# Config (env-driven)
# -------------------------
REGION = os.getenv("AWS_REGION", "us-east-1")

S3_BUCKET_NAME = os.getenv("KB_SOURCE_BUCKET")  # agentflo-kb-s3bucket-1
S3_PREFIX = (os.getenv("KB_SOURCE_PREFIX", "tenants") or "tenants").strip("/")  # tenants

KNOWLEDGE_BASE_ID = os.getenv("BEDROCK_KB_ID")  # GKFPVYBKWV
DATA_SOURCE_ID = os.getenv("BEDROCK_DS_ID")     # EQL0SEDSSV

# Timings
POLL_SECONDS = 10
S3_DELAY_SECONDS = 2  # small safety delay after upload

# -------------------------
# Clients
# -------------------------
s3_client = boto3.client("s3", region_name=REGION)
bedrock_agent_client = boto3.client("bedrock-agent", region_name=REGION)


def require_env():
    missing = []
    for k, v in {
        "AWS_REGION": REGION,
        "KB_SOURCE_BUCKET": S3_BUCKET_NAME,
        "KB_SOURCE_PREFIX": S3_PREFIX,
        "BEDROCK_KB_ID": KNOWLEDGE_BASE_ID,
        "BEDROCK_DS_ID": DATA_SOURCE_ID,
    }.items():
        if not v:
            missing.append(k)
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


def s3_exists(bucket: str, key: str) -> bool:
    """
    Returns True if object exists at s3://bucket/key
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def upload_to_s3(local_file_path: str, tenant_id: str) -> tuple[str, bool]:
    """
    Upload file to:
      s3://<bucket>/tenants/<tenant_id>/<filename>

    Idempotent by key: if the exact object already exists, skip upload.

    Returns:
      (s3_key, uploaded_bool)
    """
    if not os.path.exists(local_file_path):
        print(f"Error: File not found at {local_file_path}", file=sys.stderr)
        return "", False

    file_name = os.path.basename(local_file_path)
    s3_key = f"{S3_PREFIX}/{tenant_id}/{file_name}"

    if s3_exists(S3_BUCKET_NAME, s3_key):
        print(f"ℹ️ Already exists: s3://{S3_BUCKET_NAME}/{s3_key} (skipping upload)")
        return s3_key, False

    print(f"Uploading '{local_file_path}' -> 's3://{S3_BUCKET_NAME}/{s3_key}'")
    s3_client.upload_file(local_file_path, S3_BUCKET_NAME, s3_key)
    print("✅ Upload successful.")
    return s3_key, True


def put_metadata_sidecar(file_s3_key: str, tenant_id: str, description: str) -> str:
    """
    Writes a Bedrock KB metadata sidecar next to the file:
      <file>.metadata.json

    This is how we attach tenantId + fileDescription for pooled multi-tenancy.
    """
    meta_key = f"{file_s3_key}.metadata.json"
    payload = {
        "metadataAttributes": {
            "tenantId": tenant_id,
            "fileDescription": (description or "").strip()[:500],
            "sourceKey": file_s3_key,
        }
    }

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=meta_key,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    print(f"✅ Metadata written -> s3://{S3_BUCKET_NAME}/{meta_key}")
    return meta_key


def start_ingestion_job(kb_id: str, ds_id: str) -> str:
    """
    Starts ingestion job. If a job is already running, returns 'IN_PROGRESS'.
    """
    try:
        resp = bedrock_agent_client.start_ingestion_job(
            knowledgeBaseId=kb_id,
            dataSourceId=ds_id
        )
        job_id = resp.get("ingestionJob", {}).get("ingestionJobId")
        if not job_id:
            raise RuntimeError("start_ingestion_job response missing ingestionJobId")
        print(f"✅ Ingestion job started: {job_id}")
        return job_id
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConflictException":
            print("ℹ️ Ingestion job already in progress. Will wait for it to finish.")
            return "IN_PROGRESS"
        raise


def wait_for_ingestion(kb_id: str, ds_id: str, job_id: str) -> None:
    """
    Wait for ingestion completion.
    - If job_id == 'IN_PROGRESS', poll data source status until AVAILABLE.
    - Else poll the job status.
    """
    if job_id == "IN_PROGRESS":
        while True:
            ds = bedrock_agent_client.get_data_source(
                knowledgeBaseId=kb_id,
                dataSourceId=ds_id
            )["dataSource"]

            if ds.get("status") == "AVAILABLE":
                print("✅ Data source AVAILABLE again.")
                return

            time.sleep(POLL_SECONDS)

    while True:
        job = bedrock_agent_client.get_ingestion_job(
            knowledgeBaseId=kb_id,
            dataSourceId=ds_id,
            ingestionJobId=job_id
        )["ingestionJob"]

        status = job.get("status")
        if status == "COMPLETE":
            print("✅ Ingestion COMPLETE.")
            return
        if status in ("FAILED", "STOPPED"):
            raise RuntimeError(f"❌ Ingestion job ended with status={status}. Check Bedrock console details.")

        time.sleep(POLL_SECONDS)


def main():
    require_env()

    parser = argparse.ArgumentParser(description="Ingest a file (tenant isolated) and sync Bedrock KB.")
    parser.add_argument("--file", required=True, help="Local path to file")
    parser.add_argument("--tenant", required=True, help="Tenant code, e.g. EBM")
    parser.add_argument("--desc", required=True, help="Short description of what this file holds")
    args = parser.parse_args()

    tenant_id = args.tenant.strip()
    if not tenant_id:
        print("Error: tenant cannot be empty", file=sys.stderr)
        sys.exit(1)

    # Step 1: upload file (idempotent)
    file_key, uploaded = upload_to_s3(args.file, tenant_id)
    if not file_key:
        print("Halting due to upload failure.", file=sys.stderr)
        sys.exit(1)

    # Step 2: upload metadata sidecar (always write/update)
    meta_key = put_metadata_sidecar(file_key, tenant_id, args.desc)

    # Step 3: ingest+sync only when file was newly uploaded (your requirement)
    if uploaded:
        print(f"Waiting {S3_DELAY_SECONDS}s for S3 visibility...")
        time.sleep(S3_DELAY_SECONDS)

        job_id = start_ingestion_job(KNOWLEDGE_BASE_ID, DATA_SOURCE_ID)
        wait_for_ingestion(KNOWLEDGE_BASE_ID, DATA_SOURCE_ID, job_id)
    else:
        print("ℹ️ Upload skipped (already existed). Ingestion not triggered.")

    print("\n--- Done ---")
    print(json.dumps({
        "tenantId": tenant_id,
        "fileUri": f"s3://{S3_BUCKET_NAME}/{file_key}",
        "metadataUri": f"s3://{S3_BUCKET_NAME}/{meta_key}",
        "uploaded": uploaded
    }, indent=2))


if __name__ == "__main__":
    main()
