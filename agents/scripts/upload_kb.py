import boto3
import os
from dotenv import load_dotenv
import time
import argparse
import sys
from botocore.exceptions import ClientError

# --- Configuration (from your details) ---
S3_BUCKET_NAME = "agentflo-knowledgebases"
KNOWLEDGE_BASE_ID = "KV6HWQ9FUA"
DATA_SOURCE_ID = "I4I97Y9PXZ"
# Bedrock Knowledge Bases are regional
BEDROCK_REGION = os.getenv('AWS_REGION')
# -----------------------------------------

# Initialize AWS clients
s3_client = boto3.client('s3')
# Use 'bedrock-agent' client for knowledge base operations
bedrock_agent_client = boto3.client('bedrock-agent', region_name=BEDROCK_REGION)

def upload_to_s3(local_file_path, company_name):
    """
    Uploads a file to the specified S3 bucket inside a company-named folder.
    """
    if not os.path.exists(local_file_path):
        print(f"Error: File not found at {local_file_path}", file=sys.stderr)
        return None

    file_name = os.path.basename(local_file_path)
    # This automatically "creates" the folder if it doesn't exist.
    s3_key = f"{company_name}/{file_name}" 
    
    print(f"Uploading '{local_file_path}' to 's3://{S3_BUCKET_NAME}/{s3_key}'...")
    
    try:
        s3_client.upload_file(local_file_path, S3_BUCKET_NAME, s3_key)
        print("✅ Upload successful.")
        return s3_key
    except ClientError as e:
        print(f"Error uploading file: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred during upload: {e}", file=sys.stderr)
        return None

def start_ingestion_job(kb_id, ds_id):
    """
    Starts an ingestion job for the specified knowledge base and data source.
    """
    print(f"Starting ingestion job for Knowledge Base ID: {kb_id} and Data Source ID: {ds_id}...")
    
    try:
        response = bedrock_agent_client.start_ingestion_job(
            knowledgeBaseId=kb_id,
            dataSourceId=ds_id
        )
        ingestion_job = response.get('ingestionJob', {})
        job_id = ingestion_job.get('ingestionJobId')
        
        if not job_id:
            print("Error: Could not start ingestion job. Response did not contain job ID.", file=sys.stderr)
            return None
            
        print(f"Ingestion job started with ID: {job_id}")
        return job_id
        
    except ClientError as e:
        # Handle case where a job is already in progress
        if e.response['Error']['Code'] == 'ConflictException':
            print("Info: An ingestion job is already in progress. Waiting for it to complete.")
            # We can try to find the running job, but for this script,
            # it's simpler to just report it and exit.
            # A more complex script could fetch the list of jobs and find the one 'IN_PROGRESS'.
            return "IN_PROGRESS" # Special status to indicate we should just wait
        print(f"Error starting ingestion job: {e}", file=sys.stderr)
        return None

def wait_for_ingestion(kb_id, ds_id, job_id):
    """
    Polls the status of the ingestion job until it completes or fails.
    """
    if job_id == "IN_PROGRESS":
        print("Waiting for the currently running job to complete...")
        # In this scenario, we don't have a job ID to poll.
        # We'll just wait for *any* job to stop running.
        # A more robust solution would list jobs and find the active one.
        # For simplicity, we'll poll the data source status instead.
        return wait_for_data_source_ready(kb_id, ds_id)
        
    print(f"Waiting for job {job_id} to complete. Polling every 30 seconds...")
    
    while True:
        try:
            response = bedrock_agent_client.get_ingestion_job(
                knowledgeBaseId=kb_id,
                dataSourceId=ds_id,
                ingestionJobId=job_id
            )
            
            job_status = response.get('ingestionJob', {}).get('status')
            
            if job_status == 'COMPLETE':
                print("\n✅ Ingestion job completed successfully.")
                break
            elif job_status in ['FAILED', 'STOPPED']:
                print(f"\n❌ Ingestion job {job_status}. Please check the AWS console for details.", file=sys.stderr)
                break
            elif job_status in ['STARTING', 'IN_PROGRESS']:
                print(f"... Status: {job_status} (Last updated: {response.get('ingestionJob', {}).get('updatedAt')})")
                time.sleep(30) # Wait 30 seconds before polling again
            else:
                print(f"Unknown job status: {job_status}", file=sys.stderr)
                break
                
        except ClientError as e:
            print(f"Error checking job status: {e}", file=sys.stderr)
            break
        except KeyboardInterrupt:
            print("\nWarning: Wait interrupted by user. The job may still be running in AWS.")
            break

def wait_for_data_source_ready(kb_id, ds_id):
    """
    Alternative waiter that polls the Data Source status.
    Used when a job is already in progress and we don't have the ID.
    """
    print("Polling data source status every 30 seconds until 'Available'...")
    while True:
        try:
            response = bedrock_agent_client.get_data_source(
                knowledgeBaseId=kb_id,
                dataSourceId=ds_id
            )
            ds_status = response.get('dataSource', {}).get('status')
            
            if ds_status == 'AVAILABLE':
                print("\n✅ Data source is now 'AVAILABLE'. The active job has likely completed.")
                break
            elif ds_status == 'DELETING':
                print(f"\n❌ Data source is {ds_status}. Aborting wait.", file=sys.stderr)
                break
            else: # Status is 'PENDING' or 'IMPORTING'
                print(f"... Data Source Status: {ds_status}")
                time.sleep(30)
                
        except ClientError as e:
            print(f"Error checking data source status: {e}", file=sys.stderr)
            break
        except KeyboardInterrupt:
            print("\nWarning: Wait interrupted by user. The job may still be running in AWS.")
            break

def main():
    parser = argparse.ArgumentParser(description="Upload a file to S3 and sync a Bedrock Knowledge Base.")
    parser.add_argument("--file", required=True, help="The path to the local file to upload.")
    parser.add_argument("--company", required=True, help="The company name (used as the S3 folder).")
    
    args = parser.parse_args()
    
    # --- Step 1: Upload File ---
    s3_key = upload_to_s3(args.file, args.company)
    
    if not s3_key:
        print("Halting script due to upload failure.")
        return

    # --- Step 2: Start Ingestion Job ---
    # We only trigger the sync if the upload was for 'nestle',
    # as your KB is specifically connected to that folder.
    if args.company.lower() == 'nestle':
    
        # !!!!!!!!!!! THIS IS THE FIX !!!!!!!!!!!
        print("✅ Upload successful. Waiting 10 seconds for S3 eventual consistency...")
        time.sleep(10) # Give S3 time to register the new file
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
        job_id = start_ingestion_job(KNOWLEDGE_BASE_ID, DATA_SOURCE_ID)
        
        if not job_id:
            print("Halting script due to ingestion start failure.")
            return
            
        # --- Step 3: Wait for Job to Complete ---
        wait_for_ingestion(KNOWLEDGE_BASE_ID, DATA_SOURCE_ID, job_id)
        
        print("\n--- Pipeline Finished ---")
        print(f"File '{args.file}' is processed and the knowledge base is synced.")
        print(f"The new document should now be available for retrieval from '{KNOWLEDGE_BASE_ID}'.")
    else:
        print(f"\n--- Pipeline Finished (Upload Only) ---")
        print(f"File '{args.file}' was uploaded to s3://{S3_BUCKET_NAME}/{s3_key}.")
        print(f"Sync was skipped because company '{args.company}' is not 'nestle'.")

if __name__ == "__main__":
    main()