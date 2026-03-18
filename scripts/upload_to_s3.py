## Upload RAW data to S3 bucket
import os
import logging

from service.client import Client
from config.settings import BUCKET_NAME

logging.basicConfig(level=logging.INFO)

# -----------------------------------------------------------------------
# Function to upload file to S3
# -----------------------------------------------------------------------
def upload_files_from_folder(
        local_folder='dataset', 
        bucket_name=BUCKET_NAME
    ):
    s3 = Client.get_s3_client()

    target_files = ['telecom_customer_churn.csv', 
        'telecom_zipcode_population.csv']   

    if not os.path.exists(local_folder):
        logging.error("Local folder does not exist.")
        return

    try:
        for file_name in target_files:
            local_path = os.path.join(local_folder, file_name)

            s3_key = f'raw/{file_name}'

            s3.upload_file(
                local_path,
                bucket_name,
                s3_key
            )

            logging.info(f'Upload successful: s3://{bucket_name}/{s3_key}')

    except Exception as e:
        logging.error(f"Upload failed: {e}")
        
# -----------------------------------------------------------------------
# Function to list files in S3 bucket
# -----------------------------------------------------------------------
def list_files_in_bucket(bucket_name = BUCKET_NAME):
    s3 = Client.get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket = bucket_name)
        if 'Contents' in response:
            logging.info(f"Files in bucket '{bucket_name}':")
            for obj in response['Contents']:
                logging.info(f" - {obj['Key']}")
        else:
            logging.info(f"No files found in bucket '{bucket_name}'.")
    except Exception as e:
        logging.error(f"Failed to list files: {e}")
        
# -----------------------------------------------------------------------
# Main execution
# -----------------------------------------------------------------------
if __name__ == "__main__":
    upload_files_from_folder()
    list_files_in_bucket()

