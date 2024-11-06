from datetime import datetime, timezone
import boto3
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# Configuration
r2_access_key_id = os.getenv('R2_ACCESS_KEY_ID')
r2_secret_access_key = os.getenv('R2_SECRET_ACCESS_KEY')
r2_endpoint_url = os.getenv('R2_ENDPOINT_URL')
bucket_names = os.getenv('R2_BUCKET_NAMES')
local_directory = os.getenv('LOCAL_DIRECTORY')
amount_of_files = os.getenv('AMOUNT_OF_FILES')

start_date = datetime(2024, 5, 1, tzinfo=timezone.utc)  # First of May
end_date = datetime(2024, 10, 31, 23, 59, 59, tzinfo=timezone.utc)  # End of October

buckets = bucket_names.split(',')

print(f"r2_access_key_id: {r2_access_key_id}")

# Create a session and client
session = boto3.session.Session()
client = session.client(
    's3',
    endpoint_url=r2_endpoint_url,
    aws_access_key_id=r2_access_key_id,
    aws_secret_access_key=r2_secret_access_key
)

for bucket_name in buckets:
    # List objects in the bucket with the prefix 'meta-of'
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix='meta-of')

    object_count = 0
    for page in pages: object_count += len(page['Contents'])

    amount_of_finished_files = 0

    print(f"Found {object_count} files in {bucket_name}")
    print(f"Downloading files from {bucket_name}")

    for page in pages:

        if amount_of_files and amount_of_finished_files >= int(amount_of_files):
            break

        if 'Contents' in page:
            for obj in page.get('Contents', []):

                if amount_of_files and amount_of_finished_files >= int(amount_of_files):
                    break

                last_modified = obj['LastModified']
                if start_date > last_modified > end_date:
                    break

                object_key = obj['Key']
                file_name = os.path.basename(f"{object_key}.json")
                directory = f"{local_directory}/{bucket_name}"

                # Create the local directory if it doesn't exist
                Path(directory).mkdir(parents=True, exist_ok=True)

                local_file_path = os.path.join(directory, file_name)
                
                # Download the object and save it to the local directory
                client.download_file(bucket_name, object_key, local_file_path)
                print(f"Downloaded: {amount_of_finished_files}/{amount_of_files} {bucket_name} {object_key}")

                amount_of_finished_files += 1
        else:
            print("No objects found with the prefix 'meta-of'.")