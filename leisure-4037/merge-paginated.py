import boto3
import jsonlines
import json
import os
import re
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# AWS profile name
aws_profile = 'PowerUserAccess-484850288072'

# S3 client using specific profile
session = boto3.Session(profile_name=aws_profile)
s3 = session.client('s3')

# AWS S3 bucket and prefixes
source_bucket = 'gulp-data-vault-decode'
source_prefix = 'filtered-data-4037-updated/'
destination_bucket = 'gulp-data-vault-decode'
destination_prefix = 'filtered-data-4037-updated-json-merged/'


# Function to count total number of files to be processed
def count_total_files_and_size(source_bucket, source_prefix):
    # s3 = boto3.client('s3')
    total_files = 0
    total_size = 0  # Initialize total size variable
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': source_bucket, 'Prefix': source_prefix}
    page_iterator = paginator.paginate(**operation_parameters)
    
    for page in page_iterator:
        contents = page.get('Contents', [])
        total_files += len(contents)
        total_size += sum(obj['Size'] for obj in contents)  # Sum the sizes of the objects
    
    return total_files, total_size  # Return both total files and total size
# Function to merge JSON objects from files with similar names
def merge_json_files(source_bucket, source_prefix, destination_bucket, destination_prefix):
    try:
        # Temporary directory to store downloaded files
        local_temp_dir = "tmp"
        if not os.path.exists(local_temp_dir):
            os.makedirs(local_temp_dir)

        # Dictionary to store JSON objects for each group
        merged_data = {}

        total_files, total_size = count_total_files_and_size(source_bucket, source_prefix)
        print(f"Total files to be processed: {total_files} | size: {total_size}")

        file_counter = 0

        # Pagination handling for listing objects
        paginator = s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': source_bucket, 'Prefix': source_prefix}
        page_iterator = paginator.paginate(**operation_parameters)

        print("Merging JSON files...")
        file_counter = 0
        for page in page_iterator:  
            for obj in page.get('Contents', []):
                source_key = obj['Key']
                if source_key.endswith(".json") and "_2024" in source_key:
                    # Safe filename handling
                    file_name = re.sub(r'[^\w\-_\. ]', '_', os.path.basename(source_key))
                    local_file_path = os.path.join(local_temp_dir, file_name)

                    # Download the file from S3
                    s3.download_file(source_bucket, source_key, local_file_path)

                    # Extract the prefix before "_2024"
                    prefix = file_name.split("_2024")[0]

                    # Read the JSON lines file and extract JSON objects
                    with jsonlines.open(local_file_path) as reader:
                        for json_obj in reader:
                            merged_data.setdefault(prefix, []).append(json_obj)

                    # Upload merged data to S3
                    s3_key = f"{destination_prefix}{prefix}.json"
                    merged_content = ""
                    for json_obj in merged_data[prefix]:
                        merged_content += json.dumps(json_obj) + "\n"
                    
                    # Upload to S3
                    s3.put_object(Bucket=destination_bucket, Key=s3_key, Body=merged_content)
                    print(f"Uploaded file {file_counter + 1}: {s3_key}")
                    file_counter += 1

                    # Delete file from local temp directory
                    os.remove(local_file_path)
                    print(f"Deleted local file: {local_file_path}")
                    print(f"{file_counter}/{total_files} processed")

        print("Merging process completed.")
    except NoCredentialsError:
        print("Error: AWS credentials not found.")
    except PartialCredentialsError:
        print("Error: Incomplete AWS credentials provided.")
    except Exception as e:
        print(f"Error: {e}")

# Call the function to merge JSON files
merge_json_files(source_bucket, source_prefix, destination_bucket, destination_prefix)
