# import boto3
# import jsonlines
# import json
# import os
# import re
# from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# # AWS profile name
# aws_profile = 'PowerUserAccess-484850288072'

# # S3 client using specific profile
# session = boto3.Session(profile_name=aws_profile)
# s3 = session.client('s3')

# # AWS S3 bucket and prefixes
# source_bucket = 'gulp-data-vault-decode'
# source_prefix = 'filtered-data-4037-updated/'
# destination_bucket = 'gulp-data-vault-decode'
# destination_prefix = 'filtered-data-4037-updated-json-merged/'

# # Function to merge JSON objects from files with similar names
# def merge_json_files_locally(source_bucket, source_prefix, destination_bucket, destination_prefix):
#     try:
#         # Temporary directory to store downloaded files
#         local_temp_dir = "tmp"
#         if not os.path.exists(local_temp_dir):
#             os.makedirs(local_temp_dir)

#         # Dictionary to store merged JSON objects
#         merged_data = {}

#         # Pagination handling for listing objects
#         paginator = s3.get_paginator('list_objects_v2')
#         operation_parameters = {'Bucket': source_bucket, 'Prefix': source_prefix}
#         page_iterator = paginator.paginate(**operation_parameters)

#         print("Merging JSON files locally...")
#         file_counter = 0
#         total_files = sum(1 for _ in s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix).get('Contents', []))
        
#         for page in page_iterator:
#             for obj in page.get('Contents', []):
#                 source_key = obj['Key']
#                 if source_key.endswith(".json") and "_2024" in source_key:
#                     # Safe filename handling
#                     file_name = re.sub(r'[^\w\-_\. ]', '_', os.path.basename(source_key))
#                     local_file_path = os.path.join(local_temp_dir, file_name)

#                     # Download the file from S3
#                     s3.download_file(source_bucket, source_key, local_file_path)

#                     # Extract the prefix before "_2024"
#                     prefix = file_name.split("_2024")[0]

#                     # Read the JSON lines file and merge JSON objects
#                     with jsonlines.open(local_file_path) as reader:
#                         for json_obj in reader:
#                             merged_data.setdefault(prefix, []).append(json_obj)

#                     # Delete file from local temp directory
#                     os.remove(local_file_path)

#                     # Print progress
#                     file_counter += 1
#                     print(f"Processed file {file_counter}/{total_files}")

#         # Upload merged data to S3
#         print("Uploading merged data to S3...")
#         total_merged_files = len(merged_data)
#         uploaded_counter = 0
#         for prefix, data in merged_data.items():
#             s3_key = f"{destination_prefix}{prefix}.json"
#             merged_content = ""
#             for json_obj in data:
#                 merged_content += json.dumps(json_obj) + "\n"
#             s3.put_object(Bucket=destination_bucket, Key=s3_key, Body=merged_content)
#             uploaded_counter += 1
#             print(f"Uploaded merged file {uploaded_counter}/{total_merged_files}: {s3_key}")

#         print("Merging and uploading process completed.")
#     except NoCredentialsError:
#         print("Error: AWS credentials not found.")
#     except PartialCredentialsError:
#         print("Error: Incomplete AWS credentials provided.")
#     except Exception as e:
#         print(f"Error: {e}")

# # Call the function to merge JSON files locally and upload to S3
# merge_json_files_locally(source_bucket, source_prefix, destination_bucket, destination_prefix)



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
destination_prefix = 'filtered-data-4037-updated-json-merged-locally/'

# Function to count total number of files to be processed
def count_total_files(source_bucket, source_prefix):
    total_files = 0
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': source_bucket, 'Prefix': source_prefix}
    page_iterator = paginator.paginate(**operation_parameters)
    
    for page in page_iterator:
        contents = page.get('Contents', [])
        total_files += len(contents)
    
    return total_files

# Function to merge JSON objects from files with similar names
def merge_json_files_locally(source_bucket, source_prefix, destination_bucket, destination_prefix):
    try:
        # Temporary directory to store downloaded files
        local_temp_dir = r"D:\tmp"
        if not os.path.exists(local_temp_dir):
            os.makedirs(local_temp_dir)

        # Dictionary to store merged JSON objects
        merged_data = {}

        # Count total files to be processed
        total_files = count_total_files(source_bucket, source_prefix)
        print(f"Total files to be processed: {total_files}")

        # Pagination handling for listing objects
        paginator = s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': source_bucket, 'Prefix': source_prefix}
        page_iterator = paginator.paginate(**operation_parameters)

        print("Merging JSON files locally...")
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

                    # Read the JSON lines file and merge JSON objects
                    with jsonlines.open(local_file_path) as reader:
                        for json_obj in reader:
                            merged_data.setdefault(prefix, []).append(json_obj)

                    # Delete file from local temp directory
                    os.remove(local_file_path)

                    # Print progress
                    file_counter += 1
                    print(f"Processed file {file_counter}/{total_files}")

        # Upload merged data to S3
        print("Uploading merged data to S3...")
        total_merged_files = len(merged_data)
        uploaded_counter = 0
        for prefix, data in merged_data.items():
            s3_key = f"{destination_prefix}{prefix}.json"
            merged_content = ""
            for json_obj in data:
                merged_content += json.dumps(json_obj) + "\n"
            s3.put_object(Bucket=destination_bucket, Key=s3_key, Body=merged_content)
            uploaded_counter += 1
            print(f"Uploaded merged file {uploaded_counter}/{total_merged_files}: {s3_key}")

        print("Merging and uploading process completed.")
    except NoCredentialsError:
        print("Error: AWS credentials not found.")
    except PartialCredentialsError:
        print("Error: Incomplete AWS credentials provided.")
    except Exception as e:
        print(f"Error: {e}")

# Call the function to merge JSON files locally and upload to S3
merge_json_files_locally(source_bucket, source_prefix, destination_bucket, destination_prefix)
